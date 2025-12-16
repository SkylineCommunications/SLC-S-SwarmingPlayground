using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Skyline.DataMiner.Analytics.GenericInterface;
using Skyline.DataMiner.Net;
using Skyline.DataMiner.Net.Exceptions;
using Skyline.DataMiner.Net.Messages;

namespace ElementCountPerAgent
{

    /// <summary>
    /// Element (swarmable + non-swarmable) Count Per Agent
    /// </summary>
    [GQIMetaData(Name = "Element Count Per Agent")]
    public sealed class ElementCountPerAgent : IGQIDataSource, IGQIOnInit, IGQIUpdateable
    {
        private readonly GQIColumn[] _columns = new GQIColumn[]
        {
            new GQIIntColumn("Agent ID"),
            new GQIStringColumn("Agent Name"),
            new GQIStringColumn("Agent State"),
            new GQIIntColumn("Non-Swarmable Element Count"),
            new GQIIntColumn("Swarmable Element Count"),
            new GQIIntColumn("Total Element Count"),
        };

        private readonly Dictionary<int, RowData> _rowCache = new Dictionary<int, RowData>();
        private readonly Dictionary<ElementID, LiteElementInfoEvent> _elementInfoCache = new Dictionary<ElementID, LiteElementInfoEvent>();

        private GQIDMS _dms;
        private IGQILogger _logger;
        private IGQIUpdater _updater;
        private string _subscriptionID;
        private readonly ManualResetEventSlim _initialDataFetched = new ManualResetEventSlim(false);
        private readonly ConcurrentQueue<DMSMessage> _queuedUpdates = new ConcurrentQueue<DMSMessage>();

        /// <inheritdoc />
        public GQIColumn[] GetColumns() => _columns;

        /// <inheritdoc />
        public OnInitOutputArgs OnInit(OnInitInputArgs args)
        {
            if (args?.DMS == null)
                throw new ArgumentNullException($"{nameof(OnInitInputArgs)} or {nameof(GQIDMS)} is null.");

            _dms = args.DMS;
            _logger = args.Logger;

            return default;
        }

        /// <inheritdoc />
        public void OnStartUpdates(IGQIUpdater updater)
        {
            _logger.Debug("OnStartUpdates");

            _updater = updater;
            _subscriptionID = "DS-Element-Count-Per-Agent-" + Guid.NewGuid();
            var connection = _dms.GetConnection();

            connection.OnNewMessage += (obj, args) =>
            {
                if ((args == null) || !args.FromSet(_subscriptionID))
                    return;

                // Queue updates until initial pages are fetched
                if (!_initialDataFetched.IsSet)
                {
                    _queuedUpdates.Enqueue(args.Message);
                    return;
                }

                OnEvent(args.Message);
            };

            connection.AddSubscription(
                _subscriptionID,
                new SubscriptionFilter(typeof(DataMinerInfoEvent), SubscriptionFilterOptions.SkipInitialEvents),
                new SubscriptionFilter(typeof(LiteElementInfoEvent), SubscriptionFilterOptions.SkipInitialEvents));
        }

        /// <inheritdoc />
        public GQIPage GetNextPage(GetNextPageInputArgs args)
        {
            _logger.Debug("Fetching initial data");

            var (agentInfos, elementInfos) = FetchInitialData();

            foreach (var agentInfo in agentInfos)
                OnDataMinerInfoEvent(new DataMinerInfoEvent(agentInfo));

            foreach (var elementInfo in elementInfos)
                OnElementInfoEvent(elementInfo);

            var gqiRows = _rowCache.Select(kv => kv.Value.ToGQI()).ToArray();

            _logger.Debug($"Fetched {gqiRows.Length} initial data rows");

            // before returning, process backed up updates
            ProcessQueuedUpdates();

            _initialDataFetched.Set();
            return new GQIPage(gqiRows)
            {
                HasNextPage = false,
            };
        }

        /// <inheritdoc />
        public void OnStopUpdates()
        {
            if (_subscriptionID != null)
            {
                _dms.GetConnection().RemoveSubscription(_subscriptionID);
                _subscriptionID = null;
            }

            _initialDataFetched.Dispose();
        }

        private void ProcessQueuedUpdates()
        {
            _logger.Debug($"Processing {_queuedUpdates.Count} queued updates");

            while (_queuedUpdates.TryDequeue(out var queuedUpdate))
            {
                OnEvent(queuedUpdate);
            }
        }

        private (GetDataMinerInfoResponseMessage[], LiteElementInfoEvent[]) FetchInitialData()
        {
            if (_dms == null)
                throw new ArgumentNullException($"{nameof(GQIDMS)} is null.");

            var sw = Stopwatch.StartNew();

            DMSMessage[] resp = null;
            try
            {
                resp = _dms.SendMessages(
                    new GetInfoMessage(InfoType.DataMinerInfo),
                    new GetLiteElementInfo(includeStopped: true));
            }
            catch (Exception ex)
            {
                throw new DataMinerException($"Issue occurred in {nameof(ElementCountPerAgent)} when fetching initial data: {ex}", ex);
            }

            sw.Stop();
            _logger.Debug($"Requesting data from SLNet took {sw.ElapsedMilliseconds}ms");

            if (resp == null || resp.Length == 0)
                throw new Exception($"Response is null or empty");

            var agentInfos = resp.OfType<GetDataMinerInfoResponseMessage>().ToArray();
            if (agentInfos.Length == 0)
                throw new Exception($"{nameof(agentInfos)} is empty");

            var elementInfos = resp.OfType<LiteElementInfoEvent>().ToArray();

            return (agentInfos, elementInfos);
        }

        private void OnEvent(DMSMessage msg)
        {
            switch (msg)
            {
                case LiteElementInfoEvent elementInfo:
                    {
                        OnElementInfoEvent(elementInfo);
                        break;
                    }

                case DataMinerInfoEvent dataMinerInfo:
                    {
                        OnDataMinerInfoEvent(dataMinerInfo);
                        break;
                    }
            }
        }

        private void OnElementInfoEvent(LiteElementInfoEvent elementInfo)
        {
            var elementId = elementInfo.ToElementID();
            var shouldUpdateGQI = _initialDataFetched.IsSet;

            _logger.Debug($"Observed LiteElementInfoEvent for '{elementInfo.Name}' ({elementId}){(elementInfo.IsDeleted ? " [Deleted]" : string.Empty)} with host {elementInfo.HostingAgentID}");

            lock (_rowCache)
            {
                if (!_rowCache.TryGetValue(elementInfo.HostingAgentID, out var currentHostRow))
                {
                    _logger.Warning($"Could not find row data for agent {elementInfo.HostingAgentID} while processing addition of element {elementId} {elementInfo.Name}");
                    return;
                }

                // Case 1: Deletion
                if (elementInfo.IsDeleted)
                {
                    if (!_elementInfoCache.Remove(elementId))
                        // element was not tracked, nothing to do
                        return;

                    // update old host
                    currentHostRow.TotalElementCount--;
                    if (elementInfo.IsSwarmable)
                        currentHostRow.SwarmableElementCount--;
                    else
                        currentHostRow.NonSwarmableElementCount--;

                    if (shouldUpdateGQI)
                    {
                        _logger.Information($"Updating host {currentHostRow.AgentID} with counts ({currentHostRow.NonSwarmableElementCount}/{currentHostRow.SwarmableElementCount}/{currentHostRow.TotalElementCount})");
                        _updater.UpdateRow(currentHostRow.ToGQI());
                    }
                    return;
                }

                // Case 2: Addition
                if (!_elementInfoCache.TryGetValue(elementId, out var oldElementInfo))
                {
                    _elementInfoCache[elementId] = elementInfo;

                    // update new host
                    currentHostRow.TotalElementCount++;
                    if (elementInfo.IsSwarmable)
                        currentHostRow.SwarmableElementCount++;
                    else
                        currentHostRow.NonSwarmableElementCount++;

                    if (shouldUpdateGQI)
                    {
                        _logger.Information($"Updating host {currentHostRow.AgentID} with counts ({currentHostRow.NonSwarmableElementCount}/{currentHostRow.SwarmableElementCount}/{currentHostRow.TotalElementCount})");
                        _updater.UpdateRow(currentHostRow.ToGQI());
                    }
                    return;
                }

                // Case 3: Swarmed (change host)
                if (oldElementInfo.HostingAgentID != elementInfo.HostingAgentID)
                {
                    _elementInfoCache[elementId] = elementInfo;

                    if (!_rowCache.TryGetValue(oldElementInfo.HostingAgentID, out var oldHostRow))
                    {
                        _logger.Warning($"Could not find row data for agent {oldElementInfo.HostingAgentID} while processing swarm of element {elementId} {elementInfo.Name} to agent {elementInfo.HostingAgentID}");
                        return;
                    }

                    // update new host
                    currentHostRow.TotalElementCount++;
                    if (elementInfo.IsSwarmable)
                        currentHostRow.SwarmableElementCount++;
                    else
                        currentHostRow.NonSwarmableElementCount++;

                    // but also update old host
                    oldHostRow.TotalElementCount--;
                    if (elementInfo.IsSwarmable)
                        oldHostRow.SwarmableElementCount--;
                    else
                        oldHostRow.NonSwarmableElementCount--;

                    if (shouldUpdateGQI)
                    {
                        _logger.Information($"Updating host {oldHostRow.AgentID} with counts ({oldHostRow.NonSwarmableElementCount}/{oldHostRow.SwarmableElementCount}/{oldHostRow.TotalElementCount})");
                        _updater.UpdateRow(oldHostRow.ToGQI());
                        _logger.Information($"Updating host {currentHostRow.AgentID} with counts ({currentHostRow.NonSwarmableElementCount}/{currentHostRow.SwarmableElementCount}/{currentHostRow.TotalElementCount})");
                        _updater.UpdateRow(currentHostRow.ToGQI());
                    }
                    return;
                }

                // Case 4: Some other update we are not interested in, ignoring
            }
        }

        private void OnDataMinerInfoEvent(DataMinerInfoEvent dataMinerInfo)
        {
            _logger.Information($"Observed DataMinerInfoEvent for '{dataMinerInfo.AgentName}' ({dataMinerInfo.DataMinerID}) with state {dataMinerInfo.Raw.ConnectionState}");

            var shouldUpdateGQI = _initialDataFetched.IsSet;

            lock (_rowCache)
            {
                if (!_rowCache.TryGetValue(dataMinerInfo.DataMinerID, out var existingRow))
                {
                    var newRow = new RowData()
                    {
                        AgentID = dataMinerInfo.DataMinerID,
                        AgentName = dataMinerInfo.AgentName,
                        AgentState = dataMinerInfo.Raw.ConnectionState.ToString(),
                        NonSwarmableElementCount = 0,
                        SwarmableElementCount = 0,
                        TotalElementCount = 0,
                    };

                    _rowCache[dataMinerInfo.DataMinerID] = newRow;

                    if (shouldUpdateGQI)
                    {
                        _updater.AddRow(newRow.ToGQI());
                    }
                    return;
                }

                if (existingRow.AgentState != dataMinerInfo.Raw.ConnectionState.ToString())
                {
                    existingRow.AgentState = dataMinerInfo.Raw.ConnectionState.ToString();

                    if (shouldUpdateGQI)
                    {
                        _updater.UpdateRow(existingRow.ToGQI());
                    }
                    return;
                }
            }
        }
    }

    internal static class Extensions
    {
        internal static ElementID ToElementID(this ElementBaseEventMessage elementInfo)
            => new ElementID(elementInfo.DataMinerID, elementInfo.ElementID);
    }

    internal class RowData
    {
        public int AgentID { get; set; }
        public string AgentName { get; set; }
        public string AgentState { get; set; }
        public int NonSwarmableElementCount { get; set; }
        public int SwarmableElementCount { get; set; }
        public int TotalElementCount { get; set; }


        public GQIRow ToGQI()
        {
            return new GQIRow(
                    AgentID.ToString(),
                    new[]
                    {
                        new GQICell() { Value = AgentID, DisplayValue = AgentID.ToString() },
                        new GQICell() { Value = AgentName, DisplayValue = AgentName },
                        new GQICell() { Value = AgentState, DisplayValue = AgentState },

                        new GQICell() { Value = NonSwarmableElementCount, DisplayValue = NonSwarmableElementCount.ToString() },
                        new GQICell() { Value = SwarmableElementCount, DisplayValue = SwarmableElementCount.ToString() },
                        new GQICell() { Value = TotalElementCount, DisplayValue = TotalElementCount.ToString() },
                    });
        }
    }
}
