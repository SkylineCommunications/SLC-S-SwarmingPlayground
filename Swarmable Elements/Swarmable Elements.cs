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

namespace SwarmableElements
{
    /// <summary>
    /// Shows the elements per agent with its state and swarmablility
    /// </summary>
    [GQIMetaData(Name = "Swarmable Elements")]
    public sealed class SwarmableElements : IGQIDataSource, IGQIOnInit, IGQIUpdateable
    {
        private readonly GQIColumn[] _columns = new GQIColumn[]
        {
            new GQIStringColumn("ElementID"),
            new GQIStringColumn("Element Name"),
            new GQIStringColumn("State"),
            new GQIStringColumn("Hosting Agent"),
            new GQIBooleanColumn("Swarmable"),
        };

        private GQIDMS _dms;
        private IGQILogger _logger;
        private IGQIUpdater _updater;
        private string _subscriptionID;
        private readonly ManualResetEventSlim _initialDataFetched = new ManualResetEventSlim(false);
        private readonly ConcurrentQueue<DMSMessage> _queuedUpdates = new ConcurrentQueue<DMSMessage>();

        private readonly ConcurrentDictionary<int, string> _agentInfoCache = new ConcurrentDictionary<int, string>();
        private readonly ConcurrentDictionary<ElementID, LiteElementInfoEvent> _elementInfoCache = new ConcurrentDictionary<ElementID, LiteElementInfoEvent>();
        private readonly Dictionary<ElementID, RowData> _rowCache = new Dictionary<ElementID, RowData>();

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
            _subscriptionID = "DS-Swarmable-Elements-" + Guid.NewGuid();
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
                new SubscriptionFilter(typeof(LiteElementInfoEvent), SubscriptionFilterOptions.SkipInitialEvents),
                new SubscriptionFilter(typeof(ElementStateEventMessage), SubscriptionFilterOptions.SkipInitialEvents)
                );
        }

        /// <inheritdoc />
        public GQIPage GetNextPage(GetNextPageInputArgs args)
        {
            _logger.Debug("Fetching initial data");

            var (agentInfos, elementInfos, elementStates) = FetchInitialData();

            foreach (var agentInfo in agentInfos)
                OnDataMinerInfoEvent(new DataMinerInfoEvent(agentInfo));

            foreach (var elementInfo in elementInfos)
                OnElementInfoEvent(elementInfo);

            foreach (var elementState in elementStates)
                OnElementStateEventMessage(elementState);

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
        }

        private void ProcessQueuedUpdates()
        {
            _logger.Debug($"Processing {_queuedUpdates.Count} queued updates");

            while (_queuedUpdates.TryDequeue(out var queuedUpdate))
            {
                OnEvent(queuedUpdate);
            }
        }

        private (GetDataMinerInfoResponseMessage[], LiteElementInfoEvent[], ElementStateEventMessage[]) FetchInitialData()
        {
            if (_dms == null)
                throw new ArgumentNullException($"{nameof(GQIDMS)} is null.");

            var sw = Stopwatch.StartNew();

            DMSMessage[] resp = null;
            try
            {
                resp = _dms.SendMessages(
                    new GetInfoMessage(InfoType.DataMinerInfo),
                    new GetLiteElementInfo(includeStopped: true),
                    new GetEventsFromCacheMessage(new SubscriptionFilter(typeof(ElementStateEventMessage))));
            }
            catch (Exception ex)
            {
                throw new DataMinerException($"Issue occurred in {nameof(SwarmableElements)} when fetching initial data: {ex}", ex);
            }

            sw.Stop();
            _logger.Debug($"Requesting data from SLNet took {sw.ElapsedMilliseconds}ms");

            if (resp == null || resp.Length == 0)
                throw new Exception($"Response is null or empty");

            var agentInfos = resp.OfType<GetDataMinerInfoResponseMessage>().ToArray();
            if (agentInfos.Length == 0)
                throw new Exception($"{nameof(agentInfos)} is empty");

            var elementInfos = resp.OfType<LiteElementInfoEvent>().ToArray();
            var elementStates = resp.OfType<ElementStateEventMessage>().ToArray();

            return (agentInfos, elementInfos, elementStates);
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

                case ElementStateEventMessage elementState:
                    {
                        OnElementStateEventMessage(elementState);
                        break;
                    }

                case DataMinerInfoEvent dataMinerInfo:
                    {
                        OnDataMinerInfoEvent(dataMinerInfo);
                        break;
                    }

                default:
                    throw new InvalidOperationException($"Received unsupported message type: {msg.GetType().FullName}");
            }
        }

        private void OnDataMinerInfoEvent(DataMinerInfoEvent dataMinerInfo)
        {
            _logger.Information($"Observed DataMinerInfoEvent for '{dataMinerInfo.AgentName}' ({dataMinerInfo.DataMinerID}) with state {dataMinerInfo.Raw.ConnectionState}");

            _agentInfoCache[dataMinerInfo.DataMinerID] = dataMinerInfo.Raw.AgentName;
        }

        private void OnElementInfoEvent(LiteElementInfoEvent elementInfo)
        {
            var elementId = elementInfo.ToElementID();

            _logger.Debug($"Observed ElementInfoEventMessage for '{elementInfo.Name}' ({elementId})");

            _elementInfoCache[elementId] = elementInfo;
        }

        private void OnElementStateEventMessage(ElementStateEventMessage elementState)
        {
            var elementId = elementState.ToElementID();

            if (!_elementInfoCache.TryGetValue(elementId, out var elementInfo))
            {
                _logger.Warning($"Could not find element name in cache for {elementId}, ignoring ElementStateEventMessage");
                return;
            }

            if (!_agentInfoCache.TryGetValue(elementState.HostingAgentID, out var currentAgentName))
            {
                _logger.Warning($"Could not find agent name in cache for {elementState.HostingAgentID}, ignoring ElementStateEventMessage");
                return;
            }

            var displayState = ToDisplayFriendlyState(elementState);

            _logger.Debug($"Observed ElementStateEvent for '{elementInfo.Name}' ({elementId}) with state {displayState} and host {currentAgentName}");

            var shouldUpdateGQI = _initialDataFetched.IsSet;

            lock(_rowCache)
            {
                // Case 1: Deletion
                if (elementState.IsDeleted)
                {
                    if (!_rowCache.Remove(elementId))
                        // element was not tracked, nothing to do
                        return;

                    if (shouldUpdateGQI)
                    {
                        _logger.Debug($"Removing row for '{elementInfo.Name}' ({elementId})");
                        _updater.RemoveRow(elementId.ToString());
                    }
                    return;
                }

                // Case 2: Addition
                if (!_rowCache.TryGetValue(elementId, out var existingRow))
                {
                    var newRow = new RowData()
                    {
                        ElementID = elementId,
                        ElementName = elementInfo.Name,
                        ElementDisplayState = displayState,
                        HostingAgentName = currentAgentName,
                        IsSwarmable = elementInfo.IsSwarmable,
                    };

                    _rowCache[elementId] = newRow;

                    if (shouldUpdateGQI)
                    {
                        _logger.Debug($"Adding row for '{elementInfo.Name}' ({elementId})");
                        _updater.AddRow(newRow.ToGQI());
                    }
                    return;
                }

                // Case 3: Update
                if (existingRow.ElementDisplayState != displayState 
                    || existingRow.HostingAgentName != currentAgentName
                    || existingRow.ElementName != elementInfo.Name
                    || existingRow.IsSwarmable != elementInfo.IsSwarmable)
                {
                    existingRow.ElementDisplayState = displayState;
                    existingRow.HostingAgentName = currentAgentName;
                    existingRow.ElementName = elementInfo.Name;
                    existingRow.IsSwarmable = elementInfo.IsSwarmable;

                    if (shouldUpdateGQI)
                    {
                        _updater.UpdateRow(existingRow.ToGQI());
                    }
                    return;
                }
            }
        }

        private string ToDisplayFriendlyState(ElementStateEventMessage elementStateEvent)
        {
            if (elementStateEvent.TimeoutSubState == TimeoutSubState.IsSwarming)
                return "Swarming";
            else if (elementStateEvent.TimeoutSubState == TimeoutSubState.HostingAgentDisconnected)
                return "Unavailable";
            else if (elementStateEvent.State == ElementState.Active && !elementStateEvent.IsElementStartupComplete)
                return "Starting";
            else
                return elementStateEvent.State.ToString();
        }
    }

    internal static class Extensions
    {
        internal static ElementID ToElementID(this ElementBaseEventMessage elementInfo)
            => new ElementID(elementInfo.DataMinerID, elementInfo.ElementID);
    }

    internal class RowData
    {
        public ElementID ElementID { get; set; }
        public string ElementName { get; set; }
        public string ElementDisplayState { get; set; }
        public string HostingAgentName { get; set; }
        public bool IsSwarmable { get; set; }

        public GQIRow ToGQI()
        {
            return new GQIRow(
                ElementID.ToString(),
                new[]
                {
                    new GQICell() { Value = ElementID.ToString(), DisplayValue = ElementID.ToString() },
                    new GQICell() { Value = ElementName, DisplayValue = ElementName},
                    new GQICell() { Value = ElementDisplayState, DisplayValue = ElementDisplayState },
                    new GQICell() { Value = HostingAgentName, DisplayValue = HostingAgentName },
                    new GQICell() { Value = IsSwarmable, DisplayValue = IsSwarmable.ToString() },
                });
        }
    }
}
