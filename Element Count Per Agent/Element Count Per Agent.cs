using System;
using System.Collections.Generic;
using System.Linq;
using Skyline.DataMiner.Analytics.GenericInterface;
using Skyline.DataMiner.Net;
using Skyline.DataMiner.Net.Exceptions;
using Skyline.DataMiner.Net.Messages;

namespace ElementCountPerAgent
{

    /// <summary>
    /// Element (swarmable + non-swarmable) Count Per AGent
    /// </summary>
    [GQIMetaData(Name = "Element Count Per Agent")]
    public sealed class ElementCountPerAgent : IGQIDataSource, IGQIOnInit, IGQIUpdateable
    {
        private static readonly GQIStringColumn _stateColumn = new GQIStringColumn("Agent State");

        private readonly GQIColumn[] _columns = new GQIColumn[]
        {
            new GQIIntColumn("Agent ID"),
            new GQIStringColumn("Agent Name"),
            _stateColumn,
            new GQIIntColumn("Non-Swarmable Element Count"),
            new GQIIntColumn("Swarmable Element Count"),
            new GQIIntColumn("Total Element Count"),
        };

        private readonly object _dictLock = new object();
        private Dictionary<int, GetDataMinerInfoResponseMessage> _agentInfoCache = new Dictionary<int, GetDataMinerInfoResponseMessage>();
        private Dictionary<ElementID, ElementInfoEventMessage> _elementInfoCache = new Dictionary<ElementID, ElementInfoEventMessage>();

        private GQIDMS _dms;
        private IGQILogger _logger;
        private string _subscriptionID;

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

            _subscriptionID = "DS-Element-Count-Per-Agent-" + Guid.NewGuid();
            var connection = _dms.GetConnection();

            connection.OnNewMessage += (obj, args) =>
            {
                if ((args == null) || !args.FromSet(_subscriptionID))
                    return;

                switch (args.Message)
                {
                    case ElementInfoEventMessage elementInfo:
                        {
                            OnElementInfoEventMessage(updater, elementInfo);
                            break;
                        }

                    case DataMinerInfoEvent dataMinerInfo:
                        {
                            OnDataMinerInfoEvent(updater, dataMinerInfo);
                            break;
                        }
                }
            };

            connection.AddSubscription(
                _subscriptionID,
                new SubscriptionFilter(typeof(ElementInfoEventMessage)),
                new SubscriptionFilter(typeof(DataMinerInfoEvent)));

        }

        /// <inheritdoc />
        public GQIPage GetNextPage(GetNextPageInputArgs args)
        {
            lock (_dictLock)
            {
                _agentInfoCache = LoadAgents()
                    .ToDictionary(agentInfo => agentInfo.ID, agentInfo => agentInfo);

                _elementInfoCache = LoadElements()
                    .ToDictionary(elementInfo => elementInfo.ToElementID(), elementInfo => elementInfo);

                return new GQIPage(_agentInfoCache.Keys.Select(dataMinerId => GetRowForAgent(dataMinerId)).ToArray())
                {
                    HasNextPage = false,
                };
            }
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
        private void OnElementInfoEventMessage(IGQIUpdater updater, ElementInfoEventMessage elementInfo)
        {
            var elementId = elementInfo.ToElementID();
            var newHost = elementInfo.HostingAgentID;

            _logger.Debug($"Observed ElementInfoEvent for '{elementInfo.Name}' ({elementId}){(elementInfo.IsDeleted ? " [Deleted]" : string.Empty)} with host {newHost}");

            lock (_dictLock)
            {
                ElementInfoEventMessage oldEntry = null;
                if (_elementInfoCache.TryGetValue(elementId, out var entry))
                {
                    oldEntry = entry;
                }

                if (elementInfo.IsDeleted)
                    _elementInfoCache.Remove(elementId);
                else
                    _elementInfoCache[elementId] = elementInfo;

                if (oldEntry is null || elementInfo.IsDeleted)
                {
                    // update current host
                    var currentHostRow = GetRowForAgent(elementInfo.HostingAgentID);
                    updater.UpdateRow(currentHostRow);
                }
                else if (oldEntry.HostingAgentID != elementInfo.HostingAgentID)
                {
                    // update current host
                    var currentHostRow = GetRowForAgent(elementInfo.HostingAgentID);
                    updater.UpdateRow(currentHostRow);

                    // update old host
                    var oldHostRow = GetRowForAgent(oldEntry.HostingAgentID);
                    updater.UpdateRow(oldHostRow);
                }
            }
        }

        private void OnDataMinerInfoEvent(IGQIUpdater updater, DataMinerInfoEvent dataMinerInfo)
        {
            _logger.Information($"Observed DataMinerInfoEvent for '{dataMinerInfo.AgentName}' ({dataMinerInfo.DataMinerID}) with state {dataMinerInfo.Raw.ConnectionState}");

            lock (_dictLock)
            {
                _agentInfoCache[dataMinerInfo.DataMinerID] = dataMinerInfo.Raw;
            }

            updater.UpdateCell(dataMinerInfo.DataMinerID.ToString(), _stateColumn, dataMinerInfo.Raw.ConnectionState.ToString());
        }

        private GQIRow GetRowForAgent(int dataMinerID)
        {
            GetDataMinerInfoResponseMessage agentInfo = null;
            var swarmableCount = 0;
            var nonSwarmableCount = 0;
            lock (_dictLock)
            {
                if (!_agentInfoCache.TryGetValue(dataMinerID, out agentInfo) || agentInfo is null)
                    return null;

                foreach (var elementInfo in _elementInfoCache.Values)
                {
                    if (elementInfo.HostingAgentID != dataMinerID)
                        continue;

                    if (elementInfo.IsSwarmable)
                        swarmableCount++;
                    else
                        nonSwarmableCount++;
                }
            }

            var totalCount = swarmableCount + nonSwarmableCount;

            _logger.Information($"Updating host {agentInfo.ID} with counts ({nonSwarmableCount}/{swarmableCount}/{totalCount})");

            return new GQIRow(
                    dataMinerID.ToString(),
                    new[]
                    {
                        new GQICell() { Value = agentInfo.ID, DisplayValue = agentInfo.ID.ToString() },
                        new GQICell() { Value = agentInfo.AgentName, DisplayValue = agentInfo.AgentName },
                        new GQICell() { Value = agentInfo.ConnectionState.ToString(), DisplayValue = agentInfo.ConnectionState.ToString() },

                        new GQICell() { Value = nonSwarmableCount, DisplayValue = nonSwarmableCount.ToString() },
                        new GQICell() { Value = swarmableCount, DisplayValue = swarmableCount.ToString() },
                        new GQICell() { Value = totalCount, DisplayValue = totalCount.ToString() },
                    });
        }

        private GetDataMinerInfoResponseMessage[] LoadAgents()
        {
            if (_dms == null)
                throw new ArgumentNullException($"{nameof(GQIDMS)} is null.");

            DMSMessage[] resp = null;
            try
            {
                var req = new GetInfoMessage(InfoType.DataMinerInfo);
                resp = _dms.SendMessages(req);
            }
            catch (Exception ex)
            {
                throw new DataMinerSecurityException($"Issue occurred in {nameof(ElementCountPerAgent)} when sending request {nameof(GetInfoMessage)}.{InfoType.DataMinerInfo}: {ex}", ex);
            }

            if (resp == null || resp.Length == 0)
                throw new Exception($"Response is null or empty");

            var dmaResponses = resp.OfType<GetDataMinerInfoResponseMessage>().ToArray();
            if (dmaResponses.Length == 0)
                throw new Exception($"{nameof(dmaResponses)} is empty");

            return dmaResponses;
        }

        private ElementInfoEventMessage[] LoadElements()
        {
            if (_dms == null)
                throw new ArgumentNullException($"{nameof(GQIDMS)} is null.");

            DMSMessage[] resp = null;
            try
            {
                var req = new GetInfoMessage(InfoType.ElementInfo);
                resp = _dms.SendMessages(req);
            }
            catch (Exception ex)
            {
                throw new DataMinerSecurityException($"Issue occurred in {nameof(ElementCountPerAgent)} when sending request {nameof(GetInfoMessage)}.{InfoType.ElementInfo}: {ex}", ex);
            }

            if (resp == null)
                throw new Exception($"Response is null");

            return resp.OfType<ElementInfoEventMessage>().ToArray();
        }

    }

    internal static class Extensions
    {
        internal static ElementID ToElementID(this ElementInfoEventMessage elementInfo)
            => new ElementID(elementInfo.DataMinerID, elementInfo.ElementID);
    }
}
