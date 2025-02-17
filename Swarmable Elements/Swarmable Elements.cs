using System;
using System.Collections.Generic;
using System.Linq;
using Skyline.DataMiner.Analytics.GenericInterface;
using Skyline.DataMiner.Net;
using Skyline.DataMiner.Net.Exceptions;
using Skyline.DataMiner.Net.Messages;

namespace SwarmableElements
{
    [GQIMetaData(Name = "Swarmable Elements")]
    public sealed class SwarmableElements : IGQIDataSource, IGQIOnInit, IGQIUpdateable
    {
        private GQIDMS _dms;
        private IGQILogger _logger;
        private string _subscriptionID;

        private Dictionary<int, string> _agentIDToName = new Dictionary<int, string>();

        private Dictionary<ElementID, (int, string)> _elementToHostAndState = new Dictionary<ElementID, (int, string)>();

        private static readonly GQIStringColumn _stateColumn = new GQIStringColumn("State");
        private static readonly GQIStringColumn _hostingAgentNameColumn = new GQIStringColumn("Hosting Agent");

        private readonly GQIColumn[] _columns = new GQIColumn[]
        {
            new GQIStringColumn("ElementID"),
            new GQIStringColumn("Element Name"),
            _stateColumn,
            _hostingAgentNameColumn,
            new GQIBooleanColumn("Swarmable"),
        };

        public GQIColumn[] GetColumns() => _columns;

        public OnInitOutputArgs OnInit(OnInitInputArgs args)
        {
            if (args?.DMS == null)
                throw new ArgumentNullException($"{nameof(OnInitInputArgs)} or {nameof(GQIDMS)} is null.");

            _dms = args.DMS;
            _logger = args.Logger;

            _agentIDToName = LoadAgents()
                .ToDictionary(agentInfo => agentInfo.ID, agentInfo => agentInfo.AgentName);

            return default;
        }

        public void OnStartUpdates(IGQIUpdater updater)
        {
            _logger.Debug("OnStartUpdates");

            _subscriptionID = "DS-Swarmable-Elements-" + Guid.NewGuid();
            var connection = _dms.GetConnection();

            connection.OnNewMessage += (obj, args) =>
            {
                if ((args == null) || !args.FromSet(_subscriptionID))
                    return;

                if (!(args.Message is ElementStateEventMessage elementStateEvent))
                    return;

                var elementID = new ElementID(elementStateEvent.DataMinerID, elementStateEvent.ElementID);

                _logger.Debug($"Observed event for {elementID} with State '{elementStateEvent.State}' and TimeoutSubState '{elementStateEvent.TimeoutSubState}' and host {elementStateEvent.HostingAgentID}");

                lock (_elementToHostAndState)
                {
                    if (_elementToHostAndState.TryGetValue(elementID, out var hostAndState))
                    {
                        var oldHostingAgentID = hostAndState.Item1;
                        var newHostingAgentID = elementStateEvent.HostingAgentID;

                        if (oldHostingAgentID != newHostingAgentID)
                        {
                            // update host
                            _logger.Information($"Updating host for element {elementID} to {newHostingAgentID}");
                            updater.UpdateCell(elementID.ToString(), _hostingAgentNameColumn, ToName(newHostingAgentID));
                        }

                        var oldState = hostAndState.Item2;
                        var newState = ToDisplayFriendlyState(elementStateEvent);
                        if (oldState != newState)
                        {
                            // update state
                            _logger.Information($"Updating state for element {elementID} to {newState}");
                            updater.UpdateCell(elementID.ToString(), _stateColumn, newState);
                        }

                        // update in memory cache
                        _elementToHostAndState[elementID] = (newHostingAgentID, newState);
                    }
                }
            };

            connection.AddSubscription(
                _subscriptionID,
                new SubscriptionFilter(typeof(ElementStateEventMessage)));
        }

        public GQIPage GetNextPage(GetNextPageInputArgs args)
        {
            var elements = LoadElements();

            lock (_elementToHostAndState)
            {
                _elementToHostAndState = elements
                    .ToDictionary(
                    tup => new ElementID(tup.Item1.DataMinerID, tup.Item1.ElementID),
                    tup => (tup.Item1.HostingAgentID, ToDisplayFriendlyState(tup.Item2)));
            }

            return new GQIPage(elements.Select(ToRow).ToArray())
            {
                HasNextPage = false,
            };
        }

        public void OnStopUpdates()
        {
            if (_subscriptionID != null)
            {
                _dms.GetConnection().RemoveSubscription(_subscriptionID);
                _subscriptionID = null;
            }
        }
        private GQIRow ToRow((ElementInfoEventMessage, ElementStateEventMessage) element)
        {
            var elementInfo = element.Item1;
            var elementId = new ElementID(elementInfo.DataMinerID, elementInfo.ElementID);
            var hostingAgent = ToName(elementInfo.HostingAgentID);
            var elementDisplayState = ToDisplayFriendlyState(element.Item2);
            return new GQIRow(
                elementId.ToString(),
                new[]
                {
                    new GQICell() { Value = elementId.ToString(), DisplayValue = elementId.ToString() },
                    new GQICell() { Value = elementInfo.Name, DisplayValue = elementInfo.Name },
                    new GQICell() { Value = elementDisplayState, DisplayValue = elementDisplayState },
                    new GQICell() { Value = hostingAgent, DisplayValue = hostingAgent },
                    new GQICell() { Value = elementInfo.IsSwarmable, DisplayValue = elementInfo.IsSwarmable.ToString() },
                });
        }

        private string ToName(int hostingAgentID)
            => _agentIDToName.TryGetValue(hostingAgentID, out var agentName)
                ? agentName
                : $"<Unknown id {hostingAgentID}>";

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
                throw new DataMinerSecurityException($"Issue occurred in {nameof(SwarmableElements)} when sending request {nameof(GetInfoMessage)}.{InfoType.DataMinerInfo}: {ex}", ex);
            }

            if (resp == null || resp.Length == 0)
                throw new Exception($"Response is null or empty");

            var dmaResponses = resp.OfType<GetDataMinerInfoResponseMessage>().ToArray();
            if (dmaResponses.Length == 0)
                throw new Exception($"{nameof(dmaResponses)} is empty");

            return dmaResponses;
        }

        private (ElementInfoEventMessage, ElementStateEventMessage)[] LoadElements()
        {
            if (_dms == null)
                throw new ArgumentNullException($"{nameof(GQIDMS)} is null.");

            DMSMessage[] resp = null;
            try
            {
                var getInfo = new GetInfoMessage(InfoType.ElementInfo);
                var getState = new GetEventsFromCacheMessage(new SubscriptionFilter(nameof(ElementStateEventMessage)));
                resp = _dms.SendMessages(getInfo, getState);
            }
            catch (Exception ex)
            {
                throw new DataMinerSecurityException($"Issue occurred in {nameof(SwarmableElements)} when sending request {nameof(GetInfoMessage)}.{InfoType.ElementInfo}: {ex}", ex);
            }

            if (resp == null)
                throw new Exception($"Response is null");

            var elementStates = resp.OfType<ElementStateEventMessage>().ToArray();
            var elementInfos = resp.OfType<ElementInfoEventMessage>().ToArray();

            return elementInfos
                .Select(info => (info, elementStates.FirstOrDefault(state => state.DataMinerID == info.DataMinerID && state.ElementID == info.ElementID)))
                .Where(tup => tup.Item2 != null)
                .ToArray();
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
}
