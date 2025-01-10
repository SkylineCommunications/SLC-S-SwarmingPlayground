/*
****************************************************************************
*  Copyright (c) 2024,  Skyline Communications NV  All Rights Reserved.    *
****************************************************************************

By using this script, you expressly agree with the usage terms and
conditions set out below.
This script and all related materials are protected by copyrights and
other intellectual property rights that exclusively belong
to Skyline Communications.

A user license granted for this script is strictly for personal use only.
This script may not be used in any way by anyone without the prior
written consent of Skyline Communications. Any sublicensing of this
script is forbidden.

Any modifications to this script by the user are only allowed for
personal use and within the intended purpose of the script,
and will remain the sole responsibility of the user.
Skyline Communications will not be responsible for any damages or
malfunctions whatsoever of the script resulting from a modification
or adaptation by the user.

The content of this script is confidential information.
The user hereby agrees to keep this confidential information strictly
secret and confidential and not to disclose or reveal it, in whole
or in part, directly or indirectly to any person, entity, organization
or administration without the prior written consent of
Skyline Communications.

Any inquiries can be addressed to:

	Skyline Communications NV
	Ambachtenstraat 33
	B-8870 Izegem
	Belgium
	Tel.	: +32 51 31 35 69
	Fax.	: +32 51 31 01 29
	E-mail	: info@skyline.be
	Web		: www.skyline.be
	Contact	: Ben Vandenberghe

****************************************************************************
Revision History:

DATE		VERSION		AUTHOR			COMMENTS

dd/mm/2024	1.0.0.1		XXX, Skyline	Initial version
****************************************************************************
*/

namespace Swarmable_Elements_1
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Skyline.DataMiner.Analytics.GenericInterface;
    using Skyline.DataMiner.Net;
    using Skyline.DataMiner.Net.Exceptions;
    using Skyline.DataMiner.Net.Messages;

    [GQIMetaData(Name = "Swarmable Elements")]
    public class SwarmableElements : IGQIDataSource, IGQIOnInit, IGQIUpdateable
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

            _agentIDToName = LoadAgents().ToDictionary(agentInfo => agentInfo.ID, agentInfo => agentInfo.AgentName);

            return default;
        }

        public GQIPage GetNextPage(GetNextPageInputArgs args)
        {
            var elementInfos = LoadElements();

            lock(_elementToHostAndState)
            {
                _elementToHostAndState = elementInfos
                    .ToDictionary(
                    info => new ElementID(info.DataMinerID, info.ElementID),
                    info => (info.HostingAgentID, info.State.ToString()));
            }

            return new GQIPage(elementInfos.Select(elementInfo => ToRow(elementInfo)).ToArray())
            {
                HasNextPage = false,
            };
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
                    if(_elementToHostAndState.TryGetValue(elementID, out var hostAndState))
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
                        if(oldState != newState)
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

        public void OnStopUpdates()
        {
            if (_subscriptionID != null)
            {
                _dms.GetConnection().RemoveSubscription(_subscriptionID);
                _subscriptionID = null;
            }
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
                throw new DataMinerSecurityException($"Issue occurred in {nameof(SwarmableElements)} when sending request {nameof(GetInfoMessage)}.{InfoType.DataMinerInfo}: {ex}", ex);
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
                throw new DataMinerSecurityException($"Issue occurred in {nameof(SwarmableElements)} when sending request {nameof(GetInfoMessage)}.{InfoType.ElementInfo}: {ex}", ex);
            }

            if (resp == null)
                throw new Exception($"Response is null");

            return resp.OfType<ElementInfoEventMessage>().ToArray();
        }

        private GQIRow ToRow(ElementInfoEventMessage elementInfo)
        {
            var elementId = new ElementID(elementInfo.DataMinerID, elementInfo.ElementID);
            var hostingAgent = ToName(elementInfo.HostingAgentID);
            return new GQIRow(
                    elementId.ToString(),
                new[]
                {
                new GQICell() { Value = elementId.ToString(), DisplayValue = elementId.ToString() },
                        new GQICell() { Value = elementInfo.Name, DisplayValue = elementInfo.Name },
                        new GQICell() { Value = elementInfo.State.ToString(), DisplayValue = elementInfo.State.ToString() },
                        new GQICell() { Value = hostingAgent, DisplayValue = hostingAgent },
                        new GQICell() { Value = elementInfo.IsSwarmable, DisplayValue = elementInfo.IsSwarmable.ToString() },
                    });
        }

        private string ToName(int hostingAgentID)
            => _agentIDToName.TryGetValue(hostingAgentID, out var agentName)
                ? agentName
                : $"<Unknown id {hostingAgentID}>";

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