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

namespace Element_Count_Per_Agent_1
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Skyline.DataMiner.Analytics.GenericInterface;
    using Skyline.DataMiner.Net;
    using Skyline.DataMiner.Net.Exceptions;
    using Skyline.DataMiner.Net.Messages;

    [GQIMetaData(Name = "Element Count Per Agent")]
    public class ElementCountPerAgent : IGQIDataSource, IGQIOnInit, IGQIUpdateable
    {
        private GQIDMS _dms;
        private IGQILogger _logger;
        private string _subscriptionID;

        private GetDataMinerInfoResponseMessage[] _agentInfos;
        private Dictionary<ElementID, (int, bool)> _elementToHost = new Dictionary<ElementID, (int, bool)>();

        private static readonly GQIStringColumn _stateColumn = new GQIStringColumn("Agent State");
        private static readonly GQIIntColumn _nonSwarmableElementCountColumn = new GQIIntColumn("Non-Swarmable Element Count");
        private static readonly GQIIntColumn _swarmableElementCountColumn = new GQIIntColumn("Swarmable Element Count");
        private static readonly GQIIntColumn _totalElementCountColumn = new GQIIntColumn("Total Element Count");

        private readonly GQIColumn[] _columns = new GQIColumn[]
        {
            new GQIIntColumn("Agent ID"),
            new GQIStringColumn("Agent Name"),
            _stateColumn,

            _nonSwarmableElementCountColumn,
            _swarmableElementCountColumn,
            _totalElementCountColumn,
        };

        public GQIColumn[] GetColumns() => _columns;

        public OnInitOutputArgs OnInit(OnInitInputArgs args)
        {
            if (args?.DMS == null)
                throw new ArgumentNullException($"{nameof(OnInitInputArgs)} or {nameof(GQIDMS)} is null.");

            _dms = args.DMS;
            _logger = args.Logger;

            return default;
        }

        public GQIPage GetNextPage(GetNextPageInputArgs args)
        {
            _logger.Information("GetNextPage");

            _agentInfos = LoadAgents();

            var elementInfos = LoadElements();

            lock(_elementToHost)
            {
                _elementToHost = elementInfos.ToDictionary(
                    elementInfo => elementInfo.ToElementID(),
                    elementInfo => (elementInfo.HostingAgentID, elementInfo.IsSwarmable));
            }

            var agentWithElementsCounts = _agentInfos
                .GroupJoin
                    (
                        elementInfos.GroupBy(elementInfo => elementInfo.HostingAgentID),
                        agentInfo => agentInfo.ID,
                        elementInfoGroup => elementInfoGroup.Key,
                        (agentInfo, elementInfoGroups) =>
                        {
                            var nonSwarmableCount = elementInfoGroups.Sum(group => group.Count(elementInfo => !elementInfo.IsSwarmable));
                            var swarmableCount = elementInfoGroups.Sum(group => group.Count(elementInfo => elementInfo.IsSwarmable));
                            var totalCount = nonSwarmableCount + swarmableCount;
                            return (agentInfo, nonSwarmableCount, swarmableCount, totalCount);
                        }
                    )
                .ToArray();

            var rows = new List<GQIRow>(elementInfos.Length);
            foreach (var entry in agentWithElementsCounts)
            {
                var agentInfo = entry.agentInfo;

                rows.Add(new GQIRow(
                    agentInfo.ID.ToString(),
                    new[]
                    {
                        new GQICell() { Value = agentInfo.ID, DisplayValue = agentInfo.ID.ToString() },
                        new GQICell() { Value = agentInfo.AgentName, DisplayValue = agentInfo.AgentName },
                        new GQICell() { Value = agentInfo.ConnectionState.ToString(), DisplayValue = agentInfo.ConnectionState.ToString() },
                        
                        new GQICell() { Value = entry.nonSwarmableCount, DisplayValue = entry.nonSwarmableCount.ToString() },
                        new GQICell() { Value = entry.swarmableCount, DisplayValue = entry.swarmableCount.ToString() },
                        new GQICell() { Value = entry.totalCount, DisplayValue = entry.totalCount.ToString() },
                    }));
            }

            return new GQIPage(rows.ToArray())
            {
                HasNextPage = false,
            };
        }

        public void OnStartUpdates(IGQIUpdater updater)
        {
            _logger.Information("OnStartUpdates");

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

        private void OnElementInfoEventMessage(IGQIUpdater updater, ElementInfoEventMessage elementInfo)
        {
            var elementId = elementInfo.ToElementID();
            var newHost = elementInfo.HostingAgentID;

            _logger.Information($"Observed ElementInfoEvent for '{elementInfo.Name}' ({elementId}){(elementInfo.IsDeleted ? " [Deleted]" : string.Empty)} with host {newHost}");

            lock (_elementToHost)
            {
                int oldHost = -1;
                if(_elementToHost.TryGetValue(elementId, out var oldHostAndSwarmable))
                {
                    oldHost = oldHostAndSwarmable.Item1;
                }

                if (oldHost == newHost)
                    newHost = -1; // not really a new host

                if (elementInfo.IsDeleted)
                {
                    // deleted element
                    _logger.Information($"Removing '{elementInfo.Name}' ({elementId})");
                    _elementToHost.Remove(elementId);
                }
                else if (newHost > 0)
                {
                    // new or swarmed element
                    _logger.Information($"Updating host '{elementInfo.Name}' ({elementId}) to {newHost}");
                    _elementToHost[elementId] = (newHost, elementInfo.IsSwarmable);

                    // Recalculate count for new host
                    UpdateHostCounts(updater, newHost);
                }

                if (oldHost > 0)
                {
                    // Recalculate count for old host
                    UpdateHostCounts(updater, oldHost);
                }
            }
        }

        private void UpdateHostCounts(IGQIUpdater updater, int hostID)
        {
            var totalElements = _elementToHost.Where(element => element.Value.Item1 == hostID).ToArray();
            var nonSwarmableElementsCount = totalElements.Count(element => !element.Value.Item2);
            var swarmableElementsCount = totalElements.Length - nonSwarmableElementsCount;

            updater.UpdateCell(hostID.ToString(), _nonSwarmableElementCountColumn, nonSwarmableElementsCount);
            updater.UpdateCell(hostID.ToString(), _swarmableElementCountColumn, swarmableElementsCount);
            updater.UpdateCell(hostID.ToString(), _totalElementCountColumn, totalElements.Length);
        }

        private void OnDataMinerInfoEvent(IGQIUpdater updater, DataMinerInfoEvent dataMinerInfo)
        {
            _logger.Information($"Observed DataMinerInfoEvent for '{dataMinerInfo.AgentName}' ({dataMinerInfo.DataMinerID}) with state {dataMinerInfo.Raw.ConnectionState}");

            updater.UpdateCell(dataMinerInfo.DataMinerID.ToString(), _stateColumn, dataMinerInfo.Raw.ConnectionState.ToString());
        }

        public void OnStopUpdates()
        {
            _logger.Information("OnStopUpdates");
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

            if (resp == null || resp.Length == 0)
                throw new Exception($"Response is null or empty");

            return resp.OfType<ElementInfoEventMessage>().ToArray();
        }
    }

    public static class Extensions
    {
        public static ElementID ToElementID(this ElementInfoEventMessage elementInfo)
            => new ElementID(elementInfo.DataMinerID, elementInfo.ElementID);
    }
}