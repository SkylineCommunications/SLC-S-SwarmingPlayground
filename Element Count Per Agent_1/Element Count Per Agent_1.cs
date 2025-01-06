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

        private object _dictLock = new object();
        private Dictionary<int, GetDataMinerInfoResponseMessage> _agentInfoCache = new Dictionary<int, GetDataMinerInfoResponseMessage>();
        private Dictionary<ElementID, ElementInfoEventMessage> _elementInfoCache = new Dictionary<ElementID, ElementInfoEventMessage>();

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

        public void OnStopUpdates()
        {
            _logger.Information("OnStopUpdates");
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

                if(elementInfo.IsDeleted)
                    _elementInfoCache.Remove(elementId);
                else
                    _elementInfoCache[elementId] = elementInfo;

                if(oldEntry is null || elementInfo.IsDeleted)
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

            lock(_dictLock)
            {
                _agentInfoCache[dataMinerInfo.DataMinerID] = dataMinerInfo.Raw;
            }

            updater.UpdateCell(dataMinerInfo.DataMinerID.ToString(), _stateColumn, dataMinerInfo.Raw.ConnectionState.ToString());
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

        private GQIRow GetRowForAgent(int dataMinerID)
        {
            GetDataMinerInfoResponseMessage agentInfo = null;
            var swarmableCount = 0;
            var nonSwarmableCount = 0;
            lock (_dictLock)
            {
                if (!_agentInfoCache.TryGetValue(dataMinerID, out agentInfo) || agentInfo is null)
                    return null;

                foreach(var elementInfo in _elementInfoCache.Values)
                {
                    if(elementInfo.HostingAgentID != dataMinerID)
                        continue;

                    if(elementInfo.IsSwarmable)
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
    }

    public static class Extensions
    {
        public static ElementID ToElementID(this ElementInfoEventMessage elementInfo)
            => new ElementID(elementInfo.DataMinerID, elementInfo.ElementID);
    }
}