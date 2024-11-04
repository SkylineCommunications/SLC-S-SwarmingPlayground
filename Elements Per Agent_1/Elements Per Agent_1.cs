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

namespace Elements_Per_Agent_1
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using Skyline.DataMiner.Analytics.GenericInterface;
    using Skyline.DataMiner.Net;
    using Skyline.DataMiner.Net.Exceptions;
    using Skyline.DataMiner.Net.Messages;

    [GQIMetaData(Name = "Elements Per Agent")]
    public class ElementsPerAgents : IGQIDataSource, IGQIOnInit
    {
        private GQIDMS _dms;
        private Dictionary<int, GetDataMinerInfoResponseMessage> _agentInfos;

        private readonly GQIColumn[] _columns = new GQIColumn[]
        {
            new GQIIntColumn("Agent ID"),
            new GQIStringColumn("Agent Name"),
            new GQIStringColumn("Agent State"),

            new GQIStringColumn("Element ID"),
            new GQIStringColumn("Element Name"),
            new GQIStringColumn("Element State"),
            new GQIBooleanColumn("Swarmable"),
        };
        public GQIColumn[] GetColumns() => _columns;

        public OnInitOutputArgs OnInit(OnInitInputArgs args)
        {
            if (args?.DMS == null)
                throw new ArgumentNullException($"{nameof(OnInitInputArgs)} or {nameof(GQIDMS)} is null.");

            _dms = args.DMS;

            LoadAgents();

            return default;
        }

        public GQIPage GetNextPage(GetNextPageInputArgs args)
        {
            var elementInfos = LoadElements();
            var rows = new List<GQIRow>(elementInfos.Length);
            foreach (var elementInfo in elementInfos)
            {
                if(!_agentInfos.TryGetValue(elementInfo.HostingAgentID, out var agentInfo))
                    throw new DataMinerSecurityException($"Issue occurred in {nameof(ElementsPerAgents)}, element {elementInfo.Name} has an unknown hosting agent: {elementInfo.HostingAgentID}");

                var elementID = new ElementID(elementInfo.DataMinerID, elementInfo.ElementID);

                rows.Add(new GQIRow(new GQICell[]
                {
                    new GQICell() { Value = agentInfo.ID, DisplayValue = agentInfo.ID.ToString() },
                    new GQICell() { Value = agentInfo.AgentName, DisplayValue = agentInfo.AgentName },
                    new GQICell() { Value = agentInfo.ConnectionState.ToString(), DisplayValue = agentInfo.ConnectionState.ToString() },

                    new GQICell() { Value = elementID.ToString(), DisplayValue = elementID.ToString() },
                    new GQICell() { Value = elementInfo.Name, DisplayValue = elementInfo.Name },
                    new GQICell() { Value = elementInfo.State.ToString() , DisplayValue = elementInfo.State.ToString() },
                    new GQICell() { Value = elementInfo.IsSwarmable, DisplayValue = elementInfo.IsSwarmable.ToString() },
                }));
            }

            return new GQIPage(rows.ToArray())
            {
                HasNextPage = false,
            };
        }

        private void LoadAgents()
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
                throw new DataMinerSecurityException($"Issue occurred in {nameof(ElementsPerAgents)} when sending request {nameof(GetInfoMessage)}.{InfoType.DataMinerInfo}: {ex}", ex);
            }

            if (resp == null || resp.Length == 0)
                throw new Exception($"Response is null or empty");

            var dmaResponses = resp.OfType<GetDataMinerInfoResponseMessage>().ToArray();
            if (dmaResponses.Length == 0)
                throw new Exception($"{nameof(dmaResponses)} is empty");

            _agentInfos = dmaResponses.ToDictionary(agentInfo => agentInfo.ID, agentInfo => agentInfo);
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
                throw new DataMinerSecurityException($"Issue occurred in {nameof(ElementsPerAgents)} when sending request {nameof(GetInfoMessage)}.{InfoType.ElementInfo}: {ex}", ex);
            }

            if (resp == null || resp.Length == 0)
                throw new Exception($"Response is null or empty");

            return resp.OfType<ElementInfoEventMessage>().ToArray();
        }
    }
}