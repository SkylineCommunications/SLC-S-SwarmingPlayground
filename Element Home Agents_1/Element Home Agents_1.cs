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

namespace Element_Home_Agents_1
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Skyline.DataMiner.Analytics.GenericInterface;
    using Skyline.DataMiner.Net;
    using Skyline.DataMiner.Net.Exceptions;
    using Skyline.DataMiner.Net.Messages;

    [GQIMetaData(Name = "Element Home Agents")]
    public class ElementHomeAgents : IGQIDataSource, IGQIOnInit
	{
        public const string SWARMING_PLAYGROUND_HOME_DMA_PROPERTY_NAME = "Swarming Playground Home DataMiner ID";

        private GQIDMS _dms;
        private IGQILogger _logger;
        private Dictionary<int, string> _agentIDToName = new Dictionary<int, string>();

        private readonly GQIColumn[] _columns = new GQIColumn[]
        {
            new GQIStringColumn("ElementID"),
            new GQIStringColumn("Element Name"),
            new GQIIntColumn("Home Agent ID"),
            new GQIStringColumn("Home Agent Name"),
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
            var rows = LoadElements()
                .Where(elementInfo => elementInfo.IsSwarmable)
                .Select(elementInfo => ToRow(elementInfo))
                .ToArray();

            return new GQIPage(rows)
            {
                HasNextPage = false,
            };
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
                throw new DataMinerSecurityException($"Issue occurred in {nameof(ElementHomeAgents)} when sending request {nameof(GetInfoMessage)}.{InfoType.DataMinerInfo}: {ex}", ex);
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
                throw new DataMinerSecurityException($"Issue occurred in {nameof(ElementHomeAgents)} when sending request {nameof(GetInfoMessage)}.{InfoType.ElementInfo}: {ex}", ex);
            }

            if (resp == null || resp.Length == 0)
                throw new Exception($"Response is null or empty");

            return resp.OfType<ElementInfoEventMessage>().ToArray();
        }

        private GQIRow ToRow(ElementInfoEventMessage elementInfo)
        {
            var elementId = new ElementID(elementInfo.DataMinerID, elementInfo.ElementID);
            var homeAgentID = -1;
            if (int.TryParse(elementInfo.GetPropertyValue(SWARMING_PLAYGROUND_HOME_DMA_PROPERTY_NAME), out var parsed))
                homeAgentID = parsed;
            var homeAgentName = ToName(homeAgentID);
            return new GQIRow(
                    elementId.ToString(),
                    new[]
                    {
                        new GQICell() { Value = elementId.ToString(), DisplayValue = elementId.ToString() },
                        new GQICell() { Value = elementInfo.Name, DisplayValue = elementInfo.Name },
                        new GQICell() { Value = homeAgentID, DisplayValue = homeAgentID.ToString() },
                        new GQICell() { Value = homeAgentName, DisplayValue = homeAgentName },
                    });
        }

        private string ToName(int dmaID)
            => _agentIDToName.TryGetValue(dmaID, out var agentName)
                ? agentName
                : $"<No Home>";
    }
}