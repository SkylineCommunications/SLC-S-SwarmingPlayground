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

namespace Swarming_Prerequisites_1
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Skyline.DataMiner.Analytics.GenericInterface;
    using Skyline.DataMiner.Net.Exceptions;
    using Skyline.DataMiner.Net.Messages;
    using Skyline.DataMiner.Net.Swarming;

    [GQIMetaData(Name = "Swarming Prerequisites")]
    public class SwarmingPrerequisites : IGQIDataSource, IGQIOnInit
    {
        private GQIDMS _dms;
        private IGQILogger _logger;

        private readonly GQIColumn[] _columns = new GQIColumn[]
        {
            new GQIIntColumn("Agent ID"),
            new GQIStringColumn("Agent Name"),

            new GQIBooleanColumn("Dedicated Clustered Database"),
            new GQIBooleanColumn("No Failover"),
            new GQIBooleanColumn("No Central Database"),
            new GQIBooleanColumn("No Legacy Dashboards And Reports"),
            new GQIBooleanColumn("No Incompatible Enhanced Services"),
            new GQIBooleanColumn("No Incompatible SLAs"),

            //new GQIBooleanColumn("No Incompatible Scripts"),
            //new GQIBooleanColumn("No Incompatible QActions"),
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
            var agentInfos = LoadAgents();

            var rows = new List<GQIRow>(agentInfos.Length);
            foreach (var agentInfo in agentInfos)
            {
                var prereqResp = CheckPrerequisites(agentInfo.ID);

                rows.Add(new GQIRow(
                    agentInfo.ID.ToString(),
                    new[]
                    {
                        new GQICell() { Value = agentInfo.ID, DisplayValue = agentInfo.ID.ToString() },
                        new GQICell() { Value = agentInfo.AgentName, DisplayValue = agentInfo.AgentName },

                        new GQICell() { Value = prereqResp.SupportedDatabase, DisplayValue = prereqResp.SupportedDatabase.ToString() },
                        new GQICell() { Value = prereqResp.SupportedDMS, DisplayValue = prereqResp.SupportedDMS.ToString() },
                        new GQICell() { Value = prereqResp.CentralDatabaseNotConfigured, DisplayValue = prereqResp.CentralDatabaseNotConfigured.ToString() },
                        new GQICell() { Value = prereqResp.LegacyReportsAndDashboardsDisabled, DisplayValue = prereqResp.LegacyReportsAndDashboardsDisabled.ToString() },
                        new GQICell() { Value = prereqResp.NoIncompatibleEnhancedServicesOnDMS, DisplayValue = prereqResp.NoIncompatibleEnhancedServicesOnDMS.ToString() },
                        new GQICell() { Value = prereqResp.NoIncompatibleSLAsOnDMS, DisplayValue = prereqResp.NoIncompatibleSLAsOnDMS.ToString() },
                    }));
            }

            return new GQIPage(rows.ToArray())
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
                throw new DataMinerSecurityException($"Issue occurred in {nameof(SwarmingPrerequisites)} when sending request {nameof(GetInfoMessage)}.{InfoType.DataMinerInfo}: {ex}", ex);
            }

            if (resp == null || resp.Length == 0)
                throw new Exception($"Response is null or empty");

            var dmaResponses = resp.OfType<GetDataMinerInfoResponseMessage>().ToArray();
            if (dmaResponses.Length == 0)
                throw new Exception($"{nameof(dmaResponses)} is empty");

            return dmaResponses;
        }

        private SwarmingPrerequisitesCheckResponse CheckPrerequisites(int dataMinerID)
        {
            if (_dms == null)
                throw new ArgumentNullException($"{nameof(GQIDMS)} is null.");

            DMSMessage[] resp = null;
            try
            {
                var req = new SwarmingPrerequisitesCheckRequest()
                {
                    // skip alarmids
                    HostingDataMinerID = dataMinerID,
                    AnalyzeAlarmIDUsage = false,
                };
                resp = _dms.SendMessages(req);
            }
            catch (Exception ex)
            {
                throw new DataMinerSecurityException($"Issue occurred in {nameof(SwarmingPrerequisites)} when sending request {nameof(SwarmingPrerequisitesCheckRequest)}: {ex}", ex);
            }

            if (resp == null || resp.Length == 0)
                throw new Exception($"Response is null or empty");

            var dmaResponses = resp.OfType<SwarmingPrerequisitesCheckResponse>().ToArray();
            if (dmaResponses.Length != 1)
                throw new Exception($"{nameof(dmaResponses)} does not contain exactly 1 response");

            return dmaResponses.First();
        }
    }
}
