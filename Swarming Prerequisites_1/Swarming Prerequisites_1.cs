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
    using System.Linq;
    using Skyline.DataMiner.Analytics.GenericInterface;
    using Skyline.DataMiner.Net.Exceptions;
    using Skyline.DataMiner.Net.Messages;
    using Skyline.DataMiner.Net.Swarming;

    [GQIMetaData(Name = "Swarming Prerequisites")]
    public class SwarmingPrerequisites : IGQIDataSource, IGQIOnInit, IGQIInputArguments
    {
        private GQIDMS _dms;
        private IGQILogger _logger;
        private bool _analyzeAlarmIDs = false;
        private bool _onlyCheckLocalDMA = false;

        private static readonly GQIBooleanArgument _analyzeAlarmIDsArgument = new GQIBooleanArgument("Analyze AlarmIDs");
        private static readonly GQIBooleanArgument _onlyCheckLocalDMAArgument = new GQIBooleanArgument("Only check local DMA");

        private readonly GQIColumn[] _columns = new GQIColumn[]
        {
            new GQIBooleanColumn("Swarming Enabled"),

            new GQIBooleanColumn("Dedicated Clustered Database"),
            new GQIBooleanColumn("No Failover"),
            new GQIBooleanColumn("No Central Database"),
            new GQIBooleanColumn("No Legacy Dashboards And Reports"),
            new GQIBooleanColumn("No Incompatible Enhanced Services"),

            new GQIBooleanColumn("No Incompatible Scripts"),
            new GQIBooleanColumn("No Incompatible QActions"),

            new GQIBooleanColumn("Finished Successfully"),

            new GQIStringColumn("Summary"),
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

        public GQIArgument[] GetInputArguments() => new[] { _analyzeAlarmIDsArgument, _onlyCheckLocalDMAArgument };

        public OnArgumentsProcessedOutputArgs OnArgumentsProcessed(OnArgumentsProcessedInputArgs args)
        {
            if (args.TryGetArgumentValue(_analyzeAlarmIDsArgument, out var shoudAnalyzeAlarmIDs))
                _analyzeAlarmIDs = shoudAnalyzeAlarmIDs;

            if (args.TryGetArgumentValue(_onlyCheckLocalDMAArgument, out var onlyCheckLocalDMA))
                _onlyCheckLocalDMA = onlyCheckLocalDMA;

            return default;
        }

        public GQIPage GetNextPage(GetNextPageInputArgs args)
        {
            var localInfo = _dms.SendMessages(new GetInfoMessage(InfoType.LocalDataMinerInfo))
                .OfType<GetDataMinerInfoResponseMessage>()
                .FirstOrDefault();

            if (localInfo == null)
                throw new DataMinerException("Failed to gather local dataminer info");

            try
            {
                var prereqResp = CheckPrerequisites(localInfo.ID);
                return PrerequisiteResponseToGQIPage(localInfo.IsSwarmingEnabled, true, prereqResp);
            }
            catch (Exception ex)
            {
                var resp = new SwarmingPrerequisitesCheckResponse
                {
                    // all flags default false
                    Summary = ex.Message
                };
                return PrerequisiteResponseToGQIPage(localInfo.IsSwarmingEnabled, false, resp);
            }
        }

        private SwarmingPrerequisitesCheckResponse CheckPrerequisites(int localDataMinerID)
        {
            if (_dms == null)
                throw new ArgumentNullException($"{nameof(GQIDMS)} is null.");

            var req = new SwarmingPrerequisitesCheckRequest()
            {
                AnalyzeAlarmIDUsage = _analyzeAlarmIDs,
            };

            if (_onlyCheckLocalDMA)
                req.DataMinerID = localDataMinerID;

            var resp = _dms.SendMessages(req);

            if (resp == null || resp.Length == 0)
                throw new Exception($"Response is null or empty");

            var dmaResponses = resp.OfType<SwarmingPrerequisitesCheckResponse>().ToArray();
            if (dmaResponses.Length != 1)
                throw new Exception($"{nameof(dmaResponses)} does not contain exactly 1 response");

            return dmaResponses.First();
        }

        private GQIPage PrerequisiteResponseToGQIPage(bool isSwarmingEnabled, bool success, SwarmingPrerequisitesCheckResponse resp)
        {
            return new GQIPage(new[] { new GQIRow(
                new[]
                {
                    new GQICell() { Value = isSwarmingEnabled, DisplayValue = isSwarmingEnabled.ToString() },

                    new GQICell() { Value = resp.SupportedDatabase, DisplayValue = resp.SupportedDatabase.ToString() },
                    new GQICell() { Value = resp.SupportedDMS, DisplayValue = resp.SupportedDMS.ToString() },
                    new GQICell() { Value = resp.CentralDatabaseNotConfigured, DisplayValue = resp.CentralDatabaseNotConfigured.ToString() },
                    new GQICell() { Value = resp.LegacyReportsAndDashboardsDisabled, DisplayValue = resp.LegacyReportsAndDashboardsDisabled.ToString() },
                    new GQICell() { Value = resp.NoIncompatibleEnhancedServicesOnDMS, DisplayValue = resp.NoIncompatibleEnhancedServicesOnDMS.ToString() },

                    new GQICell() { Value = resp.NoObsoleteAlarmIdUsageInScripts, DisplayValue = resp.NoObsoleteAlarmIdUsageInScripts.ToString() },
                    new GQICell() { Value = resp.NoObsoleteAlarmIdUsageInProtocolQActions, DisplayValue = resp.NoObsoleteAlarmIdUsageInProtocolQActions.ToString() },
                    
                    new GQICell() { Value = success, DisplayValue = success.ToString() },

                    new GQICell() { Value = resp.Summary, DisplayValue = resp.Summary },
                })})
            {
                HasNextPage = false,
            };
        }
    }
}
