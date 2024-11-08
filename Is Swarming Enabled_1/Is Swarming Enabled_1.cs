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

namespace IsSwarmingEnabled_1
{
    using System;
    using System.Linq;
    using Skyline.DataMiner.Analytics.GenericInterface;
    using Skyline.DataMiner.Net.Exceptions;
    using Skyline.DataMiner.Net.Messages;

    [GQIMetaData(Name = "Is Swarming Enabled")]
    public class IsSwarmingEnabled : IGQIDataSource, IGQIOnInit
	{
        private GQIDMS _dms;
        private IGQILogger _logger;

        private readonly GQIColumn[] _columns = new GQIColumn[]
        {
            new GQIBooleanColumn("Is Swarming Enabled"),
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

            if (agentInfos.All(agentInfo => agentInfo.IsSwarmingEnabled))
            {
                return BoolToPage(true);
            }
            else if (agentInfos.All(agentInfo => !agentInfo.IsSwarmingEnabled))
            {
                return BoolToPage(false);
            }
            else
            {
                var swarmingCount = agentInfos.Count(agentInfo => agentInfo.IsSwarmingEnabled);
                var totalCount = agentInfos.Length;
                throw new DataMinerException($"Invalid configuration detected, cluster has mixed Swarming config: {swarmingCount}/{totalCount} enabled");
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
                throw new DataMinerSecurityException($"Issue occurred in {nameof(IsSwarmingEnabled)} when sending request {nameof(GetInfoMessage)}.{InfoType.DataMinerInfo}: {ex}", ex);
            }

            if (resp == null || resp.Length == 0)
                throw new Exception($"Response is null or empty");

            var dmaResponses = resp.OfType<GetDataMinerInfoResponseMessage>().ToArray();
            if (dmaResponses.Length == 0)
                throw new Exception($"{nameof(dmaResponses)} is empty");

            return dmaResponses;
        }

        private GQIPage BoolToPage(bool value)
        {
            return new GQIPage(new[]
                    {
                        new GQIRow(new[]
                        {
                            new GQICell() { Value = value, DisplayValue = value.ToString() },
                        }),
                    })
                {
                    HasNextPage = false,
                };
        }
    }
}