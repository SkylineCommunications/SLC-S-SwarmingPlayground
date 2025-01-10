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

namespace Swarm_Away_All_Elements_From_Agents_1
{
    using System;
    using System.Linq;
    using Skyline.DataMiner.Automation;
    using Skyline.DataMiner.Net.Messages;
    using Swarming_Playground;

    /// <summary>
    /// Represents a DataMiner Automation script.
    /// </summary>
    public class Script
    {
        private const string PARAM_SOURCE_AGENT_IDS = "Source Agent IDs";

        /// <summary>
        /// The script entry point.
        /// </summary>
        /// <param name="engine">Link with SLAutomation process.</param>
        public void Run(IEngine engine)
        {
            try
            {
                RunSafe(engine);
            }
            catch (ScriptAbortException)
            {
                // Catch normal abort exceptions (engine.ExitFail or engine.ExitSuccess)
                throw; // Comment if it should be treated as a normal exit of the script.
            }
            catch (ScriptForceAbortException)
            {
                // Catch forced abort exceptions, caused via external maintenance messages.
                throw;
            }
            catch (ScriptTimeoutException)
            {
                // Catch timeout exceptions for when a script has been running for too long.
                throw;
            }
            catch (InteractiveUserDetachedException)
            {
                // Catch a user detaching from the interactive script by closing the window.
                // Only applicable for interactive scripts, can be removed for non-interactive scripts.
                throw;
            }
            catch (Exception e)
            {
                engine.ExitFail("Run|Something went wrong: " + e);
            }
        }

        private void RunSafe(IEngine engine)
        {
            var agentInfos = engine.GetAgents();

            if (!Check.IfSwarmingIsEnabled(agentInfos))
                engine.ExitFail("Swarming is not enabled in this DMS. More info: https://aka.dataminer.services/Swarming");

            var elementInfos = engine.GetElements();

            var sourceAgentIds = engine.GetScriptParamInts(PARAM_SOURCE_AGENT_IDS);

            if (!sourceAgentIds.Any())
                throw new ArgumentException("Must at least provide one agent");

            if (!agentInfos.Select(agentinfo => agentinfo.ID).Except(sourceAgentIds).Any())
                throw new ArgumentException("Cannot swarm away all elements from all agents");

            if (!agentInfos.Where(agentInfo => agentInfo.ConnectionState == DataMinerAgentConnectionState.Normal).Select(agentinfo => agentinfo.ID).Except(sourceAgentIds).Any())
                throw new ArgumentException("Cannot swarm away all elements because there are no healthy agents available");

            foreach (var sourceAgentId in sourceAgentIds)
            {
                if (!agentInfos.Any(agentInfo => agentInfo.ID == sourceAgentId))
                    throw new ArgumentException($"Source agent '{sourceAgentId}' is not part of the cluster");
            }

            var clusterConfig = new ClusterConfig(engine, agentInfos, elementInfos);

            clusterConfig.RedistributeAwayFromAgents(sourceAgentIds);

            clusterConfig.SwarmElements();
        }
    }
}