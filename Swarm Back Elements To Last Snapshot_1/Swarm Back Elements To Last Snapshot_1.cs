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

namespace Swarm_Back_Elements_To_Last_Snapshot_1
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using Skyline.DataMiner.Automation;
    using Skyline.DataMiner.Net;
    using Skyline.DataMiner.Net.Messages;
    using Skyline.DataMiner.Net.PerformanceIndication;
    using Skyline.DataMiner.Net.Swarming;
    using Skyline.DataMiner.Net.Swarming.Helper;
    using Swarming_Playground;

    /// <summary>
    /// Represents a DataMiner Automation script.
    /// </summary>
	public class Script
	{
        private const string PARAM_TARGET_AGENT_IDS = "Target Agent IDs";

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

            var targetAgentIds = engine.GetScriptParamInts(PARAM_TARGET_AGENT_IDS);

            foreach (var targetAgentId in targetAgentIds)
            {
                if (!agentInfos.Any(agentInfo => agentInfo.ID == targetAgentId))
                    engine.ExitFail($"Target agent '{targetAgentId}' is not part of the cluster");
            }

            var elementsToSwarm = new Dictionary<int, List<ElementInfoEventMessage>>();
            var elementInfos = engine.GetElements();
            foreach (var elementInfo in elementInfos)
			{
				if (!elementInfo.IsSwarmable)
					continue;

				var theProperty = elementInfo.Properties.FirstOrDefault(prop => prop.Name == Constants.SWARMING_PLAYGROUND_HOME_DMA_PROPERTY_NAME);
				if(theProperty == null)
					continue;

				if(!int.TryParse(theProperty.Value, out var targetAgentId))
					continue;

				if (!targetAgentIds.Contains(targetAgentId))
					continue;

				if(!elementsToSwarm.TryGetValue(targetAgentId, out var list))
				{
					list = new List<ElementInfoEventMessage>();
					elementsToSwarm[targetAgentId] = list;
				}

				list.Add(elementInfo);
            }

            var failures = new ConcurrentBag<SwarmingResult>();
            Parallel.ForEach(elementsToSwarm, kvp =>
            {
                var targetAgentId = kvp.Key;
                var elements = kvp.Value;

                engine.Log($"Swarming elements to agent {targetAgentId}: " + string.Join(", ", elements.Select(info => info.Name)));

                var responses = SwarmingHelper.Create(engine.SendSLNetMessage)
                    .SwarmElements(elements.Select(info => new ElementID(info.DataMinerID, info.ElementID)).ToArray())
                    .ToAgent(targetAgentId);

                foreach (var failure in responses.Where(resp => !resp.Success))
                    failures.Add(failure);
            });

            if (failures.Any())
            {
                var summary = new StringBuilder();
                summary.AppendLine($"Swarming failed for {failures.Count} element(s):");
                foreach (var failure in failures)
                    summary.AppendLine($"\t- {failure.DmaObjectRef}: {failure.Message}");
                engine.ExitFail(summary.ToString());
            }
        }
    }
}