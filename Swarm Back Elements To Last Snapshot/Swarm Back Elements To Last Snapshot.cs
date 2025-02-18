using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Skyline.DataMiner.Automation;
using Skyline.DataMiner.Net;
using Skyline.DataMiner.Net.Messages;
using Skyline.DataMiner.Net.Swarming;
using Skyline.DataMiner.Net.Swarming.Helper;
using Swarming_Playground_Shared;

namespace SwarmBackElementsToLastSnapshot
{
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
                if (theProperty == null)
                    continue;

                if (!int.TryParse(theProperty.Value, out var targetAgentId))
                    continue;

                if (!targetAgentIds.Contains(targetAgentId))
                    continue;

                if (!elementsToSwarm.TryGetValue(targetAgentId, out var list))
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
