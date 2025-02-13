using System;
using System.Linq;
using Skyline.DataMiner.Automation;
using Skyline.DataMiner.Net.Messages;
using Swarming_Playground_Shared;

namespace SwarmAwayAllElementsFromAgents
{
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
                engine.ExitFail("Must at least provide one element!");

            if (!agentInfos.Select(agentinfo => agentinfo.ID).Except(sourceAgentIds).Any())
                engine.ExitFail("Cannot swarm away all elements from all agents");

            if (!agentInfos.Where(agentInfo => agentInfo.ConnectionState == DataMinerAgentConnectionState.Normal).Select(agentinfo => agentinfo.ID).Except(sourceAgentIds).Any())
                engine.ExitFail("Must at least provide one element!");

            foreach (var sourceAgentId in sourceAgentIds)
            {
                if (!agentInfos.Any(agentInfo => agentInfo.ID == sourceAgentId))
                    engine.ExitFail($"Source agent '{sourceAgentId}' is not part of the cluster");
            }

            var clusterConfig = new ClusterConfig(engine, agentInfos, elementInfos);

            clusterConfig.RedistributeAwayFromAgents(sourceAgentIds);

            clusterConfig.SwarmElements();
        }
    }
}
