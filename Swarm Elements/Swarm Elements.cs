using System;
using System.Linq;
using Newtonsoft.Json;
using Skyline.DataMiner.Automation;
using Skyline.DataMiner.Net;
using Skyline.DataMiner.Net.Swarming.Helper;
using Swarming_Playground_Shared;

namespace SwarmElements
{
    /// <summary>
    /// Represents a DataMiner Automation script.
    /// </summary>
    public class Script
    {
        private const string PARAM_ELEMENT_KEYS = "Element Keys";
        private const string PARAM_TARGET_AGENT_ID = "Target Agent ID";

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
            var agents = engine.GetAgents();

            if (!Check.IfSwarmingIsEnabled(agents))
                engine.ExitFail("Swarming is not enabled in this DMS. More info: https://aka.dataminer.services/Swarming");

            var elementKeys = GetElementIDs(engine);
            int targetAgentId = GetTargetAgentId(engine);

            if (!agents.Any(agentInfo => agentInfo.ID == targetAgentId))
                engine.ExitFail($"Target agent '{targetAgentId}' is not part of the cluster");

            if (!elementKeys.Any())
                return; // nothing to do here

            var swarmingResults = SwarmingHelper.Create(engine.GetUserConnection())
                .SwarmElements(elementKeys)
                .ToAgent(targetAgentId);

            if (swarmingResults.Any(result => !result.Success))
            {
                var failureMessages = swarmingResults
                    .Where(result => !result.Success)
                    .Select(result => result.Message)
                    .ToArray();

                engine.ExitFail(string.Join(", ", failureMessages));
            }
        }

        private ElementID[] GetElementIDs(IEngine engine)
        {
            var elementKeysRaw = engine.GetScriptParam(PARAM_ELEMENT_KEYS)?.Value;
            if (string.IsNullOrWhiteSpace(elementKeysRaw))
                engine.ExitFail("Must at least provide one element!");

            try
            {
                // first try as json structure (from low code app)
                // eg "["123/456", "753/159"]"
                var ids = JsonConvert
                    .DeserializeObject<string[]>(elementKeysRaw)
                    .Select(key => ElementID.FromString(key) ?? throw new ArgumentException($"Cannot parse {key} to valid {nameof(ElementID)}"))
                    .ToArray();

                if (ids.Length <= 0)
                    engine.ExitFail("Must at least provide one element!");

                return ids;
            }
            catch (JsonSerializationException)
            {
                // not valid json, try parse as normal input parameters
                // eg "789/123, 456/258"
                var ids = elementKeysRaw
                    .Replace(" ", string.Empty) // remove spaces
                    .Split(',')
                    .Select(key => ElementID.FromString(key) ?? throw new ArgumentException($"Cannot parse {key} to valid {nameof(ElementID)}"))
                    .ToArray();

                if (ids.Length <= 0)
                    engine.ExitFail("Must at least provide one element!");

                return ids;
            }
        }

        private int GetTargetAgentId(IEngine engine)
        {
            var targetAgentIdRaw = engine.GetScriptParam(PARAM_TARGET_AGENT_ID)?.Value;
            if (string.IsNullOrWhiteSpace(targetAgentIdRaw))
                engine.ExitFail("Must provide exactly 1 Target Agent ID!");

            try
            {
                // first try as json structure (from low code app)
                // eg "["123"]"
                string[] dmaIds = JsonConvert
                    .DeserializeObject<string[]>(targetAgentIdRaw);

                if (dmaIds.Length != 1)
                    engine.ExitFail("Must provide exactly 1 Target Agent ID!");

                return int.Parse(dmaIds.First());
            }
            catch (JsonSerializationException)
            {
                // not valid json, try parse as normal input parameters
                // eg "789"
                return int.Parse(targetAgentIdRaw);
            }
        }
    }
}
