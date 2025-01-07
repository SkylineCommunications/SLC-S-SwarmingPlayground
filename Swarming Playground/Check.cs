
namespace Swarming_Playground
{
    using System;
    using Skyline.DataMiner.Net.Messages;
    using System.Linq;

    public static class Check
    {

        /// <summary>
        /// Thows if Swarming is not enabled on every agent.
        /// </summary>
        /// <param name="agentInfos"></param>
        /// <exception cref="NotSupportedException"></exception>
        public static bool IfSwarmingIsEnabled(GetDataMinerInfoResponseMessage[] agentInfos)
        {
            return agentInfos.All(agentInfo => agentInfo.IsSwarmingEnabled);
        }
    }
}
