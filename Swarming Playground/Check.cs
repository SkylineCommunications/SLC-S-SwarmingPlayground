
namespace Swarming_Playground
{
    using System;
    using Skyline.DataMiner.Net.Messages;
    using System.Linq;

    public static class Check
    {

        /// <summary>
        /// If Swarming is not enabled on every agent.
        /// </summary>
        /// <param name="agentInfos"></param>
        public static bool IfSwarmingIsEnabled(GetDataMinerInfoResponseMessage[] agentInfos)
        {
            return agentInfos.All(agentInfo => agentInfo.IsSwarmingEnabled);
        }
    }
}
