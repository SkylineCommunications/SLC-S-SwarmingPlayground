
namespace Cluster_Maintenance
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
        public static void IfSwarmingIsEnabled(GetDataMinerInfoResponseMessage[] agentInfos)
        {
            if (!agentInfos.All(agentInfo => agentInfo.IsSwarmingEnabled))
                throw new NotSupportedException("Swarming is not supported in this DMS!");
        }
    }
}
