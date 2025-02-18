namespace Swarming_Playground_Shared
{
    using System;
    using System.Linq;
    using Skyline.DataMiner.Analytics.GenericInterface;
    using Skyline.DataMiner.Automation;
    using Skyline.DataMiner.Net.Messages;

    /// <summary>
    /// Helper class to load some basic info
    /// </summary>
    public static class Info
    {

        /// <summary>
        /// Gets the agentes via automation engine
        /// </summary>
        /// <param name="engine"></param>
        /// <returns></returns>
        public static GetDataMinerInfoResponseMessage[] GetAgents(this IEngine engine)
            => GetAgents(engine.SendSLNetMessage);

        /// <summary>
        /// Gets the element info events via automation engine
        /// </summary>
        /// <param name="engine"></param>
        /// <returns></returns>
        public static ElementInfoEventMessage[] GetElements(this IEngine engine)
            => GetElements(engine.SendSLNetMessage);

        /// <summary>
        /// Gets the agents via GQIDMS interface
        /// </summary>
        /// <param name="dms"></param>
        /// <returns></returns>
        public static GetDataMinerInfoResponseMessage[] GetAgents(this GQIDMS dms)
            => GetAgents(msg => dms.SendMessages(msg));

        /// <summary>
        /// Gets the element info events via GQIDMS interface
        /// </summary>
        /// <param name="dms"></param>
        /// <returns></returns>
        public static ElementInfoEventMessage[] GetElements(this GQIDMS dms)
            => GetElements(msg => dms.SendMessages(msg));

        private static GetDataMinerInfoResponseMessage[] GetAgents(Func<DMSMessage, DMSMessage[]> sendMessage)
            => sendMessage(new GetInfoMessage(InfoType.DataMinerInfo))
                .OfType<GetDataMinerInfoResponseMessage>()
                .ToArray();

        private static ElementInfoEventMessage[] GetElements(Func<DMSMessage, DMSMessage[]> sendMessage)
            => sendMessage(new GetInfoMessage(InfoType.ElementInfo))
                .OfType<ElementInfoEventMessage>()
                .ToArray();
    }
}
