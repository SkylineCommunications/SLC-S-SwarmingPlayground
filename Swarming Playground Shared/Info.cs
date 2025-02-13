namespace Swarming_Playground_Shared
{
    using System;
    using System.Linq;
    using Skyline.DataMiner.Analytics.GenericInterface;
    using Skyline.DataMiner.Automation;
    using Skyline.DataMiner.Net.Messages;

    public static class Info
    {
        public static GetDataMinerInfoResponseMessage[] GetAgents(this IEngine engine)
            => GetAgents(engine.SendSLNetMessage);

        public static ElementInfoEventMessage[] GetElements(this IEngine engine)
            => GetElements(engine.SendSLNetMessage);

        public static GetDataMinerInfoResponseMessage[] GetAgents(this GQIDMS dms)
            => GetAgents(msg => dms.SendMessages(msg));

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
