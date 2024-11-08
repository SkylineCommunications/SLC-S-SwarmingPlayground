namespace Swarming_Playground
{
    using System.Linq;
    using Skyline.DataMiner.Automation;
    using Skyline.DataMiner.Net.Messages;

    public static class Info
    {
        public static GetDataMinerInfoResponseMessage[] GetAgents(this IEngine engine)
            => engine.SendSLNetMessage(new GetInfoMessage(InfoType.DataMinerInfo))
                .OfType<GetDataMinerInfoResponseMessage>()
                .ToArray();

        public static ElementInfoEventMessage[] GetElements(this IEngine engine)
            => engine.SendSLNetMessage(new GetInfoMessage(InfoType.ElementInfo))
                .OfType<ElementInfoEventMessage>()
                .ToArray();
    }
}
