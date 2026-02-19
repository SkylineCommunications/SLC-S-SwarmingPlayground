namespace LoadBalanceTests
{
    using System.Collections.Generic;
    using System.Linq;
    using Moq;
    using NUnit.Framework;
    using Skyline.DataMiner.Automation;
    using Skyline.DataMiner.Net.Messages;
    using Swarming_Playground_Shared;

    public class LoadBalanceByCountTests
    {
        public static object[] TestCases = new[]
        {
            new object[]
            {
                Agents(1),
                ElementOn(1).Times(0),
                new[] { 0 },
                0,
            },

            new object[]
            {
                Agents(1),
                ElementOn(1).Times(5),
                new[] { 5 },
                0,
            },

            new object[]
            {
                Agents(1),
                new[] { NonSwarmableElementOn(1).Times(20), ElementOn(1).Times(10), }.Flatten(),
                new[] { 30 },
                0,
            },

            new object[]
            {
                Agents(1, 2, 3),
                ElementOn(1).Times(3),
                new[] { 1, 1, 1 },
                2,
            },

            new object[]
            {
                Agents(1, 2, 3),
                ElementOn(1).Times(10),
                new[] { 4, 3, 3 },
                6,
            },

            new object[]
            {
                Agents(1, 2, 3),
                new[] { NonSwarmableElementOn(1).Times(20), ElementOn(2).Times(10), }.Flatten(),
                new[] { 20, 5, 5 },
                5,
            },

            new object[]
            {
                Agents(1, 2, 3),
				new[] { NonSwarmableElementOn(1).Times(20), NonSwarmableElementOn(2).Times(10), }.Flatten(),
                new[] { 20, 10, 0 },
                0,
            },

            new object[]
            {
                Agents(1, 2, 3),
                new[] { NonSwarmableElementOn(1).Times(20), NonSwarmableElementOn(2).Times(10), ElementOn(2).Times(1) }.Flatten(),
                new[] { 20, 10, 1 },
                1,
            },

            new object[]
            {
                Agents(1, 2, 3, 4, 5),
                new[]
                {
                    ElementOn(1).Times(12),
                    ElementOn(2).Times(5),
                    ElementOn(3).Times(8),
                    ElementOn(4).Times(89),
                    ElementOn(5).Times(123),
                }.Flatten(),
                new[] { 48, 48, 47, 47, 47 },
                3 * 47 - 12 - 5 - 8,
            },
        };

        [TestCaseSource(nameof(TestCases))]
        public void LoadBalance(
            GetDataMinerInfoResponseMessage[] agentInfos,
            ElementInfoEventMessage[] elementInfos,
            int[] expectedElementCounts,
            int expectedNumberOfSwarms)
        {
            // Arrange
            var clusterConfig = new ClusterConfig(Mock.Of<IEngine>(), agentInfos, elementInfos);

			// Act
			clusterConfig.RedistributeElementsByCount(element => element.IsSwarmable);
            var output = clusterConfig.CurrentConfig;

            // Assert
            var counts = output.Select(bucket => bucket.Value.Count).ToArray();
            Assert.That(counts, Is.EquivalentTo(expectedElementCounts));

            var numberOfSwarms = output.Sum(bucket => bucket.Value.Where(element => element.HostingAgentID != bucket.Key.ID).Count());
            Assert.That(numberOfSwarms, Is.EqualTo(expectedNumberOfSwarms));

            // there should always be at least one agent that does not receive any new elements (all elements are already on the correct host)
            // otherwise there is an optimization possible where you move the swarmed elements around until
            // one ends up again at its original host
            Assert.That(output.Any(agentBucket => agentBucket.Value.All(element => element.HostingAgentID == agentBucket.Key.ID)));
        }

        private static GetDataMinerInfoResponseMessage[] Agents(params int[] ids)
            => ids.Select(id => new GetDataMinerInfoResponseMessage() { ID = id }).ToArray();

        private static int _nextElementId = 5;
        internal static ElementInfoEventMessage ElementOn(int hostingAgentId)
            => new ElementInfoEventMessage() { DataMinerID = 12345679, ElementID = _nextElementId++, HostingAgentID = hostingAgentId, IsSwarmable = true };

        internal static ElementInfoEventMessage NonSwarmableElementOn(int hostingAgentId)
            => new ElementInfoEventMessage() { DataMinerID = 12345679, ElementID = _nextElementId++, HostingAgentID = hostingAgentId, IsSwarmable = false };
    }

    internal static class Extensions
    {
        internal static ElementInfoEventMessage[] Times(this ElementInfoEventMessage original, int copies)
            => Enumerable.Range(0, copies).Select(_ =>
                original.IsSwarmable
                    ? LoadBalanceByCountTests.ElementOn(original.HostingAgentID)
                    : LoadBalanceByCountTests.NonSwarmableElementOn(original.HostingAgentID)
            ).ToArray();

        internal static T[] Flatten<T>(this IEnumerable<IEnumerable<T>> source)
            => source.SelectMany(x => x).ToArray();
    }
}