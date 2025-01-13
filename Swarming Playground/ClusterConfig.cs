namespace Swarming_Playground
{
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

    public class ClusterConfig
    {
        private IEngine _engine;
        private Dictionary<GetDataMinerInfoResponseMessage, List<ElementInfoEventMessage>> _agentToElements;

        /// <summary>
        /// Gets the config.
        /// used in unit tests to verify internal state.
        /// </summary>
        internal IReadOnlyDictionary<GetDataMinerInfoResponseMessage, List<ElementInfoEventMessage>> CurrentConfig
        {
            get => _agentToElements;
        }

        public ClusterConfig(IEngine engine, GetDataMinerInfoResponseMessage[] agentInfos, ElementInfoEventMessage[] elementInfos)
        {
            _engine = engine;

            _agentToElements = agentInfos
                .GroupJoin
                    (
                        elementInfos.GroupBy(elementInfo => elementInfo.HostingAgentID),
                        agentInfo => agentInfo.ID,
                        elementInfoGroup => elementInfoGroup.Key,
                        (agentInfo, elementInfoGroups) => (agentInfo, elementInfoGroups.SelectMany(x => x).ToList())
                    )
                .ToDictionary(kvp => kvp.Item1, kvp => kvp.Item2);
        }

        /// <summary>
        /// Swarm elements betewen DMAs to reflect the in memory configuration.
        /// </summary>
        public void SwarmElements()
        {
            var failures = new ConcurrentBag<SwarmingResult>();

            // can only swarm to healthy agents
            var swarmActions = _agentToElements
                .Where(kvp => kvp.Key.ConnectionState == DataMinerAgentConnectionState.Normal);

            Parallel.ForEach(swarmActions, kvp =>
            {
                var targetAgentId = kvp.Key.ID;
                var elements = kvp.Value.Where(element => element.HostingAgentID != targetAgentId).ToList();

                if (!elements.Any())
                    return; // this agent does not receive any new elements

                _engine.Log($"Swarming elements to agent {targetAgentId}: " + string.Join(", ", elements.Select(info => info.Name)));

                var responses = SwarmingHelper.Create(_engine.SendSLNetMessage)
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
                _engine.ExitFail(summary.ToString());
            }
        }

        /// <summary>
        /// Recalculate the elements per agent in memory.
        /// Move as many elements as possible away from the source agents.
        /// </summary>
        public void RedistributeAwayFromAgents(int[] sourceAgentIds)
        {
            var targetBuckets = _agentToElements
                .Where(bucket => bucket.Key.ConnectionState == DataMinerAgentConnectionState.Normal
                        && !sourceAgentIds.Contains(bucket.Key.ID))
                .ToDictionary(kvp => kvp.Key, kvp => kvp.Value);

            var sourceBuckets = _agentToElements
                .Where(bucket => sourceAgentIds.Contains(bucket.Key.ID))
                .Select(bucket => (bucket.Key, bucket.Value.Where(elementInfo => elementInfo.IsSwarmable).ToList()))
                .Where(bucket => bucket.Item2.Any()) // remove empty buckets for agents that don't have any swarmable elements
                .ToDictionary(kvp => kvp.Item1, kvp => kvp.Item2);

            while (sourceBuckets.Any())
            {
                var smallestBucketSize = targetBuckets.Min(bucket => bucket.Value.Count);
                var targetBucket = targetBuckets.First(bucket => bucket.Value.Count == smallestBucketSize);

                var sourceBucket = sourceBuckets.First();

                var itemToMove = sourceBucket.Value.Take(1).First();
                sourceBucket.Value.Remove(itemToMove);
                targetBucket.Value.Add(itemToMove);

                if (!sourceBucket.Value.Any())
                    sourceBuckets.Remove(sourceBucket.Key);
            }

            _agentToElements = targetBuckets;
        }

        /// <summary>
        /// Recalculate the elements per agent in memory.
        /// </summary>
        public void RedistributeByCount()
        {
            // TODO: once Swarming DVE parent elements is possible, take into account that DVE childs will be moved together with it, right now they are treated as a single group of non-swarmable elements

            var currentBuckets = _agentToElements
                .Select(bucket => (bucket.Key, bucket.Value.Where(elementInfo => !elementInfo.IsSwarmable).ToList()))
                .ToDictionary(kvp => kvp.Item1, kvp => kvp.Item2);

            if (currentBuckets.Count == 1)
                return; // nothing to do

            var leftoverBuckets = _agentToElements
                .Select(bucket => (bucket.Key, bucket.Value.Where(elementInfo => elementInfo.IsSwarmable).ToList()))
                .Where(bucket => bucket.Item2.Any()) // remove empty buckets for agents that start without any swarmable elements
                .ToDictionary(kvp => kvp.Item1, kvp => kvp.Item2);

            while (leftoverBuckets.Any())
            {
                var smallestBucketSize = currentBuckets
                    .Where(bucket => bucket.Key.ConnectionState == DataMinerAgentConnectionState.Normal) // can only target healthy agents
                    .Min(bucket => bucket.Value.Count);

                var smallestBuckets = currentBuckets.Where(bucket => bucket.Value.Count == smallestBucketSize);

                // of all the buckets with the smallest count, prefer the one that still has elements already hosted on it
                // if there is not such a bucket, just pick the first one, it does not matter
                var smallestBucketToUse = smallestBuckets.Any(bucket => leftoverBuckets.ContainsKey(bucket.Key))
                    ? smallestBuckets.First(bucket => leftoverBuckets.ContainsKey(bucket.Key))
                    : smallestBuckets.First();

                // Choose an item to pick, prefer the one already in the correct bucket from before (one less swarm in the end)
                // otherwise pick one from the projected largest bucket (already sorted + to sort if not moved)
                var leftoverBucketToRemoveFrom = leftoverBuckets.ContainsKey(smallestBucketToUse.Key)
                    ? leftoverBuckets.Single(bucket => bucket.Key.ID == smallestBucketToUse.Key.ID)
                    : leftoverBuckets
                        .OrderByDescending(bucket => bucket.Value.Count + currentBuckets[bucket.Key].Count)
                        .First();

                var itemToMove = leftoverBucketToRemoveFrom.Value.First();
                smallestBucketToUse.Value.Add(itemToMove);
                leftoverBucketToRemoveFrom.Value.Remove(itemToMove);

                if (!leftoverBucketToRemoveFrom.Value.Any())
                    leftoverBuckets.Remove(leftoverBucketToRemoveFrom.Key);
            }

            _agentToElements = currentBuckets;
        }
    }
}
