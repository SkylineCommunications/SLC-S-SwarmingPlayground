namespace Swarming_Playground_Shared
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
    using System;
    using Skyline.DataMiner.Net.ResourceManager.Objects;
    using Skyline.DataMiner.Net.Swarming.Helper.Interfaces;

	public class ClusterConfig
    {
        private IEngine _engine;
        private readonly GetDataMinerInfoResponseMessage[] _agentInfos;
        private Dictionary<GetDataMinerInfoResponseMessage, List<ElementInfoEventMessage>> _agentToElements;
        private Dictionary<GetDataMinerInfoResponseMessage, List<ReservationInstance>> _agentToBookings;


		/// <summary>
		/// Gets the config for elements.
		/// used in unit tests to verify internal state.
		/// </summary>
		internal IReadOnlyDictionary<GetDataMinerInfoResponseMessage, List<ElementInfoEventMessage>> CurrentConfigElements
        {
            get => _agentToElements;
        }

        /// <summary>
        /// Gets the config.
        /// used in unit tests to verify internal state.
        /// </summary>
        internal IReadOnlyDictionary<GetDataMinerInfoResponseMessage, List<ReservationInstance>> CurrentConfigBookings
        {
	        get => _agentToBookings;
        }

		public ClusterConfig(IEngine engine, GetDataMinerInfoResponseMessage[] agentInfos)
        {
	        _engine = engine;
	        _agentInfos = agentInfos;
        }

        public ClusterConfig(IEngine engine, GetDataMinerInfoResponseMessage[] agentInfos, ElementInfoEventMessage[] elementInfos)
        {
            _engine = engine;
            _agentInfos = agentInfos;
            InitializeAgentToElements(elementInfos);
        }

        public void InitializeAgentToElements(ElementInfoEventMessage[] elementInfos)
        {
	        _agentToElements = _agentInfos
		        .GroupJoin
		        (
			        elementInfos.GroupBy(elementInfo => elementInfo.HostingAgentID),
			        agentInfo => agentInfo.ID,
			        elementInfoGroup => elementInfoGroup.Key,
			        (agentInfo, elementInfoGroups) => (agentInfo, elementInfoGroups.SelectMany(x => x).ToList())
		        )
		        .ToDictionary(kvp => kvp.Item1, kvp => kvp.Item2);
		}

        public void InitializeAgentToBookings(ReservationInstance[] bookings)
        {
	        _agentToBookings = _agentInfos.GroupJoin
		        (
			        bookings.GroupBy(booking => booking.HostingAgentID),
			        agentInfo => agentInfo.ID,
			        bookingGroup => bookingGroup.Key,
			        (agentInfo, bookingGroup) => (agentInfo, bookingGroup.SelectMany(x => x).ToList())
		        )
		        .ToDictionary(kvp => kvp.Item1, kvp => kvp.Item2);
		}

        public void RedistributeElementsByCount(Func<ElementInfoEventMessage, bool> isLeftover)
        {
	        _agentToElements = RedistributeByCount(_agentToElements, isLeftover);
        }

        public void RedistributeBookingsByCount(Func<ReservationInstance, bool> isLeftover)
        {
	        _agentToBookings = RedistributeByCount(_agentToBookings, isLeftover);
        }

		private Dictionary<GetDataMinerInfoResponseMessage, List<T>> RedistributeByCount<T>(
	Dictionary<GetDataMinerInfoResponseMessage, List<T>> agentToItems,
	Func<T, bool> isLeftover)
		{
			var currentBuckets = agentToItems
				.Select(bucket => (bucket.Key, bucket.Value.Where(item => !isLeftover(item)).ToList()))
				.ToDictionary(kvp => kvp.Key, kvp => kvp.Item2);

			if (currentBuckets.Count == 1)
			{
				// Nothing to do
				return agentToItems;
			}

			var leftoverBuckets = agentToItems
				.Select(bucket => (bucket.Key, bucket.Value.Where(isLeftover).ToList()))
				.Where(bucket => bucket.Item2.Any()) // Remove empty buckets for agents that start without any swarmable objects
				.ToDictionary(kvp => kvp.Item1, kvp => kvp.Item2);

			while (leftoverBuckets.Any())
			{
				var smallestBucketSize = currentBuckets
					.Where(bucket => bucket.Key.ConnectionState == DataMinerAgentConnectionState.Normal) // Can only target healthy agents
					.Min(bucket => bucket.Value.Count);

				var smallestBuckets = currentBuckets
					.Where(bucket => bucket.Value.Count == smallestBucketSize);

				// Of all the buckets with the smallest count, prefer the one that still has objects already hosted on it
				// If there is not such a bucket, just pick the first one, it does not matter
				var smallestBucketToUse =
					smallestBuckets.Any(bucket => leftoverBuckets.ContainsKey(bucket.Key))
						? smallestBuckets.First(bucket => leftoverBuckets.ContainsKey(bucket.Key))
						: smallestBuckets.First();

				// Choose an item to pick, prefer the one already in the correct bucket from before (one less swarm in the end)
				// Otherwise pick one from the projected largest bucket (already sorted + to sort if not moved)
				var leftoverBucketToRemoveFrom =
					leftoverBuckets.ContainsKey(smallestBucketToUse.Key)
						? leftoverBuckets.Single(bucket => bucket.Key.ID == smallestBucketToUse.Key.ID)
						: leftoverBuckets
							.OrderByDescending(bucket => bucket.Value.Count +
														 currentBuckets[bucket.Key].Count)
							.First();

				var itemToMove = leftoverBucketToRemoveFrom.Value.First();
				smallestBucketToUse.Value.Add(itemToMove);
				leftoverBucketToRemoveFrom.Value.Remove(itemToMove);

				if (!leftoverBucketToRemoveFrom.Value.Any())
					leftoverBuckets.Remove(leftoverBucketToRemoveFrom.Key);
			}

			return currentBuckets;
		}

		/// <summary>
		/// Swarm elements between DMAs to reflect the in memory configuration.
		/// </summary>
		public void SwarmElements()
		{
			SwarmItems(_agentToElements, (element, agentId) => element.HostingAgentID == agentId,
				element => element.Name, element => new ElementID(element.DataMinerID, element.ElementID),
				(helper, ids, targetAgentId) => helper.SwarmElements(ids.Cast<ElementID>().ToArray()).ToAgent(targetAgentId), "element");
        }

        /// <summary>
        /// Swarm bookings between DMAs to reflect the in memory configuration.
        /// </summary>
        public void SwarmBookings()
        {
	        SwarmItems(_agentToBookings, (booking, agentId) => booking.HostingAgentID == agentId,
		        booking => booking.Name, booking => booking.ID,
		        (helper, ids, targetAgentId) => helper.SwarmBookings(ids.Cast<Guid>().ToArray()).ToAgent(targetAgentId), "booking");
        }

        private void SwarmItems<T>(Dictionary<GetDataMinerInfoResponseMessage, List<T>> agentToItems,
	        Func<T, int, bool> isHostedOnAgent, Func<T, object> getDisplayName, Func<T, object> toSwarmId,
	        Func<ISwarmingHelper, object[], int,SwarmingResult[]> swarmAction, string itemType)
		{
	        var failures = new ConcurrentBag<SwarmingResult>();

	        // can only swarm to healthy agents
	        var swarmActions = agentToItems
		        .Where(kvp => kvp.Key.ConnectionState == DataMinerAgentConnectionState.Normal);

	        Parallel.ForEach(swarmActions, kvp =>
	        {
		        var targetAgentId = kvp.Key.ID;
		        var itemsToMove = kvp.Value.Where(item =>!isHostedOnAgent(item, targetAgentId)).ToList();

		        if (!itemsToMove.Any())
			        return;

				_engine.Log($"Swarming {itemType}s to agent {targetAgentId}: " +
				            string.Join(", ", itemsToMove.Select(getDisplayName)));

				var itemIds = itemsToMove.Select(toSwarmId).ToArray();
				var helper = SwarmingHelper.Create(_engine.GetUserConnection());
				var responses = swarmAction(helper, itemIds, targetAgentId);
				
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

        public void RedistributeElementsAwayFromAgents(int[] sourceAgentIds, Func<ElementInfoEventMessage, bool> isSwarmable)
        {
	        _agentToElements = RedistributeAwayFromAgents(sourceAgentIds, _agentToElements, isSwarmable);
        }

        public void RedistributeBookingsAwayFromAgents(int[] sourceAgentIds, Func<ReservationInstance, bool> isSwarmable)
        {
	        _agentToBookings = RedistributeAwayFromAgents(sourceAgentIds, _agentToBookings, isSwarmable);
        }

        private Dictionary<GetDataMinerInfoResponseMessage, List<T>> RedistributeAwayFromAgents<T>(
	        int[] sourceAgentIds,
	        Dictionary<GetDataMinerInfoResponseMessage, List<T>> agentToObjects,
	        Func<T, bool> isSwarmable)
        {
	        var targetBuckets = agentToObjects
		        .Where(bucket => bucket.Key.ConnectionState == DataMinerAgentConnectionState.Normal
		                         && !sourceAgentIds.Contains(bucket.Key.ID))
		        .ToDictionary(kvp => kvp.Key, kvp => kvp.Value);

	        var sourceBuckets = agentToObjects
		        .Where(bucket => sourceAgentIds.Contains(bucket.Key.ID))
		        .Select(bucket => (bucket.Key, bucket.Value.Where(isSwarmable).ToList()))
		        .Where(bucket => bucket.Item2.Any())
		        .ToDictionary(kvp => kvp.Item1, kvp => kvp.Item2);

	        while (sourceBuckets.Any())
	        {
		        var smallestBucketSize = targetBuckets.Min(bucket => bucket.Value.Count);
		        var targetBucket = targetBuckets.First(bucket => bucket.Value.Count == smallestBucketSize);

		        var sourceBucket = sourceBuckets.First();
		        var itemToMove = sourceBucket.Value[0];

		        sourceBucket.Value.Remove(itemToMove);
		        targetBucket.Value.Add(itemToMove);

		        if (!sourceBucket.Value.Any())
			        sourceBuckets.Remove(sourceBucket.Key);
	        }

	        return targetBuckets;
        }
	}
}
