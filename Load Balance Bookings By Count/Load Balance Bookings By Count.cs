using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Skyline.DataMiner.Automation;
using Skyline.DataMiner.Net.Messages;
using Skyline.DataMiner.Net.Messages.SLDataGateway;
using Skyline.DataMiner.Net.ResourceManager.Objects;
using Skyline.DataMiner.Net.Swarming;
using Skyline.DataMiner.Net.Swarming.Helper;
using Swarming_Playground_Shared;

namespace LoadBalanceBookingsByCount
{
	/// <summary>
	/// Represents a DataMiner Automation script.
	/// </summary>
	public class Script
	{
		private const string PARAM_SWARM_BOOKINGS = "Swarm Bookings";
		private IEngine _engine;
		private ResourceManagerHelper _rmHelper;
		private Dictionary<GetDataMinerInfoResponseMessage, List<ReservationInstance>> _agentToBookings;

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
			_engine = engine;

			var agentInfos = _engine.GetAgents();

			if (!Check.IfSwarmingIsEnabled(agentInfos))
				engine.ExitFail(
					"Swarming is not enabled in this DMS. More info: https://aka.dataminer.services/Swarming");

			var swarmBookingsRaw = _engine.GetScriptParam(PARAM_SWARM_BOOKINGS)?.Value;
			bool swarmBookingsEnabled;
			try
			{
				swarmBookingsEnabled = JsonConvert.DeserializeObject<string[]>(swarmBookingsRaw)
					.Select(bool.Parse).FirstOrDefault();
			}
			catch (JsonSerializationException)
			{
				swarmBookingsEnabled = swarmBookingsRaw.Replace(" ", string.Empty).Split(',').Select(one =>
				{
					if (!bool.TryParse(one, out var result))
					{
						throw new ArgumentException($"Cannot parse {one} to valid {nameof(Guid)}");
					}

					return result;
				}).FirstOrDefault();
			}

			if (!swarmBookingsEnabled)
			{
				_engine.ExitFail("Not swarming bookings since option is not selected");
			}

			_rmHelper = new ResourceManagerHelper(_engine.SendSLNetSingleResponseMessage);
			var bookings = _rmHelper.GetReservationInstances(new TRUEFilterElement<ReservationInstance>());

			RedistributeByCount(agentInfos, bookings);
			SwarmBookings();

		}

		public void RedistributeByCount(GetDataMinerInfoResponseMessage[] agentInfos, ReservationInstance[] bookings)
		{
			_agentToBookings = agentInfos.GroupJoin
				(
					bookings.GroupBy(booking => booking.HostingAgentID),
					agentInfo => agentInfo.ID,
					bookingGroup => bookingGroup.Key,
					(agentInfo, bookingGroup) => (agentInfo, bookingGroup.SelectMany(x => x).ToList())
				)
				.ToDictionary(kvp => kvp.Item1, kvp => kvp.Item2);

			var currentBuckets = _agentToBookings.Select(
				bucket => (bucket.Key, bucket.Value.Where(booking => booking.Status == ReservationStatus.Ongoing).ToList())).
				ToDictionary(kvp => kvp.Key, entry => entry.Item2);

			if (currentBuckets.Count == 1)
				return;

			var leftoverBuckets = _agentToBookings
				.Select(bucket => (bucket.Key, bucket.Value.Where(booking => booking.Status != ReservationStatus.Ongoing).ToList()))
				.Where(bucket => bucket.Item2.Any()) // Remove empty buckets for agents that start without any swarmable bookings
				.ToDictionary(kvp => kvp.Item1, kvp => kvp.Item2);

			while (leftoverBuckets.Any())
			{
				var smallestBucketSize = currentBuckets
					.Where(bucket => bucket.Key.ConnectionState == DataMinerAgentConnectionState.Normal) // can only target healthy agents
					.Min(bucket => bucket.Value.Count);

				var smallestBuckets = currentBuckets.Where(bucket => bucket.Value.Count == smallestBucketSize);

				// of all the buckets with the smallest count, prefer the one that still has bookings already hosted on it
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

			_agentToBookings = currentBuckets;
		}

		/// <summary>
		/// Swarm bookings between DMAs to reflect the in memory configuration.
		/// </summary>
		public void SwarmBookings()
		{
			var failures = new ConcurrentBag<SwarmingResult>();

			// Can only swarm to healthy agents
			var swarmActions = _agentToBookings
				.Where(kvp => kvp.Key.ConnectionState == DataMinerAgentConnectionState.Normal);

			Parallel.ForEach(swarmActions, kvp =>
			{
				var targetAgentId = kvp.Key.ID;
				var bookings = kvp.Value.Where(element => element.HostingAgentID != targetAgentId).ToList();

				if (!bookings.Any())
					return; // This agent does not receive any new elements

				_engine.Log($"Swarming bookings to agent {targetAgentId}: " + string.Join(", ", bookings.Select(info => info.Name)));

				var responses = SwarmingHelper.Create(_engine.GetUserConnection())
					.SwarmBookings(bookings.Select(booking => booking.ID).ToArray())
					.ToAgent(targetAgentId);

				foreach (var failure in responses.Where(resp => !resp.Success))
					failures.Add(failure);
			});

			if (failures.Any())
			{
				var summary = new StringBuilder();
				summary.AppendLine($"Swarming failed for {failures.Count} booking(s):");
				foreach (var failure in failures)
					summary.AppendLine($"\t- {failure.DmaObjectRef}: {failure.Message}");
				_engine.ExitFail(summary.ToString());
			}
		}
	}
}
