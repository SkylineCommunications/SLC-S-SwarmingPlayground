using System;
using System.Linq;
using Newtonsoft.Json;
using Skyline.DataMiner.Automation;
using Skyline.DataMiner.Net.Swarming.Helper;
using Swarming_Playground_Shared;

namespace SwarmBookings
{
	/// <summary>
	/// Represents a DataMiner Automation script.
	/// </summary>
	public class Script
	{
		private IEngine _engine;
		private const string PARAM_BOOKING_IDS = "Booking IDs";
		private const string PARAM_TARGET_AGENT_ID = "Target Agent ID";

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
			var agents = engine.GetAgents();

			var bookingIds = GetBookingIds();
			var targetAgentId = _engine.GetTargetAgentId(PARAM_TARGET_AGENT_ID);
			if (targetAgentId == -1)
				_engine.ExitFail("Must provide exactly 1 Target Agent ID!");

			if (agents.All(agentInfo => agentInfo.ID != targetAgentId))
				engine.ExitFail($"Target agent '{targetAgentId}' is not part of the cluster");

			if (!bookingIds.Any())
				return;

			var swarmingResults = SwarmingHelper.Create(engine.GetUserConnection())
				.SwarmBookings(bookingIds.ToArray())
				.ToAgent(targetAgentId);

			if (swarmingResults.Any(result => !result.Success))
			{
				var failureMessages = swarmingResults
					.Where(result => !result.Success)
					.Select(result => result.Message)
					.ToArray();

				engine.ExitFail(string.Join(", ", failureMessages));
			}
		}

		private Guid[] GetBookingIds()
		{
			var bookingIdsRaw = _engine.GetScriptParam(PARAM_BOOKING_IDS)?.Value;
			if (string.IsNullOrEmpty(bookingIdsRaw))
			{
				_engine.ExitFail("Must at least provide one booking!");
			}

			try
			{
				var ids = JsonConvert.DeserializeObject<string[]>(bookingIdsRaw)
					.Select(Guid.Parse).ToArray();

				if (!ids.Any())
				{
					_engine.ExitFail("Must at least provide one booking!");
				}

				return ids;
			}
			catch (JsonSerializationException)
			{
				var ids = bookingIdsRaw.Replace(" ", string.Empty).Split(',').Select(one =>
				 {
					 if (!Guid.TryParse(one, out var result))
					 {
						 throw new ArgumentException($"Cannot parse {one} to valid {nameof(Guid)}");
					 }

					 return result;
				 }).ToArray();

				return ids;
			}
		}
	}
}
