using System;
using System.Linq;
using Newtonsoft.Json;
using Skyline.DataMiner.Automation;
using Skyline.DataMiner.Net;
using Skyline.DataMiner.Net.Messages.Advanced;
using Skyline.DataMiner.Net.Scheduler;
using Skyline.DataMiner.Net.Swarming.Helper;
using Swarming_Playground_Shared;

namespace SwarmScheduledTasks
{
	/// <summary>
	/// Swarm Scheduled task to target agent.
	/// </summary>
	public class Script
	{
		private IEngine _engine;
		private const string ParamScheduledTaskDmaIds = "Scheduled Task DMA IDs";
		private const string ParamScheduledTaskIds = "Scheduled Task IDs";
		private const string ParamTargetAgentId = "Target Agent ID";

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

			var agents = _engine.GetAgents();
			var storageType = GetStorageType();
			if (storageType != "Database")
			{
				_engine.ExitFail("Swarming requires scheduled tasks to be stored in database storage. More info: https://aka.dataminer.services/SchedulerDataStorage");
			}

			if (!Check.IfSwarmingIsEnabled(agents))
			{
				_engine.ExitFail(
					"Swarming is not enabled in this DMS. More info: https://aka.dataminer.services/Swarming");
			}

			var scheduledTaskIds = GetScheduledTaskIds();
			var targetAgentId = _engine.GetTargetAgentId(ParamTargetAgentId);
			if (targetAgentId == -1)
				_engine.ExitFail("Must provide exactly 1 Target Agent ID!");

			if (agents.All(agentInfo => agentInfo.ID != targetAgentId))
				_engine.ExitFail($"Target agent '{targetAgentId}' is not part of the cluster");

			if (!scheduledTaskIds.Any())
				return;

			var swarmingResults = SwarmingHelper.Create(engine.GetUserConnection())
				.SwarmScheduledTasks(scheduledTaskIds).ToAgent(targetAgentId);

			if (swarmingResults.Any(result => !result.Success))
			{
				var failureMessages = swarmingResults
					.Where(result => !result.Success)
					.Select(result => result.Message)
					.ToArray();

				_engine.ExitFail(string.Join(", ", failureMessages));
			}
		}

		private string GetStorageType()
		{
			var msg = new GetSchedulerInfoMessage()
			{
				What = (int)SchedulerInfoTypes.GetConfig,
			};

			var resp = _engine.SendSLNetSingleResponseMessage(msg) as GetSchedulerInfoResponseMessage;
			if (resp == null || resp.psaRet?.Psa?.Length == 0)
			{
				return string.Empty;
			}

			var configArray = resp.psaRet?.Psa?[0]?.Sa;
			if (configArray == null || configArray.Length == 0)
			{
				return string.Empty;
			}

			return configArray[0];
		}

		private ScheduledTaskID[] GetScheduledTaskIds()
		{
			var dmaIdsRaw = _engine.GetScriptParam(ParamScheduledTaskDmaIds)?.Value;
			var taskIdsRaw = _engine.GetScriptParam(ParamScheduledTaskIds)?.Value;
			if (string.IsNullOrEmpty(dmaIdsRaw) || string.IsNullOrEmpty(taskIdsRaw))
			{
				_engine.ExitFail("Must at least provide one scheduled task!");
			}

			try
			{
				var dmaIds = JsonConvert.DeserializeObject<string[]>(dmaIdsRaw).Select(int.Parse).ToArray();
				var taskIds = JsonConvert.DeserializeObject<string[]>(taskIdsRaw).Select(int.Parse).ToArray();

				if (!dmaIds.Any() || !taskIds.Any())
				{
					_engine.ExitFail("Must at least provide one scheduled task!");
				}

				if (dmaIds.Length != taskIds.Length)
				{
					_engine.ExitFail("Something went wrong while parsing scheduled task IDs.");
				}

				return dmaIds.Select((t, i) => new ScheduledTaskID(t, taskIds[i])).ToArray();
			}
			catch (JsonSerializationException)
			{
				var dmaIds = Parse(dmaIdsRaw);
				var taskIds = Parse(taskIdsRaw);

				return dmaIds.Select((t, i) => new ScheduledTaskID(t, taskIds[i])).ToArray();

				int[] Parse(string s) => s.Replace(" ", string.Empty).Split(',')
					.Select(int.Parse)
					.ToArray();
			}
		}
	}
}
