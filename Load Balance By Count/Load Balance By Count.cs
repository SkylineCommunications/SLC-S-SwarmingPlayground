using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using Skyline.DataMiner.Automation;
using Skyline.DataMiner.Net.Messages;
using Skyline.DataMiner.Net.Messages.SLDataGateway;
using Skyline.DataMiner.Net.ResourceManager.Objects;
using Swarming_Playground_Shared;

namespace LoadBalanceByCount
{
	/// <summary>
	/// Represents a DataMiner Automation script.
	/// </summary>
	public class Script
	{
		private IEngine _engine;
		private GetDataMinerInfoResponseMessage[] _agentInfos;

		private const string ParamSwarmElements = "Swarm Elements";
		private const string ParamSwarmBookings = "Swarm Bookings";

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
			engine.GenerateInformation("This script is running");
			_engine = engine;

			_agentInfos = _engine.GetAgents();

			if (!Check.IfSwarmingIsEnabled(_agentInfos))
				engine.ExitFail(
					"Swarming is not enabled in this DMS. More info: https://aka.dataminer.services/Swarming");

			// TODO check if licensed ?

			var config = new ClusterConfig(engine, _agentInfos);

			SwarmElementsIfEnabled(config);

			SwarmBookingsIfEnabled(config);
		}

		private void SwarmElementsIfEnabled(ClusterConfig config)
		{
			if (!IsSwarmingFlagEnabled(ParamSwarmElements))
			{
				_engine.Log("Not swarming elements since option is not enabled.");
				return;
			}

			var elementInfos = _engine.GetElements();
			config.InitializeAgentToElements(elementInfos);
			config.RedistributeElementsByCount(element => !element.IsSwarmable, element => element.IsSwarmable);
			config.SwarmElements();
		}

		private void SwarmBookingsIfEnabled(ClusterConfig config)
		{
			if (!IsSwarmingFlagEnabled(ParamSwarmBookings))
			{
				_engine.Log("Not swarming bookings since option is not enabled.");
				return;
			}

			var rmHelper = new ResourceManagerHelper(_engine.SendSLNetSingleResponseMessage);
			var bookings = rmHelper.GetReservationInstances(new TRUEFilterElement<ReservationInstance>());

			config.InitializeAgentToBookings(bookings);
			config.RedistributeBookingsByCount(booking => booking.Status == ReservationStatus.Ongoing, booking => booking.Status != ReservationStatus.Ongoing);
			config.SwarmBookings();
		}

		private bool IsSwarmingFlagEnabled(string param)
		{
			var swarmBookingsRaw = _engine.GetScriptParam(param)?.Value;
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

			return swarmBookingsEnabled;
		}
	}
}
