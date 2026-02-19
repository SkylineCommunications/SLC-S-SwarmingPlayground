using System;
using Skyline.DataMiner.Automation;
using Skyline.DataMiner.Net.Messages;
using Skyline.DataMiner.Net.Messages.SLDataGateway;
using Skyline.DataMiner.Net.ResourceManager.Objects;
using Skyline.DataMiner.Utils.InteractiveAutomationScript;
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
		private InteractiveController _controller;

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
			_engine = engine;

			_agentInfos = _engine.GetAgents();

			if (!Check.IfSwarmingIsEnabled(_agentInfos))
				engine.ExitFail(
					"Swarming is not enabled in this DMS. More info: https://aka.dataminer.services/Swarming");

			_controller = new InteractiveController(engine);
			var dialog = new ConfirmationDialog(engine);
			dialog.CancelButton.Pressed += (sender, args) => engine.ExitSuccess("Canceled swarming");
			dialog.ContinueButton.Pressed += (sender, args) =>
			{
				var config = new ClusterConfig(engine, _agentInfos);

				SwarmElementsIfEnabled(config);

				SwarmBookingsIfEnabled(config);

				engine.ExitSuccess("");
			};

			_controller.ShowDialog(dialog);
		}

		private void SwarmElementsIfEnabled(ClusterConfig config)
		{
			if (!_engine.IsSwarmingFlagEnabled(ParamSwarmElements))
			{
				_engine.Log("Not swarming elements since option is not enabled.");
				return;
			}

			var elementInfos = _engine.GetElements();
			config.InitializeAgentToElements(elementInfos);
			config.RedistributeElementsByCount(element => element.IsSwarmable);
			config.SwarmElements();
		}

		private void SwarmBookingsIfEnabled(ClusterConfig config)
		{
			if (!_engine.IsSwarmingFlagEnabled(ParamSwarmBookings))
			{
				_engine.Log("Not swarming bookings since option is not enabled.");
				return;
			}

			var rmHelper = new ResourceManagerHelper(_engine.SendSLNetSingleResponseMessage);
			var filter = ReservationInstanceExposers.End.GreaterThan(DateTime.UtcNow)
				.AND(ReservationInstanceExposers.Status.NotEqual((int) ReservationStatus.Canceled));
			var bookings = rmHelper.GetReservationInstances(filter);

			config.InitializeAgentToBookings(bookings);
			config.RedistributeBookingsByCount(booking => booking.Status != ReservationStatus.Ongoing);
			config.SwarmBookings();
		}
	}

	public class ConfirmationDialog : Dialog
	{
		private Label Label;
		public Button CancelButton { get; private set; }
		public Button ContinueButton { get; private set; }

		public ConfirmationDialog(IEngine engine) : base(engine)
		{
			Title = "Swarming Confirmation";
			Label = new Label("You are about to swarm (a lot of) elements/bookings, which could take a while. Are you sure you want to continue?");
			ContinueButton = new Button("Continue");
			CancelButton = new Button("Cancel");
			AddWidget(Label, 0, 0, 1, 2);
			AddWidget(ContinueButton, 1, 0);
			AddWidget(CancelButton, 1, 1);
		}
	}
}
