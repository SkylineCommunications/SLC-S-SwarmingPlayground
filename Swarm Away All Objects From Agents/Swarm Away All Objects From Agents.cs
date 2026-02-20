using System;
using System.Linq;
using Skyline.DataMiner.Automation;
using Skyline.DataMiner.Net.Messages;
using Skyline.DataMiner.Net.Messages.SLDataGateway;
using Skyline.DataMiner.Net.ResourceManager.Objects;
using Skyline.DataMiner.Utils.InteractiveAutomationScript;
using Swarming_Playground_Shared;

namespace SwarmAwayAllObjectsFromAgents
{
    /// <summary>
    /// Represents a DataMiner Automation script.
    /// </summary>
    public class Script
    {
	    private InteractiveController _controller;

		private const string PARAM_SOURCE_AGENT_IDS = "Source Agent IDs";
        private const string ParamSwarmElements = "Swarm Elements";
        private const string ParamSwarmBookings = "Swarm Bookings";

        private IEngine _engine;

		/// <summary>
		/// The script entry point.
		/// </summary>
		/// <param name="engine">Link with SLAutomation process.</param>
		public void Run(IEngine engine)
        {
	        // DO NOT REMOVE THIS COMMENTED OUT CODE OR THE SCRIPT WONT RUN!
	        // DataMiner evaluates if the script needs to launch in interactive mode.
	        // This is determined by a simple string search looking for “engine.ShowUI” in the source code.
	        // However, due to the toolkit nuget package, this string cannot be found here.
	        // So this comment is here as a workaround.
	        //// engine.ShowUI();

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

            var agentInfos = engine.GetAgents();

            if (!Check.IfSwarmingIsEnabled(agentInfos))
                engine.ExitFail("Swarming is not enabled in this DMS. More info: https://aka.dataminer.services/Swarming");

            var sourceAgentIds = engine.GetScriptParamInts(PARAM_SOURCE_AGENT_IDS);

            if (!sourceAgentIds.Any())
                engine.ExitFail("Must at least provide one agent!");

            if (!agentInfos.Select(agentinfo => agentinfo.ID).Except(sourceAgentIds).Any())
                engine.ExitFail("Cannot swarm away all elements/bookings from all agents");

            if (!agentInfos.Where(agentInfo => agentInfo.ConnectionState == DataMinerAgentConnectionState.Normal).Select(agentinfo => agentinfo.ID).Except(sourceAgentIds).Any())
                engine.ExitFail("Must at least provide one agent!");

            foreach (var sourceAgentId in sourceAgentIds)
            {
                if (!agentInfos.Any(agentInfo => agentInfo.ID == sourceAgentId))
                    engine.ExitFail($"Source agent '{sourceAgentId}' is not part of the cluster");
            }

			_controller = new InteractiveController(engine);
			var dialog = new ConfirmationDialog(engine);
			dialog.CancelButton.Pressed += (sender, args) => engine.ExitSuccess("Canceled swarming");
			dialog.ContinueButton.Pressed += (sender, args) =>
			{
				var clusterConfig = new ClusterConfig(_engine, agentInfos);
				SwarmElementsAwayIfEnabled(clusterConfig, sourceAgentIds);

				SwarmBookingsAwayIfEnabled(clusterConfig, sourceAgentIds);

				engine.ExitSuccess("");
			};
			
			_controller.ShowDialog(dialog);
        }

        private void SwarmElementsAwayIfEnabled(ClusterConfig config, int[] sourceAgentIds)
        {
	        if (!_engine.IsSwarmingFlagEnabled(ParamSwarmElements))
	        {
				_engine.Log("Not swarming elements since option is not enabled.");
				return;
			}

	        _engine.Log("Swarming elements away from agent");

			var elementInfos = _engine.GetElements();
	        config.InitializeAgentToElements(elementInfos);
			config.RedistributeElementsAwayFromAgents(sourceAgentIds, element => element.IsSwarmable);
	        config.SwarmElements();
		}

        private void SwarmBookingsAwayIfEnabled(ClusterConfig config, int[] sourceAgentIds)
        {
	        if (!_engine.IsSwarmingFlagEnabled(ParamSwarmBookings))
	        {
		        _engine.Log("Not swarming bookings since option is not enabled.");
		        return;
	        }

			_engine.Log("Swarming bookings away from agent");

	        var rmHelper = new ResourceManagerHelper(_engine.SendSLNetSingleResponseMessage);
	        var now = DateTime.UtcNow;
	        var filter = ReservationInstanceExposers.End.GreaterThan(now).AND(ReservationInstanceExposers.Status.NotEqual((int)ReservationStatus.Canceled));
	        var bookings = rmHelper.GetReservationInstances(filter);

			config.InitializeAgentToBookings(bookings);
	        config.RedistributeBookingsAwayFromAgents(sourceAgentIds, booking => booking.Status != ReservationStatus.Ongoing);
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
