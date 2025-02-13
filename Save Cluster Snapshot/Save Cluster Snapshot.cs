using System;
using System.Linq;
using System.Threading.Tasks;
using Skyline.DataMiner.Automation;
using Skyline.DataMiner.Core.DataMinerSystem.Automation;
using Skyline.DataMiner.Core.DataMinerSystem.Common;
using Swarming_Playground_Shared;

namespace SaveClusterSnapshot
{
    /// <summary>
    /// Represents a DataMiner Automation script.
    /// </summary>
    public class Script
    {
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
            engine.SetFlag(RunTimeFlags.NoCheckingSets);

            // verify if property exists, create otherwise
            var dms = engine.GetDms();
            if (!dms.PropertyExists(Constants.SWARMING_PLAYGROUND_HOME_DMA_PROPERTY_NAME, PropertyType.Element))
            {
                dms.CreateProperty(
                    Constants.SWARMING_PLAYGROUND_HOME_DMA_PROPERTY_NAME,
                    PropertyType.Element,
                    isFilterEnabled: false,
                    isReadOnly: false,
                    isVisibleInSurveyor: false);
            }

            var elements = engine
                .GetElements()
                .Where(elementInfo => elementInfo.IsSwarmable)
                .Where(elementInfo =>
                {
                    var propValue = elementInfo.GetPropertyValue(Constants.SWARMING_PLAYGROUND_HOME_DMA_PROPERTY_NAME);
                    return propValue == null || propValue != elementInfo.HostingAgentID.ToString();
                })
                .ToArray();

            Parallel.ForEach(elements, element =>
            {
                var engineElement = engine.FindElement(element.DataMinerID, element.ElementID);

                if (engineElement == null)
                    return;

                engineElement.SetPropertyValue(
                    Constants.SWARMING_PLAYGROUND_HOME_DMA_PROPERTY_NAME,
                    element.HostingAgentID.ToString());
            });
        }
    }
}
