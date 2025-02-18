using System;
using System.Collections.Generic;
using System.Linq;
using Skyline.DataMiner.Analytics.GenericInterface;
using Skyline.DataMiner.Net;
using Skyline.DataMiner.Net.Exceptions;
using Skyline.DataMiner.Net.Messages;

namespace ElementHomeAgents
{

    /// <summary>
    /// Reads the home property of the app and shows the dataminer agents
    /// </summary>
    [GQIMetaData(Name = "Element Home Agents")]
    public sealed class ElementHomeAgents : IGQIDataSource, IGQIOnInit
    {
        private const string SWARMING_PLAYGROUND_HOME_DMA_PROPERTY_NAME = "Swarming Playground Home DataMiner ID";
        
        private GQIDMS _dms;
        private IGQILogger _logger;
        private Dictionary<int, string> _agentIDToName = new Dictionary<int, string>();

        private readonly GQIColumn[] _columns = new GQIColumn[]
        {
            new GQIStringColumn("ElementID"),
            new GQIStringColumn("Element Name"),
            new GQIIntColumn("Home Agent ID"),
            new GQIStringColumn("Home Agent Name"),
        };

        /// <inheritdoc />
        public GQIColumn[] GetColumns() => _columns;

        /// <inheritdoc />
        public OnInitOutputArgs OnInit(OnInitInputArgs args)
        {
            if (args?.DMS == null)
                throw new ArgumentNullException($"{nameof(OnInitInputArgs)} or {nameof(GQIDMS)} is null.");

            _dms = args.DMS;
            _logger = args.Logger;

            _agentIDToName = LoadAgents().ToDictionary(agentInfo => agentInfo.ID, agentInfo => agentInfo.AgentName);

            return default;
        }

        /// <inheritdoc />
        public GQIPage GetNextPage(GetNextPageInputArgs args)
        {
            var rows = LoadElements()
                .Where(elementInfo => elementInfo.IsSwarmable)
                .Select(elementInfo => ToRow(elementInfo))
                .ToArray();

            return new GQIPage(rows)
            {
                HasNextPage = false,
            };
        }

        private GQIRow ToRow(ElementInfoEventMessage elementInfo)
        {
            var elementId = new ElementID(elementInfo.DataMinerID, elementInfo.ElementID);
            var homeAgentID = -1;
            if (int.TryParse(elementInfo.GetPropertyValue(SWARMING_PLAYGROUND_HOME_DMA_PROPERTY_NAME), out var parsed))
                homeAgentID = parsed;
            var homeAgentName = ToName(homeAgentID);
            return new GQIRow(
                    elementId.ToString(),
                    new[]
                    {
                        new GQICell() { Value = elementId.ToString(), DisplayValue = elementId.ToString() },
                        new GQICell() { Value = elementInfo.Name, DisplayValue = elementInfo.Name },
                        new GQICell() { Value = homeAgentID, DisplayValue = homeAgentID.ToString() },
                        new GQICell() { Value = homeAgentName, DisplayValue = homeAgentName },
                    });
        }

        private string ToName(int dmaID)
            => _agentIDToName.TryGetValue(dmaID, out var agentName)
                ? agentName
                : $"<No Home>";

        private GetDataMinerInfoResponseMessage[] LoadAgents()
        {
            if (_dms == null)
                throw new ArgumentNullException($"{nameof(GQIDMS)} is null.");

            DMSMessage[] resp = null;
            try
            {
                var req = new GetInfoMessage(InfoType.DataMinerInfo);
                resp = _dms.SendMessages(req);
            }
            catch (Exception ex)
            {
                throw new DataMinerSecurityException($"Issue occurred in {nameof(ElementHomeAgents)} when sending request {nameof(GetInfoMessage)}.{InfoType.DataMinerInfo}: {ex}", ex);
            }

            if (resp == null || resp.Length == 0)
                throw new Exception($"Response is null or empty");

            var dmaResponses = resp.OfType<GetDataMinerInfoResponseMessage>().ToArray();
            if (dmaResponses.Length == 0)
                throw new Exception($"{nameof(dmaResponses)} is empty");

            return dmaResponses;
        }

        private ElementInfoEventMessage[] LoadElements()
        {
            if (_dms == null)
                throw new ArgumentNullException($"{nameof(GQIDMS)} is null.");

            DMSMessage[] resp = null;
            try
            {
                var req = new GetInfoMessage(InfoType.ElementInfo);
                resp = _dms.SendMessages(req);
            }
            catch (Exception ex)
            {
                throw new DataMinerSecurityException($"Issue occurred in {nameof(ElementHomeAgents)} when sending request {nameof(GetInfoMessage)}.{InfoType.ElementInfo}: {ex}", ex);
            }

            if (resp == null)
                throw new Exception($"Response is null");

            return resp.OfType<ElementInfoEventMessage>().ToArray();
        }

    }
}
