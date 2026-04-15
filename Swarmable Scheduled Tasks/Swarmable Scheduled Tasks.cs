using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Skyline.DataMiner.Analytics.GenericInterface;
using Skyline.DataMiner.Net.DMSState.Agents;
using Skyline.DataMiner.Net.Exceptions;
using Skyline.DataMiner.Net.Messages;

namespace SwarmableScheduledTasks
{
	/// <summary>
	/// Shows the scheduled tasks per agent
	/// </summary>
	[GQIMetaData(Name = "Swarmable Scheduled Tasks")]
	public sealed class SwarmableScheduledTasks : IGQIDataSource, IGQIOnInit
	{
		private GQIDMS _dms;
		private readonly ConcurrentDictionary<string, GQIRow> _currentRows = new ConcurrentDictionary<string, GQIRow>();
		private Dictionary<int, GetDataMinerInfoResponseMessage> _dmInfoPerId = new Dictionary<int, GetDataMinerInfoResponseMessage>();
		private List<SchedulerTask> _cachedTasks;
		private const int PageSize = 50;
		private int _currentIndex;

		public GQIColumn[] GetColumns()
		{
			return new GQIColumn[]
			{
				new GQIStringColumn("Task name"),
				new GQIIntColumn("DMA ID"),
				new GQIIntColumn("Task ID"),
				new GQIIntColumn("Executing DMA ID"),
				new GQIStringColumn("Executing DMA"),
				new GQIStringColumn("Type"),
			};
		}

		public OnInitOutputArgs OnInit(OnInitInputArgs args)
		{
			_dms = args?.DMS ?? throw new ArgumentNullException($"{nameof(OnInitInputArgs)} or {nameof(GQIDMS)} is null.");

			GetAgentsInCluster();

			return null;
		}

		public GQIPage GetNextPage(GetNextPageInputArgs args)
		{
			if (_cachedTasks == null)
			{
				var msg = new GetInfoMessage(InfoType.SchedulerTasks);
				GetSchedulerTasksResponseMessage resp;
				try
				{
					resp = _dms.SendMessage(msg) as GetSchedulerTasksResponseMessage;
				}
				catch (Exception e)
				{
					throw new DataMinerException($"Issue in {nameof(SwarmableScheduledTasks)} while getting scheduled tasks. {e}", e);
				}

				_cachedTasks = resp?.Tasks.Cast<SchedulerTask>().ToList() ?? new List<SchedulerTask>();
				_currentIndex = 0;
			}

			if (_currentIndex >= _cachedTasks.Count)
			{
				return new GQIPage(new GQIRow[] { }) {HasNextPage = false};
			}

			var rows = _cachedTasks.Skip(_currentIndex).Take(PageSize).Select(CreateRow).ToArray();
			_currentIndex += PageSize;
			return new GQIPage(rows) {HasNextPage = _currentIndex < _cachedTasks.Count};
		}

		private void GetAgentsInCluster()
		{
			DMSMessage[] resp;
			try
			{
				resp = _dms.SendMessages(new GetInfoMessage(InfoType.DataMinerInfo));
			}
			catch (Exception ex)
			{
				throw new DataMinerException($"Issue in {nameof(SwarmableScheduledTasks)} while getting DataMiner info. {ex}", ex);
			}

			if (resp == null || resp.Length == 0)
				throw new InvalidOperationException("No DataMiner info returned.");

			_dmInfoPerId = resp.OfType<GetDataMinerInfoResponseMessage>().ToDictionary(info => info.ID);
		}

		private GQIRow CreateRow(SchedulerTask task)
		{
			_dmInfoPerId.TryGetValue(task.ExecutingDmaId, out var dmaInfo);

			var cells = new GQICell[]
			{
				new GQICell() {Value = task.TaskName},
				new GQICell() {Value = task.HandlingDMA},
				new GQICell() {Value = task.Id},
				new GQICell() {Value = task.ExecutingDmaId},
				new GQICell() {Value = dmaInfo?.AgentName ?? "Unknown"},
				new GQICell() {Value = task.RepeatType.ToString()}
			};

			var row = new GQIRow($"{task.HandlingDMA}/{task.Id}", cells);
			_currentRows.AddOrUpdate(row.Key, row, (_, __) => row);

			return row;
		}
	}
}
