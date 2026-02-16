using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Skyline.DataMiner.Analytics.GenericInterface;
using Skyline.DataMiner.Net;
using Skyline.DataMiner.Net.Messages;
using Skyline.DataMiner.Net.Messages.SLDataGateway;
using Skyline.DataMiner.Net.ResourceManager.Objects;

namespace ObjectCountPerAgent
{
	/// <summary>
	/// Shows booking & element counts per agent (swarmable + non-swarmable) and agent status.
	/// Uses debounced re-counts for bookings.
	/// </summary>
	[GQIMetaData(Name = "Object Count Per Agent")]
	public sealed class ObjectCountPerAgent : IGQIDataSource, IGQIOnInit, IGQIUpdateable
	{
		#region Columns

		private readonly GQIColumn[] _columns = new GQIColumn[]
		{
			new GQIIntColumn("Agent ID"),
			new GQIStringColumn("Agent Name"),
			new GQIStringColumn("Agent State"),
			new GQIIntColumn("Upcoming Booking Count"),
			new GQIIntColumn("Total Element Count"),
			new GQIIntColumn("Non-Swarmable Element Count"),
			new GQIIntColumn("Swarmable Element Count"),
		};

		public GQIColumn[] GetColumns() => _columns;

		#endregion

		#region Fields

		private GQIDMS _dms;
		private IGQIUpdater _updater;
		private IGQILogger _logger;

		private readonly Dictionary<int, RowData> _rows = new Dictionary<int, RowData>();
		private readonly Dictionary<ElementID, LiteElementInfoEvent> _elementCache = new Dictionary<ElementID, LiteElementInfoEvent>();

		private Dictionary<int, GetDataMinerInfoResponseMessage> _dmInfoPerId = new Dictionary<int, GetDataMinerInfoResponseMessage>();

		private readonly ManualResetEventSlim _initialDataFetched = new ManualResetEventSlim(false);
		private readonly ConcurrentQueue<DMSMessage> _queuedUpdates = new ConcurrentQueue<DMSMessage>();

		private string _subscriptionSetId;

		private Timer _debounceTimer;
		private readonly object _timerLock = new object();
		private volatile bool _rmEventReceived = false;
		private readonly TimeSpan _debounce = TimeSpan.FromSeconds(3);

		#endregion

		#region Init

		public OnInitOutputArgs OnInit(OnInitInputArgs args)
		{
			_dms = args?.DMS ?? throw new ArgumentNullException($"{nameof(OnInitInputArgs)} or {nameof(GQIDMS)} is null.");
			_logger = args.Logger;
			return null;
		}

		#endregion

		#region Start Updates

		public void OnStartUpdates(IGQIUpdater updater)
		{
			_updater = updater;
			_subscriptionSetId = $"DS-Combined-Counts-{Guid.NewGuid()}";

			var connection = _dms.GetConnection();

			lock (_timerLock)
			{
				_debounceTimer = new Timer(_ => DebouncedRefresh(), null,
					Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
			}

			connection.OnNewMessage += (sender, e) =>
			{
				if (e == null || !e.FromSet(_subscriptionSetId)) return;

				if (!_initialDataFetched.IsSet)
				{
					_queuedUpdates.Enqueue(e.Message);
					return;
				}

				if (e.Message is ResourceManagerEventMessage)
				{
					_rmEventReceived = true;
					lock (_timerLock)
					{
						_debounceTimer?.Change(_debounce, Timeout.InfiniteTimeSpan);
					}
					return;
				}

				OnEvent(e.Message);
			};

			var tracker = connection.TrackAddSubscription(
				_subscriptionSetId,
				new SubscriptionFilter(typeof(DataMinerInfoEvent), SubscriptionFilterOptions.SkipInitialEvents),
				new SubscriptionFilter(typeof(LiteElementInfoEvent), SubscriptionFilterOptions.SkipInitialEvents),
				new SubscriptionFilter(typeof(ResourceManagerEventMessage))
			);

			tracker.ExecuteAndWait(TimeSpan.FromMinutes(5));
			_logger.Debug("Subscriptions added (DMInfo, LiteElementInfo, RMEvent).");
		}

		#endregion

		public GQIPage GetNextPage(GetNextPageInputArgs args)
		{
			_logger.Debug("Fetching initial data");
			var sw = Stopwatch.StartNew();

			try
			{
				// Fetch DM info + element info
				var resp = _dms.SendMessages(
					new GetInfoMessage(InfoType.DataMinerInfo),
					new GetLiteElementInfo(includeStopped: true)
				);

				var dmInfos = resp.OfType<GetDataMinerInfoResponseMessage>().ToArray();
				var liteElms = resp.OfType<LiteElementInfoEvent>().ToArray();
				_dmInfoPerId = dmInfos.ToDictionary(x => x.ID);

				// Initialize rows for all agents
				lock (_rows)
				{
					foreach (var dm in dmInfos)
					{
						if (!_rows.ContainsKey(dm.ID))
						{
							_rows[dm.ID] = new RowData
							{
								AgentID = dm.ID,
								AgentName = dm.AgentName,
								AgentState = dm.ConnectionState.ToString()
							};
						}
						else
						{
							_rows[dm.ID].AgentName = dm.AgentName;
							_rows[dm.ID].AgentState = dm.ConnectionState.ToString();
						}
					}
				}

				// Apply element counts
				lock (_rows)
				{
					foreach (var elm in liteElms)
					{
						var elementId = new ElementID(elm.DataMinerID, elm.ElementID);
						_elementCache[elementId] = elm;

						if (!_rows.TryGetValue(elm.HostingAgentID, out var row))
						{
							row = new RowData
							{
								AgentID = elm.HostingAgentID,
								AgentName = _dmInfoPerId.TryGetValue(elm.HostingAgentID, out var dm)
									? dm.AgentName
									: $"Agent {elm.HostingAgentID}",
								AgentState = _dmInfoPerId.TryGetValue(elm.HostingAgentID, out var dm2)
									? dm2.ConnectionState.ToString()
									: "Unknown",
							};
							_rows[elm.HostingAgentID] = row;
						}

						row.TotalElementCount++;
						if (elm.IsSwarmable) row.SwarmableElementCount++;
						else row.NonSwarmableElementCount++;
					}
				}

				// Initial booking count (debounced later)
				var rmHelper = new ResourceManagerHelper(_dms.SendMessage);
				var filter = ReservationInstanceExposers.End.GreaterThan(DateTime.UtcNow)
					.AND(ReservationInstanceExposers.Status.NotEqual((int)ReservationStatus.Canceled));
				var bookings = rmHelper.GetReservationInstances(filter);

				lock (_rows)
				{
					foreach (var b in bookings)
					{
						if (!_rows.TryGetValue(b.HostingAgentID, out var row))
						{
							row = new RowData
							{
								AgentID = b.HostingAgentID,
								AgentName = _dmInfoPerId.TryGetValue(b.HostingAgentID, out var dm)
									? dm.AgentName
									: $"Agent {b.HostingAgentID}",
								AgentState = _dmInfoPerId.TryGetValue(b.HostingAgentID, out var dm2)
									? dm2.ConnectionState.ToString()
									: "Unknown",
							};
							_rows[b.HostingAgentID] = row;
						}

						row.TotalBookingCount++;
					}
				}

				sw.Stop();
				_logger.Debug($"Initial fetch completed in {sw.ElapsedMilliseconds}ms");

				var firstPageRows = _rows.Values.Select(r => r.ToGQI()).ToArray();

				ProcessQueuedUpdates();

				_initialDataFetched.Set();

				return new GQIPage(firstPageRows) { HasNextPage = false };
			}
			catch (Exception ex)
			{
				_logger.Error($"Error fetching initial page: {ex}");
				throw;
			}
		}

		public void OnStopUpdates()
		{
			try
			{
				_dms.GetConnection().ClearSubscriptions(_subscriptionSetId);
				_subscriptionSetId = null;
			}
			catch (Exception ex)
			{
				_logger.Warning($"Failed clearing subscriptions: {ex}");
			}

			lock (_timerLock)
			{
				_debounceTimer?.Dispose();
				_debounceTimer = null;
			}

			_initialDataFetched.Dispose();

			lock (_rows)
			{
				_rows.Clear();
			}
			_elementCache.Clear();
			_dmInfoPerId.Clear();

			while (_queuedUpdates.TryDequeue(out _)) { }
		}

		private void ProcessQueuedUpdates()
		{
			while (_queuedUpdates.TryDequeue(out var msg))
			{
				OnEvent(msg);
			}
		}

		private void OnEvent(DMSMessage msg)
		{
			switch (msg)
			{
				case DataMinerInfoEvent dmEvt:
					OnDataMinerInfoEvent(dmEvt);
					break;

				case LiteElementInfoEvent elmEvt:
					OnElementInfoEvent(elmEvt);
					break;
			}
		}

		private void OnDataMinerInfoEvent(DataMinerInfoEvent dataMinerInfo)
		{
			if (!_initialDataFetched.IsSet) return;

			lock (_rows)
			{
				if (!_rows.TryGetValue(dataMinerInfo.DataMinerID, out var row))
				{
					row = new RowData
					{
						AgentID = dataMinerInfo.DataMinerID,
						AgentName = dataMinerInfo.AgentName,
						AgentState = dataMinerInfo.Raw.ConnectionState.ToString(),
					};
					_rows[dataMinerInfo.DataMinerID] = row;

					_updater.AddRow(row.ToGQI());
					return;
				}

				var changed = false;

				var newState = dataMinerInfo.Raw.ConnectionState.ToString();

				if (!string.Equals(row.AgentName, dataMinerInfo.AgentName))
				{
					row.AgentName = dataMinerInfo.AgentName;
					changed = true;
				}

				if (!string.Equals(row.AgentState, newState))
				{
					row.AgentState = newState;
					changed = true;
				}

				if (changed)
				{
					_updater.UpdateRow(row.ToGQI());
				}
			}
		}

		private void OnElementInfoEvent(LiteElementInfoEvent elementInfo)
		{
			if (!_initialDataFetched.IsSet)
			{
				return;
			}

			var elementId = new ElementID(elementInfo.DataMinerID, elementInfo.ElementID);

			lock (_rows)
			{
				if (!_rows.TryGetValue(elementInfo.HostingAgentID, out var row))
				{
					row = new RowData
					{
						AgentID = elementInfo.HostingAgentID,
						AgentName = _dmInfoPerId.TryGetValue(elementInfo.HostingAgentID, out var dm) ? dm.AgentName : $"Agent {elementInfo.HostingAgentID}",
						AgentState = _dmInfoPerId.TryGetValue(elementInfo.HostingAgentID, out var dm2) ? dm2.ConnectionState.ToString() : "Unknown",
					};
					_rows[elementInfo.HostingAgentID] = row;

					_updater.AddRow(row.ToGQI());
				}

				// Handle deletions
				if (elementInfo.IsDeleted)
				{
					if (_elementCache.TryGetValue(elementId, out var oldElm))
					{
						_elementCache.Remove(elementId);

						row.TotalElementCount--;
						if (oldElm.IsSwarmable)
						{
							row.SwarmableElementCount--;
						}
						else
						{
							row.NonSwarmableElementCount--;
						}

						_updater.UpdateRow(row.ToGQI());
					}
					return;
				}

				if (!_elementCache.TryGetValue(elementId, out var existing))
				{
					_elementCache[elementId] = elementInfo;

					row.TotalElementCount++;
					if (elementInfo.IsSwarmable)
					{
						row.SwarmableElementCount++;
					}
					else
					{
						row.NonSwarmableElementCount++;
					}

					_updater.UpdateRow(row.ToGQI());
					return;
				}

				if (existing.HostingAgentID != elementInfo.HostingAgentID)
				{
					_elementCache[elementId] = elementInfo;

					if (_rows.TryGetValue(existing.HostingAgentID, out var oldRow))
					{
						oldRow.TotalElementCount--;
						if (existing.IsSwarmable)
						{
							oldRow.SwarmableElementCount--;
						}
						else
						{
							oldRow.NonSwarmableElementCount--;
						}

						_updater.UpdateRow(oldRow.ToGQI());
					}

					row.TotalElementCount++;
					if (elementInfo.IsSwarmable)
					{
						row.SwarmableElementCount++;
					}
					else
					{
						row.NonSwarmableElementCount++;
					}

					_updater.UpdateRow(row.ToGQI());
				}
			}
		}

		private void DebouncedRefresh()
		{
			try
			{
				if (!_rmEventReceived)
				{
					return;
				}
				_rmEventReceived = false;

				var rmHelper = new ResourceManagerHelper(_dms.SendMessage);

				var baseFilter =
					ReservationInstanceExposers.End.GreaterThan(DateTime.UtcNow)
						.AND(ReservationInstanceExposers.Status.NotEqual((int)ReservationStatus.Canceled));

				lock (_rows)
				{
					foreach (var dm in _dmInfoPerId)
					{
						var agentId = dm.Key;
						var filter = baseFilter.AND(ReservationInstanceExposers.HostingAgentID.Equal(agentId));
						var count = (int)rmHelper.CountReservationInstances(filter);

						if (!_rows.TryGetValue(agentId, out var row))
						{
							row = new RowData
							{
								AgentID = agentId,
								AgentName = dm.Value.AgentName,
								AgentState = dm.Value.ConnectionState.ToString(),
							};
							_rows[agentId] = row;
							_updater.AddRow(row.ToGQI());
						}

						if (row.TotalBookingCount != count)
						{
							row.TotalBookingCount = count;
							_updater.UpdateRow(row.ToGQI());
						}
					}
				}
			}
			catch (Exception ex)
			{
				_logger.Error($"DebouncedRefresh failed: {ex}");
			}
		}

		private sealed class RowData
		{
			public int AgentID { get; set; }
			public string AgentName { get; set; }
			public string AgentState { get; set; }
			public int TotalBookingCount { get; set; }
			public int TotalElementCount { get; set; }
			public int NonSwarmableElementCount { get; set; }
			public int SwarmableElementCount { get; set; }

			public GQIRow ToGQI()
			{
				return new GQIRow(
					AgentID.ToString(),
					new[]
					{
						new GQICell { Value = AgentID },
						new GQICell { Value = AgentName },
						new GQICell { Value = AgentState },
						new GQICell { Value = TotalBookingCount },
						new GQICell { Value = TotalElementCount },
						new GQICell { Value = NonSwarmableElementCount },
						new GQICell { Value = SwarmableElementCount },
					}
				);
			}
		}
	}
}