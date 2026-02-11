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
	/// </summary>
	[GQIMetaData(Name = "Object Count Per Agent")]
	public sealed class ObjectCountPerAgent : IGQIDataSource, IGQIOnInit, IGQIUpdateable
	{
		private readonly GQIColumn[] _columns = new GQIColumn[]
		{
			new GQIIntColumn("Agent ID"),
			new GQIStringColumn("Agent Name"),
			new GQIStringColumn("Agent State"),
			new GQIIntColumn("Total Booking Count"),
			new GQIIntColumn("Total Element Count"),
			new GQIIntColumn("Non-Swarmable Element Count"),
			new GQIIntColumn("Swarmable Element Count"),
		};

		private GQIDMS _dms;
		private IGQIUpdater _updater;
		private IGQILogger _logger;

		private readonly Dictionary<int, RowData> _rows = new Dictionary<int, RowData>();
		private readonly Dictionary<ElementID, LiteElementInfoEvent> _elementCache = new Dictionary<ElementID, LiteElementInfoEvent>();
		private readonly Dictionary<Guid, ReservationInstance> _bookingCache = new Dictionary<Guid, ReservationInstance>();
		private Dictionary<int, GetDataMinerInfoResponseMessage> _dmInfoPerId = new Dictionary<int, GetDataMinerInfoResponseMessage>();

		private readonly ManualResetEventSlim _initialDataFetched = new ManualResetEventSlim(false);
		private readonly ConcurrentQueue<DMSMessage> _queuedUpdates = new ConcurrentQueue<DMSMessage>();

		private string _subscriptionSetId;

		public GQIColumn[] GetColumns() => _columns;

		public OnInitOutputArgs OnInit(OnInitInputArgs args)
		{
			_dms = args?.DMS ?? throw new ArgumentNullException($"{nameof(OnInitInputArgs)} or {nameof(GQIDMS)} is null.");
			_logger = args.Logger;

			return null;
		}

		public void OnStartUpdates(IGQIUpdater updater)
		{
			_updater = updater;
			_subscriptionSetId = $"DS-Combined-Counts-{Guid.NewGuid()}";

			var connection = _dms.GetConnection();

			connection.OnNewMessage += (sender, e) =>
			{
				if (e == null || !e.FromSet(_subscriptionSetId)) return;

				if (!_initialDataFetched.IsSet)
				{
					_queuedUpdates.Enqueue(e.Message);
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
			_logger.Debug("Subscriptions added for DataMinerInfoEvent, LiteElementInfoEvent, and ResourceManagerEventMessage.");
		}

		public GQIPage GetNextPage(GetNextPageInputArgs args)
		{
			_logger.Debug("Fetching initial data");
			var sw = Stopwatch.StartNew();

			try
			{
				var resp = _dms.SendMessages(
					new GetInfoMessage(InfoType.DataMinerInfo),
					new GetLiteElementInfo(includeStopped: true)
				);

				if (resp == null || resp.Length == 0)
					throw new InvalidOperationException("Initial SLNet response is null/empty.");

				var dmInfos = resp.OfType<GetDataMinerInfoResponseMessage>().ToArray();
				var liteElms = resp.OfType<LiteElementInfoEvent>().ToArray();
				_dmInfoPerId = dmInfos.ToDictionary(x => x.ID);

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
								AgentState = dm.ConnectionState.ToString(),
								TotalBookingCount = 0,
								TotalElementCount = 0,
								NonSwarmableElementCount = 0,
								SwarmableElementCount = 0
							};
						}
						else
						{
							_rows[dm.ID].AgentName = dm.AgentName;
							_rows[dm.ID].AgentState = dm.ConnectionState.ToString();
						}
					}
				}

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
								AgentName = _dmInfoPerId.TryGetValue(elm.HostingAgentID, out var info) ? info.AgentName : $"Agent {elm.HostingAgentID}",
								AgentState = _dmInfoPerId.TryGetValue(elm.HostingAgentID, out var info2) ? info2.ConnectionState.ToString() : "Unknown",
							};
							_rows[elm.HostingAgentID] = row;
						}

						row.TotalElementCount++;
						if (elm.IsSwarmable) row.SwarmableElementCount++;
						else row.NonSwarmableElementCount++;
					}
				}

				var rmHelper = new ResourceManagerHelper(_dms.SendMessage);
				var bookings = rmHelper.GetReservationInstances(new TRUEFilterElement<ReservationInstance>());

				lock (_rows)
				{
					foreach (var booking in bookings)
					{
						_bookingCache[booking.ID] = booking;

						if (!_rows.TryGetValue(booking.HostingAgentID, out var row))
						{
							row = new RowData
							{
								AgentID = booking.HostingAgentID,
								AgentName = _dmInfoPerId.TryGetValue(booking.HostingAgentID, out var info) ? info.AgentName : $"Agent {booking.HostingAgentID}",
								AgentState = _dmInfoPerId.TryGetValue(booking.HostingAgentID, out var info2) ? info2.ConnectionState.ToString() : "Unknown",
							};
							_rows[booking.HostingAgentID] = row;
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
				_logger.Error($"Error in {nameof(ObjectCountPerAgent)} GetNextPage: {ex}");
				throw;
			}
		}

		public void OnStopUpdates()
		{
			try
			{
				if (!string.IsNullOrEmpty(_subscriptionSetId))
				{
					_dms.GetConnection().ClearSubscriptions(_subscriptionSetId);
					_subscriptionSetId = null;
				}
			}
			catch (Exception ex)
			{
				_logger.Warning($"Failed to clear subscriptions: {ex}");
			}

			_initialDataFetched.Dispose();

			lock (_rows) _rows.Clear();
			_elementCache.Clear();
			_bookingCache.Clear();
			_dmInfoPerId.Clear();
			while (_queuedUpdates.TryDequeue(out _)) { }
		}

		private void ProcessQueuedUpdates()
		{
			_logger.Debug($"Processing {_queuedUpdates.Count} queued updates");
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

				case ResourceManagerEventMessage rmEvt:
					OnResourceManagerEvent(rmEvt);
					break;
			}
		}

		private void OnDataMinerInfoEvent(DataMinerInfoEvent dataMinerInfo)
		{
			var shouldPush = _initialDataFetched.IsSet;

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

					if (shouldPush)
					{
						_updater.AddRow(row.ToGQI());
					}
					return;
				}

				var newState = dataMinerInfo.Raw.ConnectionState.ToString();
				var changed = false;

				if (!string.Equals(row.AgentName, dataMinerInfo.AgentName, StringComparison.Ordinal))
				{
					row.AgentName = dataMinerInfo.AgentName;
					changed = true;
				}

				if (!string.Equals(row.AgentState, newState, StringComparison.Ordinal))
				{
					row.AgentState = newState;
					changed = true;
				}

				if (changed && shouldPush)
				{
					_updater.UpdateRow(row.ToGQI());
				}
			}
		}

		private void OnElementInfoEvent(LiteElementInfoEvent elementInfo)
		{
			var shouldPush = _initialDataFetched.IsSet;
			var elementId = new ElementID(elementInfo.DataMinerID, elementInfo.ElementID);

			lock (_rows)
			{
				if (!_rows.TryGetValue(elementInfo.HostingAgentID, out var hostRow))
				{
					hostRow = new RowData
					{
						AgentID = elementInfo.HostingAgentID,
						AgentName = _dmInfoPerId.TryGetValue(elementInfo.HostingAgentID, out var dm) ? dm.AgentName : $"Agent {elementInfo.HostingAgentID}",
						AgentState = _dmInfoPerId.TryGetValue(elementInfo.HostingAgentID, out var dm2) ? dm2.ConnectionState.ToString() : "Unknown",
					};
					_rows[elementInfo.HostingAgentID] = hostRow;
					if (shouldPush) _updater.AddRow(hostRow.ToGQI());
				}

				if (elementInfo.IsDeleted)
				{
					if (_elementCache.TryGetValue(elementId, out var oldElm))
					{
						_elementCache.Remove(elementId);

						hostRow.TotalElementCount--;
						if (oldElm.IsSwarmable)
						{
							hostRow.SwarmableElementCount--;
						}
						else
						{
							hostRow.NonSwarmableElementCount--;
						}

						if (shouldPush)
						{
							_updater.UpdateRow(hostRow.ToGQI());
						}
					}
					return;
				}

				if (!_elementCache.TryGetValue(elementId, out var existing))
				{
					_elementCache[elementId] = elementInfo;

					hostRow.TotalElementCount++;
					if (elementInfo.IsSwarmable)
					{
						hostRow.SwarmableElementCount++;
					}
					else
					{
						hostRow.NonSwarmableElementCount++;
					}

					if (shouldPush)
					{
						_updater.UpdateRow(hostRow.ToGQI());
					}
					return;
				}

				if (existing.HostingAgentID != elementInfo.HostingAgentID)
				{
					_elementCache[elementId] = elementInfo;

					if (_rows.TryGetValue(existing.HostingAgentID, out var oldHostRow))
					{
						oldHostRow.TotalElementCount--;
						if (existing.IsSwarmable)
						{
							oldHostRow.SwarmableElementCount--;
						}
						else
						{
							oldHostRow.NonSwarmableElementCount--;
						}
					}

					hostRow.TotalElementCount++;
					if (elementInfo.IsSwarmable)
					{
						hostRow.SwarmableElementCount++;
					}
					else
					{
						hostRow.NonSwarmableElementCount++;
					}

					if (shouldPush)
					{
						if (oldHostRow != null)
						{
							_updater.UpdateRow(oldHostRow.ToGQI());
						}

						_updater.UpdateRow(hostRow.ToGQI());
					}
				}
			}
		}

		private void OnResourceManagerEvent(ResourceManagerEventMessage rmEvent)
		{
			var shouldPush = _initialDataFetched.IsSet;

			lock (_rows)
			{
				foreach (var deleted in rmEvent.DeletedReservationInstanceObjects)
				{
					if (_bookingCache.TryGetValue(deleted.ID, out var old))
					{
						_bookingCache.Remove(deleted.ID);

						if (_rows.TryGetValue(old.HostingAgentID, out var row))
						{
							row.TotalBookingCount = Math.Max(0, row.TotalBookingCount - 1);
							if (shouldPush)
							{
								_updater.UpdateRow(row.ToGQI());
							}
						}
					}
				}

				foreach (var updated in rmEvent.UpdatedReservationInstances)
				{
					if (!_bookingCache.TryGetValue(updated.ID, out var old))
					{
						_bookingCache[updated.ID] = updated;

						if (!_rows.TryGetValue(updated.HostingAgentID, out var newHostRow))
						{
							newHostRow = new RowData
							{
								AgentID = updated.HostingAgentID,
								AgentName = _dmInfoPerId.TryGetValue(updated.HostingAgentID, out var dm) ? dm.AgentName : $"Agent {updated.HostingAgentID}",
								AgentState = _dmInfoPerId.TryGetValue(updated.HostingAgentID, out var dm2) ? dm2.ConnectionState.ToString() : "Unknown",
							};
							_rows[updated.HostingAgentID] = newHostRow;
							if (shouldPush)
							{
								_updater.AddRow(newHostRow.ToGQI());
							}
						}

						newHostRow.TotalBookingCount++;
						if (shouldPush)
						{
							_updater.UpdateRow(newHostRow.ToGQI());
						}
						continue;
					}

					var oldHost = old.HostingAgentID;
					var newHost = updated.HostingAgentID;

					_bookingCache[updated.ID] = updated;

					if (oldHost != newHost)
					{
						if (_rows.TryGetValue(oldHost, out var oldRow))
						{
							oldRow.TotalBookingCount = Math.Max(0, oldRow.TotalBookingCount - 1);
							if (shouldPush)
							{
								_updater.UpdateRow(oldRow.ToGQI());
							}
						}

						if (!_rows.TryGetValue(newHost, out var newRow))
						{
							newRow = new RowData
							{
								AgentID = newHost,
								AgentName = _dmInfoPerId.TryGetValue(newHost, out var dm) ? dm.AgentName : $"Agent {newHost}",
								AgentState = _dmInfoPerId.TryGetValue(newHost, out var dm2) ? dm2.ConnectionState.ToString() : "Unknown",
							};
							_rows[newHost] = newRow;
							if (shouldPush)
							{
								_updater.AddRow(newRow.ToGQI());
							}
						}

						_rows[newHost].TotalBookingCount++;
						if (shouldPush)
						{
							_updater.UpdateRow(_rows[newHost].ToGQI());
						}
					}

					// If host didn't change, we ignore (no count change).
				}
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
						new GQICell { Value = AgentID, DisplayValue = AgentID.ToString() },
						new GQICell { Value = AgentName, DisplayValue = AgentName },
						new GQICell { Value = AgentState, DisplayValue = AgentState },
						new GQICell { Value = TotalBookingCount, DisplayValue = TotalBookingCount.ToString() },
						new GQICell { Value = TotalElementCount, DisplayValue = TotalElementCount.ToString() },
						new GQICell { Value = NonSwarmableElementCount, DisplayValue = NonSwarmableElementCount.ToString() },
						new GQICell { Value = SwarmableElementCount, DisplayValue = SwarmableElementCount.ToString() },
					}
				);
			}
		}
	}
}