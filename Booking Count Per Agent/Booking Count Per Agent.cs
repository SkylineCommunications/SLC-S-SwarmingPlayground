using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Skyline.DataMiner.Analytics.GenericInterface;
using Skyline.DataMiner.Net;
using Skyline.DataMiner.Net.Exceptions;
using Skyline.DataMiner.Net.Helper;
using Skyline.DataMiner.Net.Messages;
using Skyline.DataMiner.Net.Messages.SLDataGateway;
using Skyline.DataMiner.Net.ResourceManager.Objects;

namespace BookingCountPerAgent
{
	/// <summary>
	/// Shows the booking count per agent
	/// </summary>
	[GQIMetaData(Name = "Booking Count Per Agent")]
	public sealed class BookingCountPerAgent : IGQIDataSource, IGQIOnInit, IGQIUpdateable
	{
		private GQIDMS _dms;
		private IGQILogger _logger;
		private ResourceManagerHelper _rmHelper;
		private IGQIUpdater _updater;
		private Dictionary<int, GetDataMinerInfoResponseMessage> _dmInfoPerId = new Dictionary<int, GetDataMinerInfoResponseMessage>();
		private readonly ConcurrentDictionary<string, GQIRow> _currentRows = new ConcurrentDictionary<string, GQIRow>();
		private string _subscriptionSetId = $"DS-Booking-Count-Per-Agent-{Guid.NewGuid()}";
		private Timer _debounceTimer;
		private readonly object _timerLock = new object();
		private volatile bool _eventReceived = false;
		private readonly TimeSpan _debounce = TimeSpan.FromSeconds(3);


		public GQIColumn[] GetColumns()
		{
			return new GQIColumn[]
			{
				new GQIIntColumn("Agent ID"),
				new GQIStringColumn("Agent Name"),
				new GQIStringColumn("Agent State"),
				new GQIIntColumn("Total Booking Count"),
			};
		}

		public OnInitOutputArgs OnInit(OnInitInputArgs args)
		{
			_dms = args?.DMS ?? throw new ArgumentNullException($"{nameof(OnInitInputArgs)} or {nameof(GQIDMS)} is null.");
			_logger = args.Logger;
			_rmHelper = new ResourceManagerHelper(_dms.SendMessage);

			return null;
		}

		public void OnStartUpdates(IGQIUpdater updater)
		{
			_logger.Debug("OnStartUpdates");
			_updater = updater;

			GetAgentsInCluster();
			InitializeRowsForAllAgents();

			_dms.GetConnection().OnNewMessage += HandleBookingUpdate;

			lock (_timerLock)
			{
				_debounceTimer = new Timer(_ => DebouncedRefresh(), null,
					Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
			}

			var tracker = _dms.GetConnection().TrackAddSubscription(
				_subscriptionSetId,
				new SubscriptionFilter(typeof(ResourceManagerEventMessage))
			);
			tracker.ExecuteAndWait(TimeSpan.FromMinutes(5));
			_logger.Debug("Done adding subscription");
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
				throw new DataMinerException($"Issue in {nameof(BookingCountPerAgent)} while getting DataMiner info. {ex}", ex);
			}

			if (resp == null || resp.Length == 0)
				throw new InvalidOperationException("No DataMiner info returned.");

			_dmInfoPerId = resp.OfType<GetDataMinerInfoResponseMessage>().ToDictionary(info => info.ID);
		}

		private void InitializeRowsForAllAgents()
		{
			foreach (var kv in _dmInfoPerId)
			{
				var info = kv.Value;
				var key = kv.Key.ToString();

				var row = new GQIRow(key, new[]
				{
					new GQICell { Value = info.ID },
					new GQICell { Value = info.AgentName },
					new GQICell { Value = info.ConnectionState.ToString()},
					new GQICell { Value = 0 },
				});

				_currentRows.TryAdd(key, row);
			}
		}

		private void HandleBookingUpdate(object sender, NewMessageEventArgs args)
		{
			if (!(args.Message is ResourceManagerEventMessage))
			{
				return;
			}

			_eventReceived = true;

			lock (_timerLock)
			{
				_debounceTimer?.Change(_debounce, Timeout.InfiniteTimeSpan);
			}
		}

		private void DebouncedRefresh()
		{
			try
			{
				if (!_eventReceived)
					return;

				_eventReceived = false;

				var baseFilter = ReservationInstanceExposers.End.GreaterThan(DateTime.UtcNow)
					.AND(ReservationInstanceExposers.Status.NotEqual((int)ReservationStatus.Canceled));

				foreach (var dmInfo in _dmInfoPerId)
				{
					var count = (int) _rmHelper.CountReservationInstances(baseFilter.AND(ReservationInstanceExposers.HostingAgentID.Equal(dmInfo.Key)));

					var row = _currentRows[dmInfo.Key.ToString()];
					row.Cells[3].Value = count;
					_updater.UpdateRow(row);
				}
			}
			catch (Exception ex)
			{
				_logger.Error($"DebouncedRefresh failed: {ex}");
			}
		}

		public GQIPage GetNextPage(GetNextPageInputArgs args)
		{
			_logger.Debug("GetNextPage");

			try
			{
				if (_dmInfoPerId.IsNullOrEmpty())
				{
					GetAgentsInCluster();
				}

				foreach (var dmInfo in _dmInfoPerId)
				{
					var filter = ReservationInstanceExposers.End.GreaterThan(DateTime.UtcNow)
						.AND(ReservationInstanceExposers.Status.NotEqual((int) ReservationStatus.Canceled))
						.AND(ReservationInstanceExposers.HostingAgentID.Equal(dmInfo.Key));
					var bookingCount = (int) _rmHelper.CountReservationInstances(filter);

					var cells = new GQICell[]
					{
						new GQICell() {Value = dmInfo.Value.ID},
						new GQICell() {Value = dmInfo.Value.AgentName},
						new GQICell() {Value = dmInfo.Value.ConnectionState.ToString()},
						new GQICell() {Value = bookingCount}
					};

					var row = new GQIRow(dmInfo.Key.ToString(), cells);
					_currentRows[row.Key] = row;
				}

				return new GQIPage(_currentRows.Select(kv => kv.Value).OrderBy(one => one.Key).ToArray())
				{
					HasNextPage = false
				};
			}
			catch (Exception e)
			{
				_logger.Error($"Exception: {e}. {e.StackTrace}");
				return null;
			}
		}

		public void OnStopUpdates()
		{
			try
			{
				_dms.GetConnection().ClearSubscriptions(_subscriptionSetId);
				_subscriptionSetId = null;

				lock (_timerLock)
				{
					_debounceTimer?.Dispose();
					_debounceTimer = null;
				}

				_currentRows.Clear();
			}
			catch (Exception ex)
			{
				_logger.Error($"OnStopUpdates error: {ex}");
			}
		}
	}
}
