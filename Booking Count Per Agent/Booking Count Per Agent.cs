using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Skyline.DataMiner.Analytics.GenericInterface;
using Skyline.DataMiner.Net;
using Skyline.DataMiner.Net.Exceptions;
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
		private readonly Dictionary<Guid, ReservationInstance> _bookingCache = new Dictionary<Guid, ReservationInstance>();
		private readonly Dictionary<int, int> _amountOfBookingsPerAgent = new Dictionary<int, int>();
		private readonly ConcurrentDictionary<string, GQIRow> _currentRows = new ConcurrentDictionary<string, GQIRow>();
		private string _subscriptionSetId = $"DS-Booking-Count-Per-Agent-{Guid.NewGuid()}";


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
			_logger.Information("OnStartUpdates");
			_updater = updater;

			GetAgentsInCluster();
			InitializeRowsForAllAgents();

			_dms.GetConnection().OnNewMessage += HandleBookingUpdate;

			var tracker = _dms.GetConnection().TrackAddSubscription(
				_subscriptionSetId,
				new SubscriptionFilter(typeof(ResourceManagerEventMessage))
			);
			tracker.ExecuteAndWait(TimeSpan.FromMinutes(5));
			_logger.Information("Done adding subscription");
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
			_logger.Information("Incoming update");

			if (!(args.Message is ResourceManagerEventMessage rmEvent))
			{
				return;
			}

			lock (_currentRows)
			{
				foreach (var deletedBooking in rmEvent.DeletedReservationInstanceObjects)
				{
					var hostingAgentId = deletedBooking.HostingAgentID;

					_amountOfBookingsPerAgent[hostingAgentId] -= 1;
					var row = _currentRows[hostingAgentId.ToString()];
					row.Cells[3].Value = _amountOfBookingsPerAgent[hostingAgentId];

					_bookingCache.Remove(deletedBooking.ID);
				}

				foreach (var updatedBooking in rmEvent.UpdatedReservationInstances)
				{
					if (!_bookingCache.TryGetValue(updatedBooking.ID, out var oldBooking))
					{
						// Newly added booking
						// Add booking to cache
						_bookingCache.Add(updatedBooking.ID, updatedBooking);

						// Change _amountOfBookingsPerAgent
						var amountOfBookings = _amountOfBookingsPerAgent[updatedBooking.HostingAgentID];
						amountOfBookings += 1;

						// Update row of new hosting agent
						var row = _currentRows[updatedBooking.HostingAgentID.ToString()];
						row.Cells[3].Value = amountOfBookings;
						_updater.UpdateRow(row);
						continue;
					}

					// Update of booking
					// Update _amountOfBookingsPerAgent on old hosting agent (-1) and update row
					// Update _amountOfBookingsPerAgent on new hosting agent (+1) and update row
					// Update _bookingCache
					var oldHostingId = oldBooking.HostingAgentID;
					_amountOfBookingsPerAgent[oldHostingId] -= 1;
					_amountOfBookingsPerAgent[updatedBooking.HostingAgentID] += 1;
					var rowOldAgent = _currentRows[oldBooking.HostingAgentID.ToString()];
					rowOldAgent.Cells[3].Value = _amountOfBookingsPerAgent[oldHostingId];
					var rowNewAgent = _currentRows[updatedBooking.HostingAgentID.ToString()];
					rowNewAgent.Cells[3].Value = _amountOfBookingsPerAgent[updatedBooking.HostingAgentID];
					_updater.UpdateRow(rowOldAgent);
					_updater.UpdateRow(rowNewAgent);

					_bookingCache[updatedBooking.ID] = updatedBooking;
				}
			}
		}

		public GQIPage GetNextPage(GetNextPageInputArgs args)
		{
			try
			{
				_logger.Information("GetNextPage");

				var bookingsPerHostingAgents = _rmHelper
					.GetReservationInstances(new TRUEFilterElement<ReservationInstance>())
					.GroupBy(r => r.HostingAgentID)
					.ToDictionary(g => g.Key, g => g.ToList());
				foreach (var bookingsPerAgent in bookingsPerHostingAgents)
				{
					if (!_dmInfoPerId.TryGetValue(bookingsPerAgent.Key, out var dmInfo))
					{
						GetAgentsInCluster();
						dmInfo = _dmInfoPerId[bookingsPerAgent.Key];
					}

					_amountOfBookingsPerAgent.Add(dmInfo.ID, bookingsPerAgent.Value.Count);
					var cells = new GQICell[]
					{
						new GQICell() {Value = dmInfo.ID},
						new GQICell() {Value = dmInfo.AgentName},
						new GQICell() {Value = dmInfo.ConnectionState.ToString()},
						new GQICell() {Value = bookingsPerAgent.Value.Count}
					};

					var row = new GQIRow(bookingsPerAgent.Key.ToString(), cells);
					_currentRows[row.Key] = row;

					foreach (var booking in bookingsPerAgent.Value)
					{
						_bookingCache.Add(booking.ID, booking);
					}
				}

				return new GQIPage(_currentRows.Select(kv => kv.Value).ToArray())
				{
					HasNextPage = false
				};
			}
			catch (Exception e)
			{
				_logger.Information($"Exception: {e}. {e.StackTrace}");
				return null;
			}
		}

		public void OnStopUpdates()
		{
			_bookingCache.Clear();
			_currentRows.Clear();

			_dms.GetConnection().ClearSubscriptions(_subscriptionSetId);
			_subscriptionSetId = null;
		}
	}
}
