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

namespace SwarmableBookings
{
	/// <summary>
	/// Shows the bookings per agent
	/// </summary>
	[GQIMetaData(Name = "Swarmable Bookings")]
	public sealed class SwarmableBookings : IGQIDataSource, IGQIOnInit, IGQIUpdateable
	{
		private GQIDMS _dms;
		private IGQILogger _logger;
		private ResourceManagerHelper _rmHelper;
		private IGQIUpdater _updater;
		private Dictionary<int, GetDataMinerInfoResponseMessage> _dmInfoPerId = new Dictionary<int, GetDataMinerInfoResponseMessage>();
		private readonly ConcurrentDictionary<string, GQIRow> _currentRows = new ConcurrentDictionary<string, GQIRow>();
		private string _subscriptionSetId = $"DS-Swarmable-Bookings-{Guid.NewGuid()}";

		public GQIColumn[] GetColumns()
		{
			return new GQIColumn[]
			{
				new GQIStringColumn("Booking ID"),
				new GQIStringColumn("Booking name"),
				new GQIDateTimeColumn("Start time"),
				new GQIDateTimeColumn("End time"),
				new GQIStringColumn("Status"),
				new GQIIntColumn("Hosting agent ID"),
				new GQIStringColumn("Hosting agent")
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
			_dms.GetConnection().OnNewMessage += HandleBookingUpdate;

			var tracker = _dms.GetConnection().TrackAddSubscription(
				_subscriptionSetId,
				new SubscriptionFilter(typeof(ResourceManagerEventMessage))
			);
			tracker.ExecuteAndWait(TimeSpan.FromMinutes(5));
			_logger.Debug("Done adding subscription");
		}

		public GQIPage GetNextPage(GetNextPageInputArgs args)
		{
			_logger.Debug("GetNextPage");
			var bookings = _rmHelper.GetReservationInstances(new TRUEFilterElement<ReservationInstance>());
			return new GQIPage(bookings.Select(SelectRow).ToArray());
		}

		public void OnStopUpdates()
		{
			_logger.Debug("OnStopUpdates");
			_dms.GetConnection().ClearSubscriptions(_subscriptionSetId);
			_subscriptionSetId = null;

			_currentRows.Clear();
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
				throw new DataMinerException($"Issue in {nameof(SwarmableBookings)} while getting DataMiner info. {ex}", ex);
			}

			if (resp == null || resp.Length == 0)
				throw new InvalidOperationException("No DataMiner info returned.");

			_dmInfoPerId = resp.OfType<GetDataMinerInfoResponseMessage>().ToDictionary(info => info.ID);
		}

		private void HandleBookingUpdate(object sender, NewMessageEventArgs args)
		{
			_logger.Debug("Incoming updates");

			if (!(args.Message is ResourceManagerEventMessage rmEvent))
			{
				return;
			}

			foreach (var oneBooking in rmEvent.UpdatedReservationInstances)
			{
				if (_currentRows.ContainsKey(oneBooking.ID.ToString()))
				{
					_logger.Debug($"Updating row for booking with ID '{oneBooking.ID}'");
					_updater.UpdateRow(SelectRow(oneBooking));
				}
				else
				{
					if (oneBooking.Start <= DateTime.Now)
						continue;

					_logger.Debug($"Adding row for booking with ID '{oneBooking.ID}'");
					_updater.AddRow(SelectRow(oneBooking));
				}
			}

			foreach (var oneBooking in rmEvent.DeletedReservationInstances)
			{
				_logger.Debug($"Removing row for booking with ID '{oneBooking}'");
				_updater.RemoveRow(oneBooking.ToString());
			}
		}

		private GQIRow SelectRow(ReservationInstance booking)
		{
			if (!_dmInfoPerId.TryGetValue(booking.HostingAgentID, out var dmaInfo))
			{
				GetAgentsInCluster();
				_dmInfoPerId.TryGetValue(booking.HostingAgentID, out dmaInfo);
			}
			
			var cells = new GQICell[]
			{
				new GQICell() { Value = booking.ID.ToString() },
				new GQICell() { Value = booking.Name },
				new GQICell() { Value = booking.Start },
				new GQICell() { Value = booking.End },
				new GQICell() { Value = booking.Status.ToString() },
				new GQICell() { Value = booking.HostingAgentID },
				new GQICell() { Value = dmaInfo?.AgentName ?? "Unknown" }
			};

			var row = new GQIRow(booking.ID.ToString(), cells);
			_currentRows.AddOrUpdate(row.Key, row, (_, __) => row);
			return row;
		}
	}
}
