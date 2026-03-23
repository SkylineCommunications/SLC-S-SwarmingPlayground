using Skyline.DataMiner.Net.ResourceManager.Objects;

namespace LoadBalanceTests
{
    using System.Collections.Generic;
    using System.Linq;
    using Moq;
    using NUnit.Framework;
    using Skyline.DataMiner.Automation;
    using Skyline.DataMiner.Net.Messages;
    using Swarming_Playground_Shared;

    public class SwarmAwayObjectsTests
    {
        [TestCase]
        public void RedistributeBookingsAwayTest()
        {
	        var bookings = new List<ReservationInstance>()
	        {
		        CreateReservationInstance(0, 1),
		        CreateReservationInstance(1, 1),
		        CreateReservationInstance(2, 2),
		        CreateReservationInstance(3, 2),
		        CreateReservationInstance(4, 3),
		        CreateReservationInstance(5, 3),
	        };

	        var config = new ClusterConfig(Mock.Of<IEngine>(), Agents(1, 2, 3));
			config.InitializeAgentToBookings(bookings.ToArray());
			config.RedistributeBookingsAwayFromAgents(new []{1}, booking => booking.Status != ReservationStatus.Ongoing);

			Assert.That(config.CurrentConfigBookings.Count.Equals(2));
			Assert.That(config.CurrentConfigBookings.Keys.ToList().FirstOrDefault(one => one.ID == 2) != null);
			Assert.That(config.CurrentConfigBookings.Keys.ToList().FirstOrDefault(one => one.ID == 3) != null);

			var agentTwoBookings = config.CurrentConfigBookings.First(one => one.Key.ID == 2);
			var agentThreeBookings = config.CurrentConfigBookings.First(one => one.Key.ID == 3);
			Assert.That(agentTwoBookings.Value.Count == 3);
			Assert.That(agentThreeBookings.Value.Count == 3);
			Assert.That(agentTwoBookings.Value.Count(one => one.HostingAgentID != agentTwoBookings.Key.ID) == 1);
			Assert.That(agentThreeBookings.Value.Count(one => one.HostingAgentID != agentThreeBookings.Key.ID) == 1);
        }

        private ReservationInstance CreateReservationInstance(int name, int hostingAgentId)
        {
	        return new ReservationInstance()
	        {
		        Name = $"{name}",
		        HostingAgentID = hostingAgentId,
		        Status = ReservationStatus.Confirmed
	        };
        }

		private static GetDataMinerInfoResponseMessage[] Agents(params int[] ids)
            => ids.Select(id => new GetDataMinerInfoResponseMessage() { ID = id }).ToArray();
    }
}