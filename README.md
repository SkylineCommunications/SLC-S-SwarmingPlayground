# Swarming Playground

Demo solution to play around with [Swarming](https://aka.dataminer.services/swarming).

Requires a 10.5.1+ DMS with Swarming enabled.

Focuses on the main user stories of Swarming:

- As a DataMiner System Admin, you can apply maintenance (e.g. Windows updates) on a live cluster, Agent by Agent, by temporarily moving functionalities away to other Agents in the cluster.

- As a DataMiner System Admin, you can easily extend your system with an extra node and move functionalities from existing nodes to new nodes, so you can rebalance your cluster.

- Swarming makes it possible to recover functionalities from failing nodes by moving activities hosted on such a node to the remaining nodes.

> [!NOTE]
> The above capabilities are possible with limited downtime and as long as there is spare capacity.

The Swarming feature provides flexibility in choosing a redistribution strategy, allowing you to implement your preferred approach through its API. Note that automatic recovery is not built into the core of DataMiner; however, various examples will be available in the Catalog to guide and inspire you.
