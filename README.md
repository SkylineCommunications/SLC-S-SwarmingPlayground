# Swarming Playground

Demo solution to play around with [Swarming](https://aka.dataminer.services/swarming).

Requires a 10.5.1+ DMS with Swarming enabled.

Focusses on the main user stories of Swarming:

- As a DataMiner System Administrator, I want to be able to perform maintenance tasks, such as installing Windows updates, on my live cluster, one agent at a time, when there is sufficient spare capacity available.*
- As a DataMiner System Administrator, I want the ability to seamlessly add an additional node to my system and effortlessly transfer functionalities from existing agents to the new agents. This allows me to effectively rebalance my cluster for optimal performance.*
- As a DataMiner System, I need the ability to automatically recover from failing nodes by redistributing activities hosted on the failed node to the remaining nodes in the cluster.* **

\* with limited downtime and as long as there is spare capacity

** The Swarming feature provides flexibility in choosing a redistribution strategy, allowing you to implement your preferred approach through its API. Note that automatic recovery is not built into the core of DataMiner; however, various examples will be available in the Catalog to guide and inspire you.
