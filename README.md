flume-ng-graphstore-sink
========================

A flume sink that writes to a set of graph databases, the initial efforts will target neo4j and titan and will build an abstraction layer around both of these databases
This sink will be an abstraction for reading a generic set of graphstore events that include:
1) Creating an empty new graphstore
2) Populating all the nodes and relationships of the graphstore
3) Updating some of the nodes and relationships of the graphstore
4) Running a query using the local declarative language for the graphstore if there is one available (for example for neo4j there is cipher)
5) Running a query to find the shortest path inside the graph database using either the A* or the dijkstra algorithms
6) Running a query to find the shortest paths inside the graph database with constraints
7) Running a graphsearch to find one or more nodes or relationships

More to come as this effort moves forward
