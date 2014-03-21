Using ElasticSearch Flume integration
=====================================

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

Pre-Conditions:
---------------
* have Flume installed, or at least cloned from the Flume git repo,
    if not, go here http://github.com/cloudera/flume , and build it (currently using 'ant', but follow their docs).

    From here on, this Flume directory will be referred to as FLUME_HOME

* Have either neo4j or titan installed locally, we'll assume that from a Getting Started point of view you have a local
  Neo4j server running locally, if not go here for neo4j http://www.neo4j.org/download or here for titan http://thinkaurelius.github.io/titan/

  The best way to do this is just download the binary, unpack it and then:

    bin/neo4j

  See the installation guide for ElasticSearch here: http://www.elasticsearch.com/docs/elasticsearch/setup/installation/


Getting Started with elasticflume
---------------------------------
0. First, setup some environment variables to your local paths, to make the following steps simpler:
    export FLUME_HOME=<path to where you have Flume checkedout/installed>
    export ELASTICSEARCH_HOME=<path to where you have ElasticSearch checked out>

    export ELASTICFLUME_HOME=path to where you have elasticflume checked out>

    *(Be careful with these last 2 env vars because they are deceivingly similar)*

1. Build it using Maven:

    Flume is now available via the Cloudera Maven repository, which is already part of this pom, however if you are
    using a local Maven Repository Manager (Nexus, Artifactory) you will need to add the following URLs to your list of
    proxies:

        OSS Sonatype: http://oss.sonatype.org/content/groups/public
        Cloudera: https://repository.cloudera.com/content/groups/public

    If you're not sure if you do or not, you probably don't, so skip this.

    Don't forget to mark this to proxy both Releases AND Snapshots (in Nexus, you should configure 2 separate Proxies to host the releases and snapshots respectively)

    1.3 Build elasticflume
    cd $ELASTICFLUME_HOME
    mvn package

    If Maven complains that it cannot locate an artifact, check 1.2 above to make sure you got it correct.  Contact me if you have problems.

2. Now add the elasticflume jar into the classpath too, I do this personally with a symlink for testing, but copying is probably a better idea.. :):

    ln -s $ELASTICFLUME_HOME/target/elasticflume-1.0.0-SNAPSHOT-jar-with-dependencies.jar $FLUME_HOME/lib/

    **NOTE**: On Ubuntu, with certain JVM's installed, there have been reports that using the Symlink approach does not work because the JVM will refuse to load a jar coming from a symlink, so if you get a ClassNotFoundException, perhaps try copying instead.

3. Ensure your Flume config is correct, check the $FLUME_HOME/conf/flume-conf.xml correctly identifies your local master, you
    may have to copy the template file that's in that directory to be 'flume-conf.xml' and then add the following:

      <property>
        <name>flume.master.servers</name>
        <value>localhost</value>
        <description>A comma-separated list of hostnames, one for each
          machine in the Flume Master.
        </description>
      </property>

  ... (the above may not be necessary, because it's the default, but I had to do it for some reason).

  You will also need to register the elasticflume plugin via creating a new a property block:

      <property>
          <name>flume.plugin.classes</name>
          <value>org.elasticsearch.flume.ElasticSearchSink</value>
          <description>Comma separated list of plugins</description>
      </property>


4. Startup Flume Master, and Flume nodes, you will need 2 different shells here.

    Shell #1:
        cd $FLUME_HOME
        bin/flume master

    Shell #2:
        cd $FLUME_HOME
        bin/flume node_nowatch

        VERIFY that you see in the startup log for the master the following log line, if you don't see this, you've missed at least Step 3:

        2010-11-20 13:15:03,556 [main] INFO conf.SinkFactoryImpl: Found sink builder elasticSearchSink in org.elasticsearch.flume.ElasticSearchSink

    **NOTE**: If you get other errors like "org.elasticsearch.discovery.MasterNotDiscoveredException", please ensure you have a running ElasticSearch instance locally
    or somewhere on your local network, and have Multicast enabled on the relevant network interfaces.  elasticflume will try to connect to the default
    named ElasticSearch cluster it can detect via multicast auto-discovery (see Pre-conditions section above).   


5. From yet another shell, setup a basic console based source so you can type in data manually and have it indexed (pretending to be a log message)
    cd $FLUME_HOME
    bin/flume shell -c localhost -e "exec config localhost 'console' 'elasticSearchSink'"

    NOTE: For some reason my local testing Flume installaton used a default node name of my IP address, and not
        'localhost' which it is often.  If things are not working properly, you should check by:

        bin/flume shell -c localhost -e "getnodestatus"

       If you see a node listed using an IP address, then you may need to then map that to localhost inside flume with
       a logical name by doing this:

       bin/flume shell -c localhost -e "exec map <IP ADDRESS> localhost"


6. NOW FOR THE TEST! :)  In the console window you started the "node_nowatch" above,
   type (and yes, straight after all those log messages, just start typing, trust me..):

    hello world

    hello there good sir

    (ie. that is, type the 2 lines ensuring you press return after each)

7. Verify you can search for your "Hello World" log, in another console, use curl to search your local elasticsearch node:


    curl -XGET 'http://localhost:9200/flume/_search?pretty=true' -d '
    {
        "query" : {
            "term" : { "text" : "hello" }
        }
    }
    '

    You should get a pretty printed JSON formatted search results, something like:


    {
    "_shards" : {
    "total" : 5,
    "successful" : 5,
    "failed" : 0
    },
    "hits" : {
    "total" : 2,
    "max_score" : 1.1976817,
    "hits" : [ {
      "_index" : "flume",
      "_type" : "LOG",
      "_id" : "4e5a6f5b-1dd3-4bb6-9fd9-c8d785f39680",
      "_score" : 1.1976817, "_source" : {"message":"hello world","timestamp":"2010-09-14T03:19:36.857Z","host":"192.168.1.170","priority":"INFO"}
    }, {
      "_index" : "flume",
      "_type" : "LOG",
      "_id" : "c77c18cc-af40-4362-b20b-193e5a3f6ff5",
      "_score" : 0.8465736, "_source" : {"message":"hello there good sir","timestamp":"2010-09-14T03:28:04.168Z","host":"192.168.1.170","priority":"INFO"}
    } ]
    }
    }



8. So now you have some basic setup to stream logs via Flume into ElasticSearch.  To search the logs, you can use [Mobz elasticsearch-head project](https://github.com/mobz/elasticsearch-head) this will allow you to do searching across the index you create via elasticflume (or any other method).

9. A more complete discussion of how to setup a log4j-based system to stream via flume to elasticsearch can be found in the same directory as this README (log4j-to-elasticsearch).

TODO
====
# Create a standard Mapping script/example that sets up the mappings properly in the index in ES, right now everything is using the defaults in ES which is not optimal in cases like Host and Priority in some cases.

# elasticflume currently submits a single event via HTTP (async) to ES.  This isn't that scalable when the firehose of
events is coming in.  Need to have another Sink that is a RabbitMQToESSink that uses the ES river batching logic (pretty simple)

