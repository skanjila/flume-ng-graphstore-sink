/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.sink.graphstore.neo4j;

import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.annotations.VisibleForTesting;
import org.elasticsearch.common.base.Throwables;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.NodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

/**
 * A sink which reads events from a channel and writes them to Neo4j
 * based on the work done by https://github.com/Aconex/elasticflume.git.</p>
 *
 * This sink supports batch reading of events from the channel and writing them
 * to Neo4j.</p>
 * Events can be of the following type:
 * 1) An event to initialize a brand new graph database
 * 2) An event to update (create or delete) one or more nodes inside a graph database
 * 3) An event to update one or more relationships inside a graph database
 * 4) An event to run a shortest path algorithm through the graph database
 * 5) An event to run a query in cipher
 * 6) An event to run a query in gremlin
 *
 * This sink must be configured with with mandatory parameters detailed in
 * {@link Neo4jSinkConstants}</p>
 * In a nutshell here is the workflow:
 * a) Check the event type
 * b) If (eventType==initialize) {
 *        read json schema and initialize graphDB using spring data
 *    } else if (eventType==updateNode) {
 *        read node information to update and perform the spring update
 *    } else if (eventType==updateRelationship) {
 *        read relationship information to update and perform the spring update
 *    }
 * b) </p>
 * @see http
 *      ://www.Neo4j.org/guide/reference/api/admin-indices-templates.
 *      html
 */
public class Neo4jSink extends AbstractSink implements Configurable {

  private static final Logger logger = LoggerFactory
      .getLogger(Neo4jSink.class);

  static final FastDateFormat df = FastDateFormat.getInstance("yyyy-MM-dd");

  // Used for testing
  private boolean isLocal = false;
  private final CounterGroup counterGroup = new CounterGroup();

  private static final int defaultBatchSize = 100;

  private int batchSize = defaultBatchSize;
  private long ttlMs = Neo4jSinkConstants.DEFAULT_TTL;
  private String clusterName = Neo4jSinkConstants.DEFAULT_CLUSTER_NAME;
  private String indexName = Neo4jSinkConstants.DEFAULT_INDEX_NAME;
  private String indexType = Neo4jSinkConstants.DEFAULT_INDEX_TYPE;

  private InetSocketTransportAddress[] serverAddresses;

  private Node node;
  private Client client;
  private Neo4jEventSerializer serializer;
  private SinkCounter sinkCounter;

  /**
   * Create an {@link Neo4jSink} configured using the supplied
   * configuration
   */
  public Neo4jSink() {
    this(false);
  }

  /**
   * Create an {@link Neo4jSink}</p>
   *
   * @param isLocal
   *          If <tt>true</tt> sink will be configured to only talk to an
   *          Neo4j instance hosted in the same JVM, should always be
   *          false is production
   *
   */
  @VisibleForTesting
  Neo4jSink(boolean isLocal) {
    this.isLocal = isLocal;
  }

  @VisibleForTesting
  InetSocketTransportAddress[] getServerAddresses() {
    return serverAddresses;
  }

  @VisibleForTesting
  String getClusterName() {
    return clusterName;
  }

  @VisibleForTesting
  String getIndexName() {
    return indexName + "-" + df.format(new Date());
  }

  @VisibleForTesting
  String getIndexType() {
    return indexType;
  }

  @VisibleForTesting
  long getTTLMs() {
    return ttlMs;
  }

  @Override
  public Status process() throws EventDeliveryException {
    logger.debug("processing...");
    Status status = Status.READY;
    Channel channel = getChannel();
    Transaction txn = channel.getTransaction();
    try {
      txn.begin();
      String indexName = getIndexName();
      BulkRequestBuilder bulkRequest = client.prepareBulk();
      for (int i = 0; i < batchSize; i++) {
        Event event = channel.take();

        if (event == null) {
          break;
        }

        XContentBuilder builder = serializer.getContentBuilder(event);
        IndexRequestBuilder request = client.prepareIndex(indexName, indexType)
            .setSource(builder);

        if (ttlMs > 0) {
          request.setTTL(ttlMs);
        }

        bulkRequest.add(request);
      }

      int size = bulkRequest.numberOfActions();
      if (size <= 0) {
        sinkCounter.incrementBatchEmptyCount();
        counterGroup.incrementAndGet("channel.underflow");
        status = Status.BACKOFF;
      } else {
        if (size < batchSize) {
          sinkCounter.incrementBatchUnderflowCount();
          status = Status.BACKOFF;
        } else {
          sinkCounter.incrementBatchCompleteCount();
        }

        sinkCounter.addToEventDrainAttemptCount(size);

        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        if (bulkResponse.hasFailures()) {
          throw new EventDeliveryException(bulkResponse.buildFailureMessage());
        }
      }
      txn.commit();
      sinkCounter.addToEventDrainSuccessCount(size);
      counterGroup.incrementAndGet("transaction.success");
    } catch (Throwable ex) {
      try {
        txn.rollback();
        counterGroup.incrementAndGet("transaction.rollback");
      } catch (Exception ex2) {
        logger.error(
            "Exception in rollback. Rollback might not have been successful.",
            ex2);
      }

      if (ex instanceof Error || ex instanceof RuntimeException) {
        logger.error("Failed to commit transaction. Transaction rolled back.",
            ex);
        Throwables.propagate(ex);
      } else {
        logger.error("Failed to commit transaction. Transaction rolled back.",
            ex);
        throw new EventDeliveryException(
            "Failed to commit transaction. Transaction rolled back.", ex);
      }
    } finally {
      txn.close();
    }
    return status;
  }

  @Override
  public void configure(Context context) {
    if (!isLocal) {
      String[] hostNames = null;
      if (StringUtils.isNotBlank(context.getString(Neo4jSinkConstants.HOSTNAMES))) {
        hostNames = context.getString(Neo4jSinkConstants.HOSTNAMES).split(",");
      }
      Preconditions.checkState(hostNames != null && hostNames.length > 0,
          "Missing Param:" + Neo4jSinkConstants.HOSTNAMES);

      serverAddresses = new InetSocketTransportAddress[hostNames.length];
      for (int i = 0; i < hostNames.length; i++) {
        String[] hostPort = hostNames[i].split(":");
        String host = hostPort[0];
        int port = hostPort.length == 2 ? Integer.parseInt(hostPort[1])
            : Neo4jSinkConstants.DEFAULT_PORT;
        serverAddresses[i] = new InetSocketTransportAddress(host, port);
      }

      Preconditions.checkState(serverAddresses != null
          && serverAddresses.length > 0, "Missing Param:" + Neo4jSinkConstants.HOSTNAMES);
    }

    if (StringUtils.isNotBlank(context.getString(Neo4jSinkConstants.INDEX_NAME))) {
      this.indexName = context.getString(Neo4jSinkConstants.INDEX_NAME);
    }

    if (StringUtils.isNotBlank(context.getString(Neo4jSinkConstants.INDEX_TYPE))) {
      this.indexType = context.getString(Neo4jSinkConstants.INDEX_TYPE);
    }

    if (StringUtils.isNotBlank(context.getString(Neo4jSinkConstants.CLUSTER_NAME))) {
      this.clusterName = context.getString(Neo4jSinkConstants.CLUSTER_NAME);
    }

    if (StringUtils.isNotBlank(context.getString(Neo4jSinkConstants.BATCH_SIZE))) {
      this.batchSize = Integer.parseInt(context.getString(Neo4jSinkConstants.BATCH_SIZE));
    }

    if (StringUtils.isNotBlank(context.getString(Neo4jSinkConstants.TTL))) {
      this.ttlMs = TimeUnit.DAYS.toMillis(Integer.parseInt(context
          .getString(TTL)));
      Preconditions.checkState(ttlMs > 0, Neo4jSinkConstants.TTL
          + " must be greater than 0 or not set.");
    }

    String serializerClazz = "org.apache.flume.sink.graphstore.neo4j.Neo4jLogStashEventSerializer";
    if (StringUtils.isNotBlank(context.getString(Neo4jSinkConstants.SERIALIZER))) {
      serializerClazz = context.getString(Neo4jSinkConstants.SERIALIZER);
    }

    Context serializerContext = new Context();
    serializerContext.putAll(context.getSubProperties(Neo4jSinkConstants.SERIALIZER_PREFIX));

    try {
      @SuppressWarnings("unchecked")
      Class<? extends Neo4jEventSerializer> clazz = (Class<? extends Neo4jEventSerializer>) Class
          .forName(serializerClazz);
      serializer = clazz.newInstance();
      serializer.configure(serializerContext);
    } catch (Exception e) {
      logger.error("Could not instantiate event serializer.", e);
      Throwables.propagate(e);
    }

    if (sinkCounter == null) {
      sinkCounter = new SinkCounter(getName());
    }

    Preconditions.checkState(StringUtils.isNotBlank(indexName),
        "Missing Param:" + Neo4jSinkConstants.INDEX_NAME);
    Preconditions.checkState(StringUtils.isNotBlank(indexType),
        "Missing Param:" + Neo4jSinkConstants.INDEX_TYPE);
    Preconditions.checkState(StringUtils.isNotBlank(clusterName),
        "Missing Param:" + Neo4jSinkConstants.CLUSTER_NAME);
    Preconditions.checkState(batchSize >= 1, Neo4jSinkConstants.BATCH_SIZE
        + " must be greater than 0");
  }

  @Override
  public void start() {
    logger.info("Neo4j sink {} started");
    sinkCounter.start();
    try {
      openConnection();
    } catch (Exception ex) {
      sinkCounter.incrementConnectionFailedCount();
      closeConnection();
    }

    super.start();
  }

  @Override
  public void stop() {
    logger.info("Neo4j sink {} stopping");
    closeConnection();

    sinkCounter.stop();
    super.stop();
  }

  private void openConnection() {
    if (isLocal) {
      logger.info("Using Neo4j AutoDiscovery mode");
      openLocalDiscoveryClient();
    } else {
      logger.info("Using Neo4j hostnames: {} ",
          Arrays.toString(serverAddresses));
      openClient();
    }
    sinkCounter.incrementConnectionCreatedCount();
  }

  /*
   * FOR TESTING ONLY...
   *
   * Opens a local discovery node for talking to an Neo4j server running
   * in the same JVM
   */
  private void openLocalDiscoveryClient() {
    node = NodeBuilder.nodeBuilder().client(true).local(true).node();
    client = node.client();
  }

  private void openClient() {
    Settings settings = ImmutableSettings.settingsBuilder()
        .put("cluster.name", clusterName).build();

    TransportClient transport = new TransportClient(settings);
    for (InetSocketTransportAddress host : serverAddresses) {
      transport.addTransportAddress(host);
    }
    client = transport;
  }

  private void closeConnection() {
    if (client != null) {
      client.close();
    }
    client = null;

    if (node != null) {
      node.close();
    }
    node = null;

    sinkCounter.incrementConnectionClosedCount();
  }
}
