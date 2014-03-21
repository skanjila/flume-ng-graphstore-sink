package org.neo4j.flume;

import static org.neo4j.common.xcontent.XContentFactory.jsonBuilder;
import static org.neo4j.node.NodeBuilder.nodeBuilder;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.util.Pair;
import org.neo4j.client.Client;
import org.neo4j.client.transport.TransportClient;
import org.neo4j.cluster.ClusterName;
import org.neo4j.common.settings.ImmutableSettings;
import org.neo4j.common.settings.Settings;
import org.neo4j.common.transport.InetSocketTransportAddress;
import org.neo4j.common.xcontent.XContentBuilder;
import org.neo4j.common.xcontent.XContentFactory;
import org.neo4j.common.xcontent.XContentParser;
import org.neo4j.common.xcontent.XContentType;
import org.neo4j.node.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Neo4jSink extends EventSink.Base {

    private static final Logger LOG = LoggerFactory.getLogger(Neo4jSink.class);
    private static final String DEFAULT_INDEX_NAME = "flume";
    private static final String DEFAULT_LOG_TYPE = "log";
    private static final int DEFAULT_ELASTICSEARCH_PORT = 9300;

    private Node node;
    private Client client;
    private String indexName = DEFAULT_INDEX_NAME;
    private String indexPattern = null;
    private String indexType = DEFAULT_LOG_TYPE;

    private Charset charset = Charset.defaultCharset();

    private String[] hostNames = new String[0];
    private String clusterName = ClusterName.DEFAULT.value();

    // Enabled only for testing
    private boolean localOnly = false;

    private AtomicLong eventErrorCount = new AtomicLong(0L);
    private static final String NO_OF_FAILED_EVENTS = "NO_OF_FAILED_EVENTS";

    @Override
    public void append(Event e) throws IOException {
        try {
            XContentBuilder builder = jsonBuilder()
                    .startObject()
                    .field("timestamp", new Date(e.getTimestamp()))
                    .field("host", e.getHost())
                    .field("priority", e.getPriority().name());

            addBody(builder, e.getBody());

            addAttrs(builder, e.getAttrs());

            index(e, builder);
        } catch (Exception ex) {
            LOG.error("Error Processing event: {}", e.toString(), ex);
            eventErrorCount.incrementAndGet();
        }
    }

    @Override
    public synchronized ReportEvent getMetrics() {
        ReportEvent event = new ReportEvent("Neo4jSink");
        event.setLongMetric(NO_OF_FAILED_EVENTS, eventErrorCount.longValue());
        return event;
    }

    private void addBody(XContentBuilder builder, byte[] data) throws IOException {
        XContentType contentType = XContentFactory.xContentType(data);

        if (contentType == null) {
            builder.startObject("message");
            addSimpleField(builder, "text", data);
            builder.endObject();
        } else {
            addComplexField(builder, "message", contentType, data);
        }
    }

    private void addAttrs(XContentBuilder builder, Map<String, byte[]> attrs) throws IOException {
        builder.startObject("fields");
        for (Map.Entry<String, byte[]> entry : attrs.entrySet()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("field: {}, data: {}", entry.getKey(), new String(entry.getValue()));
            }
            addField(builder, entry.getKey(), entry.getValue());
        }
        builder.endObject();
    }

    private void addField(XContentBuilder builder, String fieldName, byte[] data) throws IOException {
        XContentType contentType = XContentFactory.xContentType(data);
        if (contentType == null) {
            addSimpleField(builder, fieldName, data);
        } else {
            addComplexField(builder, fieldName, contentType, data);
        }
    }

    private void addSimpleField(XContentBuilder builder, String fieldName, byte[] data) throws IOException {
        builder.field(fieldName, new String(data, charset));
    }

    private void addComplexField(XContentBuilder builder, String fieldName, XContentType contentType, byte[] data)
            throws IOException {
        XContentParser parser = null;
        try {
            parser = XContentFactory.xContent(contentType).createParser(data);
            parser.nextToken();
            builder.field(fieldName).copyCurrentStructure(parser);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }

    private void index(Event e, XContentBuilder builder) {
        String iName = indexName;
        if (indexPattern != null) {
            iName = e.escapeString(indexPattern);
        }
        client.prepareIndex(iName, indexType, null)
                .setSource(builder)
                .execute()
                .actionGet();

        if (!iName.equals(indexName)) {
            client.admin().indices().prepareAliases().addAlias(iName, indexName).execute().actionGet();
        }
    }

    @Override
    public void close() throws IOException, InterruptedException {
        super.close();

        if (client != null) {
            client.close();
        }
        if (node != null) {
            node.close();
        }
    }

    @Override
    public void open() throws IOException, InterruptedException {
        super.open();

        if (hostNames.length == 0) {
            LOG.info("Using neo4j AutoDiscovery mode");
            node = nodeBuilder().client(true).clusterName(clusterName).local(localOnly).node();
            client = node.client();
        } else {
            LOG.info("Using provided ES hostnames: {} ", Arrays.toString(hostNames));

            Settings settings = ImmutableSettings.settingsBuilder()
                    .put("cluster.name", clusterName)
                    .build();

            TransportClient transportClient = new TransportClient(settings);
            for (String esHostName : hostNames) {
                LOG.info("Adding TransportClient: {}", esHostName);
                transportClient = transportClient.addTransportAddress(new InetSocketTransportAddress(esHostName,
                        DEFAULT_ELASTICSEARCH_PORT));
            }
            client = transportClient;
        }

    }

    /**
     * This is a special function used by the SourceFactory to pull in this class as a plugin sink.
     */
    public static List<Pair<String, SinkBuilder>> getSinkBuilders() {
        List<Pair<String, SinkBuilder>> builders =
                new ArrayList<Pair<String, SinkBuilder>>();

        builders.add(new Pair<String, SinkBuilder>("neo4jSink", new Neo4jSinkBuilder()));
        return builders;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public String getIndexPattern() {
        return indexPattern;
    }

    public void setIndexPattern(String indexPattern) {
        this.indexPattern = indexPattern;
    }

    public String getIndexType() {
        return indexType;
    }

    public void setIndexType(String indexType) {
        this.indexType = indexType;
    }

    public void setHostNames(String[] hostNames) {
        this.hostNames = hostNames;
    }

    public String[] getHostNames() {
        return hostNames;
    }

    void setLocalOnly(boolean localOnly) {
        this.localOnly = localOnly;
    }

    boolean isLocalOnly() {
        return localOnly;
    }
}
