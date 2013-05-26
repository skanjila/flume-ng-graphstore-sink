package org.apache.flume.sink.neo4j;

import junit.framework.Assert;

import org.apache.flume.sink.graphstore.neo4j.Neo4jWrapper;
import org.codehaus.jackson.JsonFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.neo4j.graphdb.GraphDatabaseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:**/testContext.xml"})
public class Neo4jWrapperTest {

    private Neo4jWrapper neo4jWrapper;

    @Autowired
    private GraphDatabaseService graphDatabaseService;

    @Autowired
    JsonFactory jsonFactory;

    private String schemaFile = "/schema.json";

    @Before
    public void setup() {
        neo4jWrapper = new Neo4jWrapper(schemaFile);
    }

    @Test
    public void testParseJson() {
        neo4jWrapper.setGraphDatabaseService(graphDatabaseService);
        neo4jWrapper.setJsonFactory(jsonFactory);
        boolean result = neo4jWrapper.parseJson();
        Assert.assertTrue(result);
    }

}
