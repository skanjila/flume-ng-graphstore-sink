/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.flume.sink.graphstore.neo4j;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.JsonMappingException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * A class responsible for abstracting away neo4j, specifically this class will
 * have a handle to the graphstore and will be responsible for: 1)
 * instantiating/creating the graphstore from scratch 2) CRUD operations on
 * nodes 3) CRUD operations on relationships 4) running cipher queries through
 * the database 5) running shortest path algorithms through the database
 * 
 * 
 */
public class Neo4jWrapper {

    private static final Logger logger = LoggerFactory.getLogger(Neo4jWrapper.class);

    @Autowired
    GraphDatabaseService graphDatabaseService;

    @Autowired
    JsonFactory jsonFactory;

    String schemaJsonFile;

    public Neo4jWrapper(String schemaJsonFile) {
        this.schemaJsonFile = schemaJsonFile;
    }

    /**
     * Given a chunk of json parse it using Jackson's streaming API and store
     * the nodes and relationships into appropriate objects
     * 
     * @return true or false based on whether the json was parsed correctly
     */
    public boolean parseJson() {
        try {
            File test = FileUtils.toFile(this.getClass().getResource(this.schemaJsonFile));
            JsonParser jParser = jsonFactory.createJsonParser(test);
            // loop until token equal to "}"
            String fieldName = "";
            while (jParser.nextToken() != JsonToken.END_OBJECT) {
                fieldName = jParser.getCurrentName();
                logger.debug("The field name=" + fieldName + " and value=" + jParser.getText());
            }
            jParser.close();

        } catch (JsonGenerationException e) {
            logger.debug("Neo4jWrapper::parseJson caught a JsonGenerationException with message=" + e.getMessage());
        } catch (JsonMappingException e) {
            logger.debug("Neo4jWrapper::parseJson caught a JsonMappingException with message=" + e.getMessage());
        } catch (IOException e) {
            logger.debug("Neo4jWrapper::parseJson caught a IOException with message=" + e.getMessage());
        }
        return true;
    }

    public void setGraphDatabaseService(GraphDatabaseService graphDatabaseService) {
        this.graphDatabaseService = graphDatabaseService;
    }

    public void setJsonFactory(JsonFactory jsonFactory) {
        this.jsonFactory = jsonFactory;
    }
}
