package net.jmp.demo.mongodb.atlas;

/*
 * (#)Aggregation.java  0.10.0  12/09/2023
 *
 * Copyright (c) Jonathan M. Parker
 * All Rights Reserved.
 *
 * @author    Jonathan Parker
 * @version   0.10.0
 * @since     0.10.0
 */

import com.mongodb.MongoBulkWriteException;

import com.mongodb.client.MongoClient;

import com.mongodb.client.model.*;

import java.util.*;

import org.bson.Document;

import org.slf4j.LoggerFactory;

import org.slf4j.ext.XLogger;

final class Aggregation {
    private final XLogger logger = new XLogger(LoggerFactory.getLogger(this.getClass().getName()));
    private final MongoClient mongoClient;
    private final String dbName;
    private final String collectionName;

    Aggregation(final Properties properties, final MongoClient mongoClient) {
        super();

        this.mongoClient = mongoClient;

        this.dbName = properties.getProperty("mongodb.aggregation.db", "training");
        this.collectionName = properties.getProperty("mongodb.aggregation.collection", "restaurants");
    }

    void run() {
        this.logger.entry();
        this.logger.info("Beginning aggregation operations...");

        this.logger.info("Ending aggregation operations...");
        this.logger.exit();
    }
}
