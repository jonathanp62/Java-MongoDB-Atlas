package net.jmp.demo.mongodb.atlas;

/*
 * (#)Upsert.java   0.8.0   12/06/2023
 *
 * Copyright (c) Jonathan M. Parker
 * All Rights Reserved.
 *
 * @author    Jonathan Parker
 * @version   0.8.0
 * @since     0.8.0
 */

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;

import com.mongodb.client.MongoClient;

import com.mongodb.client.model.*;

import java.util.*;

import org.bson.Document;

import org.slf4j.LoggerFactory;

import org.slf4j.ext.XLogger;

final class Upsert {
    private final XLogger logger = new XLogger(LoggerFactory.getLogger(this.getClass().getName()));
    private final MongoClient mongoClient;
    private final String dbName;
    private final String collectionName;

    Upsert(final Properties properties, final MongoClient mongoClient) {
        super();

        this.mongoClient = mongoClient;

        this.dbName = properties.getProperty("mongodb.upsert.db", "training");
        this.collectionName = properties.getProperty("mongodb.upsert.collection", "colors");
    }

    void run() {
        this.logger.entry();
        this.logger.info("Beginning upsert operations...");

        this.insertData();

        Helpers.printAllDocuments(this.mongoClient,
                this.dbName,
                this.collectionName,
                this.logger);

        this.upsertThatInserts();

        Helpers.printAllDocuments(this.mongoClient,
                this.dbName,
                this.collectionName,
                this.logger);

        this.upsertThatUpdates();

        Helpers.printAllDocuments(this.mongoClient,
                this.dbName,
                this.collectionName,
                this.logger);

        this.deleteData();

        this.logger.info("Ending upsert operations...");
        this.logger.exit();
    }

    private void insertData() {
        this.logger.entry();

        final var color = "color";
        final var quantity = "quantity";
        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);

        final var documents = List.of(
                new Document(color, "red").append(quantity, 5),
                new Document(color, "purple").append(quantity, 8),
                new Document(color, "blue").append(quantity, 0),
                new Document(color, "white").append(quantity, 0),
                new Document(color, "yellow").append(quantity, 6),
                new Document(color, "pink").append(quantity, 0),
                new Document(color, "green").append(quantity, 0),
                new Document(color, "black").append(quantity, 8)
        );

        try {
            final var result = collection.insertMany(documents);

            result.getInsertedIds().values()
                    .forEach(id -> this.logger.info("Inserted document: {}", id.asObjectId().getValue()));
        } catch (final MongoBulkWriteException mbwe) {
            this.logger.catching(mbwe);

            mbwe.getWriteResult().getInserts()
                    .forEach(doc -> this.logger.info("Inserted document: {}", doc.getId().asObjectId().getValue()));
        }

        this.logger.exit();
    }

    private void upsertThatInserts() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.eq("color", "orange");
        final var update = Updates.inc("quantity", 10);
        final var options = new UpdateOptions().upsert(true);

        try {
            final var result = collection.updateOne(filter, update, options);

            this.logger.info("{}", result);
        } catch (final MongoException me) {
            this.logger.catching(me);
        }

        this.logger.exit();
    }

    private void upsertThatUpdates() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.eq("color", "orange");
        final var update = Updates.inc("quantity", 15);
        final var options = new UpdateOptions().upsert(true);

        try {
            final var result = collection.updateOne(filter, update, options);

            this.logger.info("{}", result);
        } catch (final MongoException me) {
            this.logger.catching(me);
        }

        this.logger.exit();
    }

    private void deleteData() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.empty();

        try {
            final var result = collection.deleteMany(filter);

            this.logger.info("{} document(s) were deleted", result.getDeletedCount());
        } catch (final MongoException me) {
            this.logger.catching(me);
        }

        this.logger.exit();
    }
}
