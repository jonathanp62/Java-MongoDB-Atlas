package net.jmp.demo.mongodb.atlas;

/*
 * (#)Bulk.java 0.8.0   12/06/2023
 *
 * Copyright (c) Jonathan M. Parker
 * All Rights Reserved.
 *
 * @author    Jonathan Parker
 * @version   0.8.0
 * @since     0.8.0
 */

import com.mongodb.MongoBulkWriteException;

import com.mongodb.client.MongoClient;

import com.mongodb.client.model.*;

import java.util.*;

import org.bson.Document;

import org.slf4j.LoggerFactory;

import org.slf4j.ext.XLogger;

final class Bulk {
    private final XLogger logger = new XLogger(LoggerFactory.getLogger(this.getClass().getName()));
    private final MongoClient mongoClient;
    private final String dbName;
    private final String collectionName;

    Bulk(final Properties properties, final MongoClient mongoClient) {
        super();

        this.mongoClient = mongoClient;

        this.dbName = properties.getProperty("mongodb.bulk.db", "training");
        this.collectionName = properties.getProperty("mongodb.bulk.collection", "colors");
    }

    void run() {
        this.logger.entry();
        this.logger.info("Beginning bulk operations...");

        this.insert();

        Helpers.printAllDocuments(this.mongoClient,
                this.dbName,
                this.collectionName,
                this.logger);

        this.replace();

        Helpers.printAllDocuments(this.mongoClient,
                this.dbName,
                this.collectionName,
                this.logger);

        this.update();

        Helpers.printAllDocuments(this.mongoClient,
                this.dbName,
                this.collectionName,
                this.logger);

        this.delete();

        this.logger.info("Ending bulk operations...");
        this.logger.exit();
    }

    private void insert() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);

        final var karenDoc = new InsertOneModel<>(new Document("name", "Karen Sandoval").append("age", 31));
        final var williamDoc = new InsertOneModel<>(new Document("name", "William Chin").append("age", 54));
        final var shaylaDoc = new InsertOneModel<>(new Document("name", "Shayla Ray").append("age", 20));
        final var juneDoc = new InsertOneModel<>(new Document("name", "June Carrie").append("age", 17));
        final var kevinDoc = new InsertOneModel<>(new Document("name", "Kevin Moss").append("age", 22));

        final List<WriteModel<Document>> bulkDocuments = new ArrayList<>();

        bulkDocuments.add(karenDoc);
        bulkDocuments.add(williamDoc);
        bulkDocuments.add(shaylaDoc);
        bulkDocuments.add(juneDoc);
        bulkDocuments.add(kevinDoc);

        final var options = new BulkWriteOptions().ordered(true);

        try {
            final var result = collection.bulkWrite(bulkDocuments, options);

            this.logger.info("Documents inserted: {}", result.getInsertedCount());
        } catch (final MongoBulkWriteException mbwe) {
            this.logger.catching(mbwe);

            mbwe.getWriteResult().getInserts()
                    .forEach(doc -> this.logger.info("Inserted document: {}", doc.getId().asObjectId().getValue()));
        }

        this.logger.exit();
    }

    private void replace() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.eq("name", "William Chin");

        final var marinaDoc = new ReplaceOneModel<>(filter,
                new Document("name", "Marina Squerciati")
                        .append("age", 39)
                        .append("location", "New York, NY"));

        final List<WriteModel<Document>> bulkDocuments = List.of(marinaDoc);

        try {
            final var result = collection.bulkWrite(bulkDocuments);

            if (this.logger.isInfoEnabled()) {
                this.logger.info("Documents matched: {}", result.getMatchedCount());
                this.logger.info("Documents modified: {}", result.getModifiedCount());
            }
        } catch (final MongoBulkWriteException mbwe) {
            this.logger.catching(mbwe);
        }

        this.logger.exit();
    }

    private void update() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.eq("name", "June Carrie");
        final var update = Updates.set("age", 19);

        final UpdateOneModel<Document> juneDoc = new UpdateOneModel<>(filter, update);

        final List<UpdateOneModel<Document>> bulkDocuments = List.of(juneDoc);

        try {
            final var result = collection.bulkWrite(bulkDocuments);

            if (this.logger.isInfoEnabled()) {
                this.logger.info("Documents matched: {}", result.getMatchedCount());
                this.logger.info("Documents modified: {}", result.getModifiedCount());
            }
        } catch (final MongoBulkWriteException mbwe) {
            this.logger.catching(mbwe);
        }

        this.logger.exit();
    }

    private void delete() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.empty();

        final DeleteManyModel<Document> deleteDocs = new DeleteManyModel<>(filter);

        final List<DeleteManyModel<Document>> bulkDocuments = List.of(deleteDocs);

        final var options = new BulkWriteOptions().ordered(false);

        try {
            final var result = collection.bulkWrite(bulkDocuments, options);

            this.logger.info("Documents deleted: {}", result.getDeletedCount());
        } catch (final MongoBulkWriteException mbwe) {
            this.logger.catching(mbwe);
        }

        this.logger.exit();
    }
}
