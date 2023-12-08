package net.jmp.demo.mongodb.atlas;

/*
 * (#)Delete.java   0.8.0   12/06/2023
 *
 * Copyright (c) Jonathan M. Parker
 * All Rights Reserved.
 *
 * @author    Jonathan Parker
 * @version   0.8.0
 * @since     0.8.0
 */

import com.mongodb.MongoException;

import com.mongodb.client.MongoClient;

import com.mongodb.client.model.Filters;

import java.util.Properties;

import org.bson.Document;

import org.slf4j.LoggerFactory;

import org.slf4j.ext.XLogger;

final class Delete {
    private final XLogger logger = new XLogger(LoggerFactory.getLogger(this.getClass().getName()));
    private final MongoClient mongoClient;
    private final String dbName;
    private final String collectionName;

    Delete(final Properties properties, final MongoClient mongoClient) {
        super();

        this.mongoClient = mongoClient;

        this.dbName = properties.getProperty("mongodb.insert.db", "training");
        this.collectionName = properties.getProperty("mongodb.insert.collection", "colors");
    }

    void run() {
        this.logger.entry();
        this.logger.info("Beginning delete operations...");

        this.deleteOneDocument();
        this.findAndDeleteOneDocument();
        this.deleteMultipleDocuments();
        this.deleteAllDocuments();

        this.logger.info("Ending delete operations.");
        this.logger.exit();
    }

    private void deleteOneDocument() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.eq("color", "red");

        try {
            final var result = collection.deleteOne(filter);

            this.logger.info("{} document(s) were deleted", result.getDeletedCount());
        } catch (final MongoException me) {
            this.logger.catching(me);
        }

        this.logger.exit();
    }

    private void findAndDeleteOneDocument() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.eq("color", "orange");

        try {
            final var document = collection.findOneAndDelete(filter);

            if (document != null) {
                if (this.logger.isInfoEnabled())
                    this.logger.info("Deleted document: {}", document.toJson());
            } else {
                this.logger.warn("No documents with a color of orange were found");
            }
        } catch (final MongoException me) {
            this.logger.catching(me);
        }

        this.logger.exit();
    }

    private void deleteMultipleDocuments() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.eq("qty", 15);

        try {
            final var result = collection.deleteMany(filter);

            this.logger.info("{} document(s) were deleted", result.getDeletedCount());
        } catch (final MongoException me) {
            this.logger.catching(me);
        }

        this.logger.exit();
    }

    private void deleteAllDocuments() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);

        // An empty document as a filter will delete all documents

        try {
            final var result = collection.deleteMany(new Document());

            this.logger.info("{} document(s) were deleted", result.getDeletedCount());
        } catch (final MongoException me) {
            this.logger.catching(me);
        }

        this.logger.exit();
    }
}
