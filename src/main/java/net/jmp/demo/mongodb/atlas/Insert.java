package net.jmp.demo.mongodb.atlas;

/*
 * (#)Insert.java   0.8.0   12/06/2023
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

import java.util.List;
import java.util.Objects;
import java.util.Properties;

import org.bson.Document;

import org.bson.types.ObjectId;

import org.slf4j.LoggerFactory;

import org.slf4j.ext.XLogger;

final class Insert {
    private final XLogger logger = new XLogger(LoggerFactory.getLogger(this.getClass().getName()));
    private final MongoClient mongoClient;
    private final String dbName;
    private final String collectionName;

    Insert(final Properties properties, final MongoClient mongoClient) {
        super();

        this.mongoClient = mongoClient;

        this.dbName = properties.getProperty("mongodb.insert.db", "training");
        this.collectionName = properties.getProperty("mongodb.insert.collection", "colors");
    }

    void run() {
        this.logger.entry();
        this.logger.info("Beginning insert operations...");

        this.insertOneDocument();
        this.insertMultipleDocuments();

        Helpers.printAllDocuments(this.mongoClient,
                this.dbName,
                this.collectionName,
                this.logger);

        this.logger.info("Ending insert operations.");
        this.logger.exit();
    }

    private void insertOneDocument() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var document = new Document("color", "red").append("qty", 5);

        try {
            var result = collection.insertOne(document);

            this.logger.info("Inserted document: {}", Objects.requireNonNull(result.getInsertedId()).asObjectId().getValue());

            result = collection.insertOne(new Document()
                    .append("_id", new ObjectId())
                    .append("color", "orange")
                    .append("qty", 6)
            );

            this.logger.info("Inserted document: {}", Objects.requireNonNull(result.getInsertedId()).asObjectId().getValue());
        } catch (final MongoException me) {
            this.logger.catching(me);
        }

        this.logger.exit();
    }

    private void insertMultipleDocuments() {
        this.logger.entry();

        final var color = "color";
        final var quantity = "qty";
        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);

        final var documents = List.of(
                new Document(color, "blue").append(quantity, 5),
                new Document(color, "purple").append(quantity, 8),
                new Document(color, "green").append(quantity, 9),
                new Document(color, "yellow").append(quantity, 5)
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
}
