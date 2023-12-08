package net.jmp.demo.mongodb.atlas;

/*
 * (#)Query.java    0.9.0   12/07/2023
 * (#)Query.java    0.8.0   12/06/2023
 *
 * Copyright (c) Jonathan M. Parker
 * All Rights Reserved.
 *
 * @author    Jonathan Parker
 * @version   0.9.0
 * @since     0.8.0
 */

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;

import com.mongodb.client.MongoClient;

import com.mongodb.client.model.*;

import java.util.*;

import org.bson.Document;

import org.bson.conversions.Bson;

import org.slf4j.LoggerFactory;

import org.slf4j.ext.XLogger;

final class Query {
    private final XLogger logger = new XLogger(LoggerFactory.getLogger(this.getClass().getName()));
    private final MongoClient mongoClient;
    private final String dbName;
    private final String collectionName;
    private final Bson projectionFields = Projections.fields(
            Projections.include("color", "qty", "vendor", "rating"),
            Projections.excludeId());

    Query(final Properties properties, final MongoClient mongoClient) {
        super();

        this.mongoClient = mongoClient;

        this.dbName = properties.getProperty("mongodb.query.db", "training");
        this.collectionName = properties.getProperty("mongodb.query.collection", "colors");
    }

    void run() {
        this.logger.entry();
        this.logger.info("Beginning query operations...");

        this.insertData();

        this.comparisonOperators();
        this.logicalOperators();
        this.arrayOperators();
        this.elementOperators();
        this.evaluationOperators();

        this.deleteData();

        this.logger.info("Ending query operations...");
        this.logger.exit();
    }

    private void insertData() {
        this.logger.entry();

        final var jsonDocuments = List.of(
            "{ \"_id\": 1, \"color\": \"red\", \"qty\": 9, \"vendor\": [\"A\", \"E\"] }",
            "{ \"_id\": 2, \"color\": \"purple\", \"qty\": 8, \"vendor\": [\"B\", \"D\", \"F\"], \"rating\": 5 }",
            "{ \"_id\": 3, \"color\": \"blue\", \"qty\": 5, \"vendor\": [\"A\", \"E\"] }",
            "{ \"_id\": 4, \"color\": \"white\", \"qty\": 6, \"vendor\": [\"D\"], \"rating\": 9 }",
            "{ \"_id\": 5, \"color\": \"yellow\", \"qty\": 4, \"vendor\": [\"A\", \"B\"] }",
            "{ \"_id\": 6, \"color\": \"pink\", \"qty\": 3, \"vendor\": [\"C\"] }",
            "{ \"_id\": 7, \"color\": \"green\", \"qty\": 8, \"vendor\": [\"C\", \"E\"], \"rating\": 7 }",
            "{ \"_id\": 8, \"color\": \"black\", \"qty\": 7, \"vendor\": [\"A\", \"C\", \"D\"] }"
        );

        final List<Document> documents = new ArrayList<>();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);

        jsonDocuments.forEach(jsonDocument -> documents.add(Document.parse(jsonDocument)));

        try {
            final var result = collection.insertMany(documents);

            result.getInsertedIds().values()
                    .forEach(id -> this.logger.info("Inserted document: {}", id.asInt32().getValue()));
        } catch (final MongoBulkWriteException mbwe) {
            this.logger.catching(mbwe);

            mbwe.getWriteResult().getInserts()
                    .forEach(doc -> this.logger.info("Inserted document: {}", doc.getId().asInt32().getValue()));
        }

        this.logger.exit();
    }

    private void comparisonOperators() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.gt("qty", 7);

        final var sort = Sorts.orderBy(
                Sorts.descending("qty"),
                Sorts.ascending("color")
        );

        try (final var cursor = collection
                .find(filter)
                .projection(this.projectionFields)
                .sort(sort)
                .iterator()) {
            Helpers.printCursor(cursor, "comparisonOperators", this.logger);
        }

        this.logger.exit();
    }

    private void logicalOperators() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);

        final var filter = Filters.and(
                Filters.lte("qty", 5),
                Filters.ne("color", "pink")
        );

        final var sort = Sorts.orderBy(
                Sorts.descending("qty"),
                Sorts.ascending("color")
        );

        try (final var cursor = collection
                .find(filter)
                .projection(this.projectionFields)
                .sort(sort)
                .iterator()) {
            Helpers.printCursor(cursor, "logicalOperators", this.logger);
        }

        this.logger.exit();
    }

    private void arrayOperators() {
        this.logger.entry();

        this.arraySizeOperator();
        this.arrayValueOperator();

        this.logger.exit();
    }

    private void arraySizeOperator() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.size("vendor", 3);   // 3 elements in the array

        try (final var cursor = collection
                .find(filter)
                .projection(this.projectionFields)
                .sort(Sorts.ascending("color"))
                .iterator()) {
            Helpers.printCursor(cursor, "arraySizeOperator", this.logger);
        }

        this.logger.exit();
    }

    private void arrayValueOperator() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.eq("vendor", "A");

        try (final var cursor = collection
                .find(filter)
                .projection(this.projectionFields)
                .sort(Sorts.ascending("color"))
                .iterator()) {
            Helpers.printCursor(cursor, "arrayValueOperator", this.logger);
        }

        this.logger.exit();
    }

    private void elementOperators() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.exists("rating");

        try (final var cursor = collection
                .find(filter)
                .projection(this.projectionFields)
                .sort(Sorts.ascending("rating"))
                .iterator()) {
            Helpers.printCursor(cursor, "elementOperators", this.logger);
        }

        this.logger.exit();
    }

    private void evaluationOperators() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.regex("color", "k$");

        try (final var cursor = collection
                .find(filter)
                .projection(this.projectionFields)
                .sort(Sorts.ascending("color"))
                .iterator()) {
            Helpers.printCursor(cursor, "evaluationOperators", this.logger);
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
