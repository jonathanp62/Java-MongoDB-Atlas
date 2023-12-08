package net.jmp.demo.mongodb.atlas;

/*
 * (#)UpdateArrays.java 0.8.0   12/06/2023
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

import java.util.*;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Updates;

import org.bson.Document;

import org.bson.types.ObjectId;

import org.slf4j.LoggerFactory;

import org.slf4j.ext.XLogger;

final class UpdateArrays {
    private final XLogger logger = new XLogger(LoggerFactory.getLogger(this.getClass().getName()));
    private final MongoClient mongoClient;
    private final String dbName;
    private final String collectionName;

    UpdateArrays(final Properties properties, final MongoClient mongoClient) {
        super();

        this.mongoClient = mongoClient;

        this.dbName = properties.getProperty("mongodb.update.db", "training");
        this.collectionName = properties.getProperty("mongodb.update.collection", "colors");
    }

    void run() {
        this.logger.entry();
        this.logger.info("Beginning update arrays operations...");

        final var objectId = this.insertData();

        if (objectId.isPresent()) {
            final var id = objectId.get();

            this.appendQuantity(id);
            this.printOneDocument(id);

            this.updateFirstArrayElement(id);
            this.printOneDocument(id);

            this.updateAllArrayElements(id);
            this.printOneDocument(id);

            this.updateMultipleArrayElements(id);
            this.printOneDocument(id);

            this.deleteData(id);
        }

        this.logger.info("Ending update arrays operations...");
        this.logger.exit();
    }

    private Optional<ObjectId> insertData() {
        this.logger.entry();

        ObjectId objectId = null;

        final List<Integer> quantities = List.of(
            8, 12, 18
        );

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var document = new Document("color", "green").append("quantities", quantities);

        try {
            final var insertOneResult = collection.insertOne(document);

            objectId = Objects.requireNonNull(insertOneResult.getInsertedId()).asObjectId().getValue();

            this.logger.info("Inserted document: {}", objectId);
        } catch (final MongoException me) {
            this.logger.catching(me);
        }

        final var result = Optional.ofNullable(objectId);

        this.logger.exit(result);

        return result;
    }

    private void appendQuantity(final ObjectId objectId) {
        this.logger.entry(objectId);

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.eq("_id", objectId);
        final var update = Updates.push("quantities", 26);

        final var options = new FindOneAndUpdateOptions()
                .returnDocument(ReturnDocument.AFTER);

        try {
            final var document = collection.findOneAndUpdate(filter, update, options);

            if (document != null) {
                @SuppressWarnings("unchecked")
                final List<Integer> quantities = document.get("quantities", List.class);

                if (quantities != null)
                    quantities.forEach(q -> this.logger.info("Quantity: {}", q));
            }
        } catch (final MongoException me) {
            this.logger.catching(me);
        }

        this.logger.exit();
    }

    private void updateFirstArrayElement(final ObjectId objectId) {
        this.logger.entry(objectId);

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);

        final var filter = Filters.and(
                Filters.eq("_id", objectId),
                Filters.eq("quantities", 18)
        );

        final var update = Updates.inc("quantities.$", -3);

        final var options = new FindOneAndUpdateOptions()
                .returnDocument(ReturnDocument.AFTER);

        try {
            final var document = collection.findOneAndUpdate(filter, update, options);

            if (document != null)
                this.logger.info("Updated the {} document", document.get("color"));
        } catch (final MongoException me) {
            this.logger.catching(me);
        }

        this.logger.exit();
    }

    private void updateAllArrayElements(final ObjectId objectId) {
        this.logger.entry(objectId);

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.eq("_id", objectId);
        final var update = Updates.mul("quantities.$[]", 2);

        final var options = new FindOneAndUpdateOptions()
                .returnDocument(ReturnDocument.AFTER);

        try {
            final var document = collection.findOneAndUpdate(filter, update, options);

            if (document != null)
                this.logger.info("Updated the {} document", document.get("color"));
        } catch (final MongoException me) {
            this.logger.catching(me);
        }

        this.logger.exit();
    }

    private void updateMultipleArrayElements(final ObjectId objectId) {
        this.logger.entry(objectId);

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.eq("_id", objectId);
        final var smallerFilter = Filters.lt("smaller", 30);
        final var update = Updates.inc("quantities.$[smaller]", 5);

        final var options = new FindOneAndUpdateOptions()
                .returnDocument(ReturnDocument.AFTER)
                .arrayFilters(List.of(smallerFilter));

        try {
            final var document = collection.findOneAndUpdate(filter, update, options);

            if (document != null)
                this.logger.info("Updated the {} document", document.get("color"));
        } catch (final MongoException me) {
            this.logger.catching(me);
        }

        this.logger.exit();
    }

    private void deleteData(final ObjectId objectId) {
        this.logger.entry(objectId);

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.eq("_id", objectId);

        try {
            final var result = collection.deleteOne(filter);

            this.logger.info("{} document(s) were deleted", result.getDeletedCount());
        } catch (final MongoException me) {
            this.logger.catching(me);
        }

        this.logger.exit();
    }

    private void printOneDocument(final ObjectId objectId) {
        this.logger.entry(objectId);

        Helpers.printOneDocument(this.mongoClient,
                this.dbName,
                this.collectionName,
                Filters.eq("_id", objectId),
                this.logger);

        this.logger.exit();
    }
}
