package net.jmp.demo.mongodb.atlas;

/*
 * (#)Compound.java 0.9.0   12/07/2023
 *
 * Copyright (c) Jonathan M. Parker
 * All Rights Reserved.
 *
 * @author    Jonathan Parker
 * @version   0.9.0
 * @since     0.9.0
 */

import com.mongodb.MongoBulkWriteException;

import com.mongodb.MongoException;

import com.mongodb.client.MongoClient;

import com.mongodb.client.model.*;

import java.util.*;

import java.util.concurrent.TimeUnit;

import org.bson.Document;

import org.slf4j.LoggerFactory;

import org.slf4j.ext.XLogger;

final class Compound {
    private final XLogger logger = new XLogger(LoggerFactory.getLogger(this.getClass().getName()));
    private final MongoClient mongoClient;
    private final String dbName;
    private final String collectionName;

    Compound(final Properties properties, final MongoClient mongoClient) {
        super();

        this.mongoClient = mongoClient;

        this.dbName = properties.getProperty("mongodb.compound.db", "training");
        this.collectionName = properties.getProperty("mongodb.compound.collection", "food");
    }

    void run() {
        this.logger.entry();
        this.logger.info("Beginning compound operations...");

        this.insertData();

        try {
            this.findAndUpdate();
            this.findAndReplace();
            this.findAndDelete();

            this.raceCondition();
            this.noRaceCondition();
        } finally {
            this.deleteData();
        }

        this.logger.info("Ending compound operations...");
        this.logger.exit();
    }

    private void insertData() {
        this.logger.entry();

        final var jsonDocuments = List.of(
                "{ \"_id\": 1, \"food\": \"donut\", \"color\": \"green\" }",
                "{ \"_id\": 2, \"food\": \"pear\", \"color\": \"yellow\" }",
                "{ \"_id\": 3, \"guest\": null, \"room\": \" Blue Room\", \"reserved\": false }"
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

    private void findAndUpdate() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.eq("color", "green");
        final var projection = Projections.excludeId();
        final var update = Updates.set("food", "pizza");

        final var options = new FindOneAndUpdateOptions().
                projection(projection).
                upsert(true).
                maxTime(5, TimeUnit.SECONDS).
                returnDocument(ReturnDocument.AFTER);

        // The found document is in the state AFTER the update

        final var document = collection.findOneAndUpdate(filter, update, options);

        Helpers.printOneDocument(document, this.logger);

        this.logger.exit();
    }

    private void findAndReplace() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.eq("color", "green");
        final var projection = Projections.excludeId();
        final var newDocument = new Document("music", "classical").append("color", "green");

        final var options = new FindOneAndReplaceOptions().
                projection(projection).
                upsert(true).
                maxTime(5, TimeUnit.SECONDS).
                returnDocument(ReturnDocument.AFTER);

        // The found document is in the state AFTER the update

        final var document = collection.findOneAndReplace(filter, newDocument, options);

        Helpers.printOneDocument(document, this.logger);

        this.logger.exit();
    }

    private void findAndDelete() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.empty();
        final var sort = Sorts.ascending("_id");
        final var options = new FindOneAndDeleteOptions().sort(sort);

        // The deleted document is returned

        final var document = collection.findOneAndDelete(filter, options);

        Helpers.printOneDocument(document, this.logger);

        this.logger.exit();
    }

    private void raceCondition() {
        this.logger.entry();

        final Runnable john = () -> this.bookRoomWithRaceCondition("John");
        final Runnable jane = () -> this.bookRoomWithRaceCondition("Jane");

        final var t1 = new Thread(john);
        final var t2 = new Thread(jane);

        t1.start();
        t2.start();

        try {
            t1.join();
            t2.join();
        } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
        }

        this.logger.exit();
    }

    private void noRaceCondition() {
        this.logger.entry();
        this.logger.exit();
    }

    private void bookRoomWithRaceCondition(final String name) {
        this.logger.entry(name);

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.eq("reserved", false);
        final var room = collection.find(filter).first();

        if (room == null) {
            this.logger.warn("Sorry, {}, a room is not available", name);
        } else {
            this.logger.info("Congratulations, {}, a room is available", name);

            final var update = Updates.combine(Updates.set("reserved", true), Updates.set("guest", name));
            final var roomFilter = Filters.eq("_id", room.get("_id", Integer.class));

            collection.updateOne(roomFilter, update);
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
