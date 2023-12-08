package net.jmp.demo.mongodb.atlas;

/*
 * (#)Helpers.java  0.9.0   12/07/2023
 * (#)Helpers.java  0.8.0   12/06/2023
 *
 * Copyright (c) Jonathan M. Parker
 * All Rights Reserved.
 *
 * @author    Jonathan Parker
 * @version   0.9.0
 * @since     0.8.0
 */

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCursor;

import org.bson.Document;

import org.bson.conversions.Bson;

import org.slf4j.ext.XLogger;

final class Helpers {
    private Helpers() {
        super();
    }

    static void printAllDocuments(final MongoClient mongoClient,
                                  final String databaseName,
                                  final String collectionName,
                                  final XLogger logger) {
        logger.entry(mongoClient, databaseName, collectionName);

        final var database = mongoClient.getDatabase(databaseName);
        final var collection = database.getCollection(collectionName);

        collection.find().forEach(doc -> {
            if (logger.isInfoEnabled())
                logger.info(doc.toJson());
        });

        logger.exit();
    }

    static void printOneDocument(final MongoClient mongoClient,
                                  final String databaseName,
                                  final String collectionName,
                                  final Bson filter,
                                  final XLogger logger) {
        logger.entry(mongoClient, databaseName, collectionName, filter);

        final var database = mongoClient.getDatabase(databaseName);
        final var collection = database.getCollection(collectionName);
        final var document = collection.find(filter).first();

        if (document != null && logger.isInfoEnabled())
            logger.info(document.toJson());

        logger.exit();
    }

    static void printCursor(final MongoCursor<Document> mongoCursor,
                             final String methodName,
                             final XLogger logger) {
        logger.entry(mongoCursor, methodName);

        logger.info("There are {} results available for {}", mongoCursor.available(), methodName);

        while (mongoCursor.hasNext()) {
            if (logger.isInfoEnabled())
                logger.info(mongoCursor.next().toJson());
        }

        logger.exit();
    }
}
