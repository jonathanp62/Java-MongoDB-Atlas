package net.jmp.demo.mongodb.atlas;

/*
 * (#)Find.java 0.8.0   12/06/2023
 *
 * Copyright (c) Jonathan M. Parker
 * All Rights Reserved.
 *
 * @author    Jonathan Parker
 * @version   0.8.0
 * @since     0.8.0
 */

import com.mongodb.client.MongoClient;

import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;

import java.util.Properties;

import org.slf4j.LoggerFactory;

import org.slf4j.ext.XLogger;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.lt;

final class Find {
    private final XLogger logger = new XLogger(LoggerFactory.getLogger(this.getClass().getName()));
    private final MongoClient mongoClient;
    private final String dbName;
    private final String collectionName;

    Find(final Properties properties, final MongoClient mongoClient) {
        super();

        this.mongoClient = mongoClient;

        this.dbName = properties.getProperty("mongodb.find.db", "sample_mflix");
        this.collectionName = properties.getProperty("mongodb.find.collection", "movies");
    }

    void run() {
        this.logger.entry();
        this.logger.info("Beginning find operations...");

        this.findOneDocument();
        this.findMultipleDocuments();

        this.logger.info("Ending find operations.");
        this.logger.exit();
    }

    private void findOneDocument() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);

        final var projectionFields = Projections.fields(
                Projections.include("title", "imdb"),
                Projections.excludeId());

        final var document = collection
                .find(eq("title", "The Room"))
                .projection(projectionFields)
                .sort(Sorts.descending("imdb.rating"))
                .first();

        if (document != null) {
            if (this.logger.isInfoEnabled())
                this.logger.info(document.toJson());
        } else {
            this.logger.info("No matching document(s) found: {}", "{'title': 'The Room'}");
        }

        this.logger.exit();
    }

    private void findMultipleDocuments() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);

        final var projectionFields = Projections.fields(
                Projections.include("title", "runtime", "imdb"),
                Projections.excludeId());

        try (final var cursor = collection
                .find(lt("runtime", 15))
                .projection(projectionFields)
                .sort(Sorts.descending("title"))
                .iterator()) {
            this.logger.info("There are {} results available", cursor.available());

            while (cursor.hasNext()) {
                if (this.logger.isInfoEnabled())
                    this.logger.info(cursor.next().toJson());
            }
        }

        this.logger.exit();
    }
}
