package net.jmp.demo.mongodb.atlas;

/*
 * (#)Aggregation.java  0.10.1  12/15/2023
 * (#)Aggregation.java  0.10.0  12/09/2023
 *
 * Copyright (c) Jonathan M. Parker
 * All Rights Reserved.
 *
 * @author    Jonathan Parker
 * @version   0.10.1
 * @since     0.10.0
 */

import com.mongodb.ExplainVerbosity;
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

        this.createCollection();

        try {
            this.insertData();
            this.basic();
            this.explain();
            this.expression();
        } finally {
            this.dropCollection();  // Will delete any documents in the collection
        }

        this.logger.info("Ending aggregation operations...");
        this.logger.exit();
    }

    private void createCollection() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collectionExists = database.listCollectionNames()
                .into(new ArrayList<>()).contains(this.collectionName);

        if (!collectionExists)
            database.createCollection(this.collectionName);

        this.logger.exit();
    }

    private void insertData() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);

        final var result = collection.insertMany(Arrays.asList(
                new Document("name", "Sun Bakery Trattoria").append("contact", new Document().append("phone", "386-555-0189").append("email", "SunBakeryTrattoria@example.org").append("location", Arrays.asList(-74.0056649, 40.7452371))).append("stars", 4).append("categories", Arrays.asList("Pizza", "Pasta", "Italian", "Coffee", "Sandwiches")),
                new Document("name", "Blue Bagels Grill").append("contact", new Document().append("phone", "786-555-0102").append("email", "BlueBagelsGrill@example.com").append("location", Arrays.asList(-73.92506, 40.8275556))).append("stars", 3).append("categories", Arrays.asList("Bagels", "Cookies", "Sandwiches")),
                new Document("name", "XYZ Bagels Restaurant").append("contact", new Document().append("phone", "435-555-0190").append("email", "XYZBagelsRestaurant@example.net").append("location", Arrays.asList(-74.0707363, 40.59321569999999))).append("stars", 4).append("categories", Arrays.asList("Bagels", "Sandwiches", "Coffee")),
                new Document("name", "Hot Bakery Cafe").append("contact", new Document().append("phone", "264-555-0171").append("email", "HotBakeryCafe@example.net").append("location", Arrays.asList(-73.96485799999999, 40.761899))).append("stars", 4).append("categories", Arrays.asList("Bakery", "Cafe", "Coffee", "Dessert")),
                new Document("name", "Green Feast Pizzeria").append("contact", new Document().append("phone", "840-555-0102").append("email", "GreenFeastPizzeria@example.com").append("location", Arrays.asList(-74.1220973, 40.6129407))).append("stars", 2).append("categories", Arrays.asList("Pizza", "Italian")),
                new Document("name", "ZZZ Pasta Buffet").append("contact", new Document().append("phone", "769-555-0152").append("email", "ZZZPastaBuffet@example.com").append("location", Arrays.asList(-73.9446421, 40.7253944))).append("stars", 0).append("categories", Arrays.asList("Pasta", "Italian", "Buffet", "Cafeteria")),
                new Document("name", "XYZ Coffee Bar").append("contact", new Document().append("phone", "644-555-0193").append("email", "XYZCoffeeBar@example.net").append("location", Arrays.asList(-74.0166091, 40.6284767))).append("stars", 5).append("categories", Arrays.asList("Coffee", "Cafe", "Bakery", "Chocolates")),
                new Document("name", "456 Steak Restaurant").append("contact", new Document().append("phone", "990-555-0165").append("email", "456SteakRestaurant@example.com").append("location", Arrays.asList(-73.9365108, 40.8497077))).append("stars", 0).append("categories", Arrays.asList("Steak", "Seafood")),
                new Document("name", "456 Cookies Shop").append("contact", new Document().append("phone", "604-555-0149").append("email", "456CookiesShop@example.org").append("location", Arrays.asList(-73.8850023, 40.7494272))).append("stars", 4).append("categories", Arrays.asList("Bakery", "Cookies", "Cake", "Coffee")),
                new Document("name", "XYZ Steak Buffet").append("contact", new Document().append("phone", "229-555-0197").append("email", "XYZSteakBuffet@example.org").append("location", Arrays.asList(-73.9799932, 40.7660886))).append("stars", 3).append("categories", Arrays.asList("Steak", "Salad", "Chinese"))
        ));

        this.logger.info("Documents inserted: {}", result.getInsertedIds().size());

        this.logger.exit();
    }

    private void basic() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);

        collection.aggregate(
                Arrays.asList(
                        Aggregates.match(Filters.eq("categories", "Bakery")),
                        Aggregates.group("$stars", Accumulators.sum("count", 1))
                )
        ).forEach(doc -> Helpers.printOneDocument(doc, this.logger));

        /*
         * {"_id": 4, "count": 2} // Two four-star bakeries
         * {"_id": 5, "count": 1} // One five-star bakery
         */

        this.logger.exit();
    }

    private void explain() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);

        final var aggregationIterable = collection.aggregate(
                Arrays.asList(
                        Aggregates.match(Filters.eq("categories", "Bakery")),
                        Aggregates.group("$stars", Accumulators.sum("count", 1))
                )
        );

        final Document explanation = aggregationIterable.explain(ExplainVerbosity.EXECUTION_STATS);

        @SuppressWarnings("unchecked")
        final List<Document> stages = explanation.get("stages", List.class);

        if (stages != null) {
            final List<String> keys = Arrays.asList("queryPlanner", "winningPlan");

            for (final var stage : stages) {
                final var cursorStage = stage.get("$cursor", Document.class);

                if (cursorStage != null && (this.logger.isInfoEnabled()))
                    this.logger.info(cursorStage.getEmbedded(keys, Document.class).toJson());
            }
        } else {
            /* Provided as the stages variable seems to always be null */

            Helpers.printOneDocument(explanation, this.logger);
        }

        this.logger.exit();
    }

    private void expression() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);

        collection.aggregate(
                Arrays.asList(
                        Aggregates.project(
                                Projections.fields(
                                        Projections.excludeId(),
                                        Projections.include("name"),
                                        Projections.computed(
                                                "firstCategory",
                                                new Document("$arrayElemAt", Arrays.asList("$categories", 0))
                                        )
                                )
                        )
                )
        ).forEach(doc -> Helpers.printOneDocument(doc, this.logger));

        this.logger.exit();
    }

    private void dropCollection() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);

        collection.drop();

        this.logger.exit();
    }
}
