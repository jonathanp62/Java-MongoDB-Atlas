package net.jmp.demo.mongodb.atlas;

/*
 * (#)Main.java 0.10.0  12/09/2023
 * (#)Main.java 0.9.0   12/07/2023
 * (#)Main.java 0.8.0   12/06/2023
 *
 * Copyright (c) Jonathan M. Parker
 * All Rights Reserved.
 *
 * @author    Jonathan Parker
 * @version   0.10.0
 * @since     0.8.0
 */

import com.mongodb.client.MongoClients;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import java.util.Optional;
import java.util.Properties;

import org.slf4j.LoggerFactory;

import org.slf4j.ext.XLogger;

public final class Main {
    private static final String MONGODB_URI = "mongodb.uri";

    private final XLogger logger = new XLogger(LoggerFactory.getLogger(this.getClass().getName()));

    private Main() {
        super();
    }

    private void run() {
        this.logger.entry();
        this.logger.info("Beginning MongoDb Atlas demo...");

        final var properties = this.getAppProperties();

        properties.ifPresent(props -> {
            final var mongoDbUri = props.getProperty(MONGODB_URI);

            this.logger.info("Connecting to {}", mongoDbUri);

            try (final var mongoClient = MongoClients.create(mongoDbUri)) {
                new Find(props, mongoClient).run();
                new Insert(props, mongoClient).run();
                new UpdateAndReplace(props, mongoClient).run();
                new Delete(props, mongoClient).run();
                new UpdateArrays(props, mongoClient).run();
                new Upsert(props, mongoClient).run();
                new Bulk(props, mongoClient).run();
                new Query(props, mongoClient).run();
                new Compound(props, mongoClient).run();
                new Aggregation(props, mongoClient).run();
            } finally {
                this.logger.info("Disconnected from {}", mongoDbUri);
            }
        });

        this.logger.info("Ending MongoDb Atlas demo.");
        this.logger.exit();
    }

    private Optional<Properties> getAppProperties() {
        this.logger.entry();

        Properties properties = null;

        final var configFileName = System.getProperty("app.configurationFile");

        if (configFileName != null) {
            this.logger.info("Loading the configuration from {}", configFileName);

            properties = new Properties();

            try (final var fis = new FileInputStream(configFileName)) {
                properties.load(fis);

                this.replaceUriWithSecrets(properties);
            } catch (final IOException ioe) {
                this.logger.catching(ioe);
            }
        } else {
            this.logger.error("System property app.configurationFile was not found");
        }

        final var results = Optional.ofNullable(properties);

        this.logger.exit(results);

        return results;
    }

    private void replaceUriWithSecrets(final Properties appProperties) {
        this.logger.entry(appProperties);

        var mongoDbUri = appProperties.getProperty(MONGODB_URI);

        final var configFileName = System.getProperty("app.configurationFile");
        final var configDirectory = new File(configFileName).getParent();
        final var secretsConfigFileName = configDirectory + File.separator + "secrets.properties";

        this.logger.info("Loading the secrets configuration from {}", secretsConfigFileName);

        final var secretProperties = new Properties();

        try (final var fis = new FileInputStream(secretsConfigFileName)) {
            secretProperties.load(fis);

            mongoDbUri = mongoDbUri.replace("{uri.userid}", secretProperties.getProperty("mongodb.uri.userid"));
            mongoDbUri = mongoDbUri.replace("{uri.password}", secretProperties.getProperty("mongodb.uri.password"));
            mongoDbUri = mongoDbUri.replace("{uri.domain}", secretProperties.getProperty("mongodb.uri.domain"));

            appProperties.setProperty(MONGODB_URI, mongoDbUri);
        } catch (final IOException ioe) {
            this.logger.catching(ioe);
        }

        this.logger.exit();
    }

    public static void main(final String[] args) {
        new Main().run();
    }
}
