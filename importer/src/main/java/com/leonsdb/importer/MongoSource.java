// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.importer;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import java.util.concurrent.TimeUnit;
import org.bson.Document;

/** Documents of a MongoDB collection. The source is only queried, never changed. */
final class MongoSource implements RowSource {

    private final Plan.Source source;
    private final Plan.From from;
    private final MongoClient client;

    MongoSource(Plan.Source source, Plan.From from) throws ImportException {
        this.source = source;
        this.from = from;
        try {
            ConnectionString cs = new ConnectionString(source.url());
            MongoClientSettings.Builder b = MongoClientSettings.builder().applyConnectionString(cs)
                    .applyToClusterSettings(c -> c.serverSelectionTimeout(15, TimeUnit.SECONDS));
            if (source.user() != null) {
                String authDb = cs.getCredential() != null ? cs.getCredential().getSource() : "admin";
                b.credential(MongoCredential.createCredential(source.user(), authDb,
                        source.password() == null ? new char[0] : source.password().toCharArray()));
            }
            client = MongoClients.create(b.build());
        } catch (IllegalArgumentException | MongoException e) {
            throw new ImportException("Cannot connect to " + source.describe() + ": " + e.getMessage(), e);
        }
    }

    private MongoDatabase database() throws ImportException {
        String name = source.database() != null ? source.database() : new ConnectionString(source.url()).getDatabase();
        if (name == null) {
            throw new ImportException("source.database: name the MongoDB database (or put it in the URL: mongodb://host/database)");
        }
        return client.getDatabase(name);
    }

    @Override
    public void forEach(RowConsumer consumer) throws ImportException {
        try {
            Document filter = from.filter() == null ? new Document() : new Document(from.filter());
            try (MongoCursor<Document> it = database().getCollection(from.collection()).find(filter).batchSize(1000).iterator()) {
                long n = 0;
                while (it.hasNext()) {
                    Document doc = it.next();
                    long row = ++n;
                    Object id = doc.get("_id");
                    consumer.accept(new Row() {
                        @Override
                        public Object get(String path) {
                            return RowSource.path(doc, path);
                        }

                        @Override
                        public String where() {
                            return "document " + (id != null ? id : "#" + row);
                        }
                    });
                }
            }
        } catch (MongoException e) {
            throw new ImportException("Cannot read collection " + from.collection() + " from " + source.describe() + ": "
                    + e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        client.close();
    }
}
