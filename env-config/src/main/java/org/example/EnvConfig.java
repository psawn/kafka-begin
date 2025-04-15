package org.example;

import io.github.cdimascio.dotenv.Dotenv;

public class EnvConfig {
    private static final Dotenv dotenv = Dotenv.load();

    public static final String BOOTSTRAP_SERVERS = dotenv.get("KAFKA_BOOTSTRAP_SERVERS");
    public static final String TOPIC = dotenv.get("KAFKA_TOPIC");
    public static final String URL = dotenv.get("WIKI_RECENT_CHANGE_URL");
    public static final String LINGER_MS = dotenv.get("KAFKA_LINGER_MS_CONFIG");
    public static final String BATCH_SIZE = dotenv.get("KAFKA_BATCH_SIZE_CONFIG");
    public static final String COMPRESSION_TYPE = dotenv.get("KAFKA_COMPRESSION_TYPE_CONFIG");
}
