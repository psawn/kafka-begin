package kafka;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.EnvConfig;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerOpenSearch {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerOpenSearch.class.getSimpleName());

    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "http://localhost:9200";

        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);

        String userInfo = connUri.getUserInfo();
        if (userInfo == null) {
            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));
        } else {
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                              .setHttpClientConfigCallback(
                                      httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                                                                      .setKeepAliveStrategy(
                                                                                              new DefaultConnectionKeepAliveStrategy())));
        }

        return restHighLevelClient;
    }

    public static void main(String[] args) throws IOException {
        // Create OpenSearch client
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer();

        // Get a reference to the main thread
        final Thread mainThread = Thread.currentThread();

        // Add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                logger.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                kafkaConsumer.wakeup(); // Interrupt consumer.poll() in main thread -> poll() will throw WakeupException

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    logger.error("Exception", e);
                }
            }
        });

        try (openSearchClient; kafkaConsumer) {
            // Create index on OpenSearch if it doesn't exist
            GetIndexRequest getIndexRequest = new GetIndexRequest("kafka-wiki");

            boolean indexExists = openSearchClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT);

            if (!indexExists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("kafka-wiki");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);

                logger.info("Index created: kafka-wiki");
            } else {
                logger.info("Index already exists: kafka-wiki");
            }

            // Subscribe to Kafka topic
            kafkaConsumer.subscribe(Collections.singleton(EnvConfig.TOPIC));

            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(3000));

                int recordCount = records.count();

                logger.info("Received {} records", recordCount);

                // Can create a bulk request to send multiple records at once -> better performance
                BulkRequest bulkRequest = new BulkRequest();

                // Send records to OpenSearch
                records.forEach(record -> {
                    try {
                        // Strategy for handling duplicate records when processing down
                        // Strategy 1: define unique Id
                        String id1 = record.topic() + "_" + record.partition() + "_" + record.offset();

                        // Strategy 2: extract id unique ID
                        String id2 = extractId(record.value());

                        // Using id opensearch will update the record if it already exists -> idempotent
                        IndexRequest indexRequest = new IndexRequest("kafka-wiki").source(record.value(),
                                                                                          XContentType.JSON).id(id1);

                        // Send one by one
                        // IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
                        // logger.info(response.getId());

                        // Add to bulk request
                        bulkRequest.add(indexRequest);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });


                if (bulkRequest.numberOfActions() > 0) {
                    // Send bulk request
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);

                    logger.info("Sent: " + bulkResponse.getItems().length + " records");

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }

                // Commit offset if turn off auto commit
                // kafkaConsumer.commitSync();
                // logger.info("Offsets have been committed");
            }
        } catch (WakeupException e) {
            logger.info("Consumer is starting to shutdown");
        } catch (Exception e) {
            logger.error("Unexpected exception", e);
        } finally {
            kafkaConsumer.close();
            openSearchClient.close();

            logger.info("The consumer is now gracefully closed.");
        }
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        String groupId = "consumer-opensearch";

        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, EnvConfig.BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        return consumer;
    }


    private static String extractId(String json) {
        return JsonParser.parseString(json).getAsJsonObject().get("meta").getAsJsonObject().get("id").getAsString();
    }
}
