package kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.EnvConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerCooperative {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerCooperative.class.getSimpleName());

    public static void main(String[] args) {
        logger.info("Kafka Consumer Example");

        String groupId = "my-java-application";
        String topic = "my_topic";

        // Create a properties object
        Properties properties = new Properties();

        // Set the properties for the producer
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, EnvConfig.BOOTSTRAP_SERVERS);

        // Create consumer config
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // earliest ~ start from beginning of topic
        // latest ~ start from end of topic
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

        // Consumer Static Group Membership
        // properties.setProperty("group.instance.id", "...");

        // Create a Kafka consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Get a reference to the main thread
        final Thread mainThread = Thread.currentThread();

        // Add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                logger.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup(); // Interrupt consumer.poll() in main thread -> poll() will throw WakeupException

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    logger.error("Exception", e);
                }
            }
        });

        try {
            // Subscribe to a topic
            consumer.subscribe(List.of(topic));

            // Poll for new data
            while (true) {
                logger.info("Polling");

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                // Loop through the records
                records.forEach(record -> {
                    logger.info("Key: {}, Value: {}", record.key(), record.value());
                    logger.info("Partition: {}, Offset: {}", record.partition(), record.offset());
                });
            }
        } catch (WakeupException e) {
            logger.info("Consumer is starting to shutdown");
        } catch (Exception e) {
            logger.error("Unexpected exception", e);
        } finally {
            consumer.close();
            logger.info("The consumer is now gracefully closed.");
        }
    }
}
