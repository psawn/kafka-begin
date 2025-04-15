package kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.EnvConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallback {

    private static final Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class.getSimpleName());

    public static void main(String[] args) {
        logger.info("Kafka Producer With Callback Example");

        String topic = "my_topic";

        // Create a properties object
        Properties properties = new Properties();

        // Set the properties for the producer
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, EnvConfig.BOOTSTRAP_SERVERS);

        // Create producer config
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Not recommend for production
        // properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());
        // properties.setProperty("batch.size", "400");

        // Create a Kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            // Create a producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, "hello world " + i);

            // Send the record to the topic
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Record sent successfully \nTopic: {}\nPartition: {}\nOffset: {}\nTimestamp: {}",
                                    recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(),
                                    recordMetadata.timestamp());
                    } else {
                        logger.error("Error while sending record", e);
                    }
                }
            });
        }

        // Tell producer to send all data and block until done - synchronous
        // producer.flush();

        // Close the producer -> close() contains flush()
        producer.close();
    }
}
