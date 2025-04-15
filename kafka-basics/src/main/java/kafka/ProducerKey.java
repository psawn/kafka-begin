package kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.EnvConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerKey {

    private static final Logger logger = LoggerFactory.getLogger(ProducerKey.class.getSimpleName());

    public static void main(String[] args) {
        logger.info("Kafka Producer Key Example");

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

        for (int j = 0; j < 2; j ++) {
            for (int i = 0; i < 10; i++) {
                String key = "key_" + i;
                String value = "hello world " + i;

                // Create a producer record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

                // Send the record to the topic
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e == null) {
                            logger.info("Key: {} | Partition: {}", key, recordMetadata.partition());
                        } else {
                            logger.error("Error while sending record", e);
                        }
                    }
                });
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                logger.error("Error while sleeping", e);
            }
        }

        // Tell producer to send all data and block until done - synchronous
        // producer.flush();

        // Close the producer -> close() contains flush()
        producer.close();
    }
}
