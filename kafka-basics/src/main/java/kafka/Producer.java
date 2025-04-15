package kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.EnvConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer {

    private static final Logger logger = LoggerFactory.getLogger(Producer.class.getSimpleName());

    public static void main(String[] args) {
        logger.info("Kafka Producer Example");

        String topic = "my_topic";

        // Create a properties object
        Properties properties = new Properties();

        // Set the properties for the producer
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, EnvConfig.BOOTSTRAP_SERVERS);

        // Create producer config
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class.getName());


        // Load schema registry URL from environment variable
        properties.setProperty("schema.registry.url", "http://localhost:8081");
        properties.setProperty("auto.register.schemas", "true");
        properties.setProperty("json.oneof.for.nullables", "false");

        // Create a Kafka producer
        // KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        KafkaProducer<String, User> producer = new KafkaProducer<String, User>(properties);

        User user = new User("1");

        // Create a producer record
        // ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, "hello");
        ProducerRecord<String, User> producerRecord = new ProducerRecord<String, User>(topic, "1", user);

        // Send the record to the topic
        producer.send(producerRecord);

        // Tell producer to send all data and block until done - synchronous
        // producer.flush();

        // Close the producer -> close() contains flush()
        producer.close();
    }
}
