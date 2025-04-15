package kafka.wiki;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.example.EnvConfig;

public class WikiChangesProducer {
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, EnvConfig.BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Set high throughput
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, EnvConfig.LINGER_MS);
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, EnvConfig.BATCH_SIZE);
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, EnvConfig.COMPRESSION_TYPE);

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        BackgroundEventHandler handler = new WikiChangeHandler(kafkaProducer, EnvConfig.TOPIC);

        BackgroundEventSource.Builder builder = new BackgroundEventSource.Builder(handler, new EventSource.Builder(URI.create(EnvConfig.URL)));

        BackgroundEventSource eventSource = builder.build();

        eventSource.start();

        // Produce for 10 minutes and block the program
        TimeUnit.MINUTES.sleep(10);
    }
}
