package stream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;

import java.util.Properties;

class Main {
    public static void main(String[] args) {
        Properties props = createProperties();

        ZtmStream serDeJsonStream = new ZtmStream();
        final KafkaStreams streams = new KafkaStreams(serDeJsonStream.createTopology(), props);
        streams.setUncaughtExceptionHandler(exception -> {
            System.err.println(exception.getMessage());
            return null;
        });

        streams.start();
    }

    private static Properties createProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ztm-stream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        return props;
    }
}