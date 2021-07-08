package stream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

class Main {
    public static void main(String[] args) {
        Properties props = createProperties();

        ZtmStream serDeJsonStream = new ZtmStream();
        final KafkaStreams streams = new KafkaStreams(serDeJsonStream.createTopology(), props);
//        streams.setUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
//            System.err.println("oh no! error! " + throwable.getMessage());
//        });
        streams.setUncaughtExceptionHandler(exception -> {
            System.err.println("oh no! error! " + exception.getMessage());
//                return StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            return null;
        });

        streams.start(); // TODO exception

//        final CountDownLatch latch = new CountDownLatch(1);
//
//        // attach shutdown handler to catch control-c
//        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
//            @Override
//            public void run() {
//                streams.close();
//                latch.countDown();
//            }
//        });
//
//        try {
//            streams.start();
//            latch.await();
//        } catch (Throwable e) {
//            System.exit(1);
//        }
//        System.exit(0);
    }

    private static Properties createProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wiaderko-ztm-stream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        return props;
    }
}