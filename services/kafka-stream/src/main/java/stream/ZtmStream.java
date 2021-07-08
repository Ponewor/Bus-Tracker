package stream;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

public class ZtmStream {

    public static final String INPUT_TOPIC = "ztm-input";
    public static final String OUTPUT_TOPIC = "ztm-output";

    public ZtmStream() {
    }

    public Topology createTopology() {
        final Serde<ZtmRecord> outputZtmRecordSerde = Serdes.serdeFrom(new GenericSerializer<>(), new ZtmRecordDeserializer());///new ZtmRecordDeserializer());

        StoreBuilder<KeyValueStore<String, ZtmRecord>> ztmStoreBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("ztmStore"),
                        Serdes.String(),
                        outputZtmRecordSerde
                );

        Topology topology = new Topology();
        topology.addSource("Source", new StringDeserializer(), new InputZtmRecordToZtmRecordDeserializer(), INPUT_TOPIC)
                .addProcessor("ZtmProcess", ZtmProcessor::new, "Source")
                .addStateStore(ztmStoreBuilder, "ZtmProcess")
                .addSink("Sink", OUTPUT_TOPIC, new StringSerializer(), new GenericSerializer<>(),"ZtmProcess");

        return topology;
    }
}