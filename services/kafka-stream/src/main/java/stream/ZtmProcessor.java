package stream;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.lucene.spatial.util.GeoDistanceUtils;
import org.apache.lucene.spatial.util.GeoProjectionUtils;
import stream.models.VehicleStatus;

public class ZtmProcessor implements Processor<String, VehicleStatus, String, VehicleStatus> {
    public static final int SUSPICIOUS_SPEED = 120;
    private ProcessorContext<String, VehicleStatus> context;
    private KeyValueStore<String, VehicleStatus> vehicleStatusStore;

    @Override
    public void init(ProcessorContext<String, VehicleStatus> context) {
        this.context = context;
        vehicleStatusStore = context.getStateStore("ztmStore");
    }

    @Override
    public void process(Record<String, VehicleStatus> record) {
        VehicleStatus previousRecord = vehicleStatusStore.get(record.key());
        if (previousRecord == null) {
            vehicleStatusStore.put(record.key(), record.value());
            context.forward(record);
            return;
        }
        if (previousRecord.time.compareTo(record.value().time) >= 0) {
            return;
        }
        VehicleStatus newValue = calculateRecord(previousRecord, record.value());
        if (record.value().speed > SUSPICIOUS_SPEED) {
            return;
        }
        vehicleStatusStore.put(record.key(), newValue);
        context.forward(record);
    }

    private VehicleStatus calculateRecord(VehicleStatus previousRecord, VehicleStatus record) {
        double lat1 = previousRecord.lat;
        double lat2 = record.lat;
        double lon1 = previousRecord.lon;
        double lon2 = record.lon;
        double distance = GeoDistanceUtils.vincentyDistance(lon1, lat1, lon2, lat2);
        record.bearing = GeoProjectionUtils.bearingGreatCircle(lon1, lat1, lon2, lat2);
        record.speed = distance / ((record.time.getTime() - previousRecord.time.getTime()) * 60 * 60);
        return record;
    }

    @Override
    public void close() {
    }
}
