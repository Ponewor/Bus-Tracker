package stream;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.lucene.spatial.util.GeoDistanceUtils;
import org.apache.lucene.spatial.util.GeoProjectionUtils;

public class ZtmProcessor implements Processor<String, ZtmRecord, String, ZtmRecord> {
    public static final int SUSPICIOUS_SPEED = 120;
    private ProcessorContext<String, ZtmRecord> context;
    private KeyValueStore<String, ZtmRecord> ztmRecordStore;

    @Override
    public void init(ProcessorContext<String, ZtmRecord> context) {
        this.context = context;
        ztmRecordStore = context.getStateStore("ztmStore");
    }

    @Override
    public void process(Record<String, ZtmRecord> record) {
        ZtmRecord previousRecord = ztmRecordStore.get(record.key());
        if (previousRecord == null) {
            ztmRecordStore.put(record.key(), record.value());
            context.forward(record);
            return;
        }
        if (previousRecord.time.compareTo(record.value().time) >= 0) {
            return; // ignore old/same record
        }
        ZtmRecord newValue = calculateRecord(previousRecord, record.value());
        if (record.value().speed > SUSPICIOUS_SPEED)
        {
            return; // probably measurement error
        }
        ztmRecordStore.put(record.key(), newValue);
        context.forward(record);
        System.out.println("New record");
    }

    private ZtmRecord calculateRecord(ZtmRecord previousRecord, ZtmRecord record) {
        double lat1 = previousRecord.lat;
        double lat2 = record.lat;
        double lon1 = previousRecord.lon;
        double lon2 = record.lon;
        record.distance = GeoDistanceUtils.vincentyDistance(lon1, lat1, lon2, lat2);
        record.bearing = GeoProjectionUtils.bearingGreatCircle(lon1, lat1, lon2, lat2);
        record.speed = record.distance / ((record.time.getTime() - previousRecord.time.getTime()) * 60 * 60);
        return record;
    }

    @Override
    public void close() {
    }
}
