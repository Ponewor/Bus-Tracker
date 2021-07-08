package stream.serializers;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Deserializer;
import stream.models.VehicleStatus;
import stream.models.ZtmRecord;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class RecordToVehicleStatusDeserializer implements Deserializer<VehicleStatus> {
    private static final Charset CHARSET = StandardCharsets.UTF_8;
    static private final Gson gson = new GsonBuilder()
            .setDateFormat("yyyy-MM-dd HH:mm:ss")
            .create();

    @Override
    public VehicleStatus deserialize(String s, byte[] bytes) {
        String rawRecord = new String(bytes, CHARSET);
        ZtmRecord inputRecord = gson.fromJson(rawRecord, ZtmRecord.class);
        return new VehicleStatus(inputRecord);
    }
}
