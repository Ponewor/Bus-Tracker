package stream.serializers;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Deserializer;
import stream.models.VehicleStatus;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ZtmRecordDeserializer implements Deserializer<VehicleStatus> {

    private static final Charset CHARSET = StandardCharsets.UTF_8;
    static private final Gson gson = new GsonBuilder()
            .setDateFormat("yyyy-MM-dd HH:mm:ss")
            .create();

    @Override
    public VehicleStatus deserialize(String s, byte[] bytes) {
        String rawRecord = new String(bytes, CHARSET);
        Pattern pattern = Pattern.compile("(.*)time\":\"(.*)\",\"speed(.*)");
        Matcher matcher = pattern.matcher(rawRecord);
        matcher.matches();
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        DateFormat format2 = new SimpleDateFormat("MMM d, yyyy, h:mm:ss a");
        DateFormat format3 = new SimpleDateFormat("MMM d, yyyy h:mm:ss a");
        try {
            Date date = format2.parse(matcher.group(2));
            rawRecord = matcher.group(1) + "time\":\"" + format.format(date) + "\",\"speed" + matcher.group(3);
        } catch (ParseException ignored) {
        }

        try {
            Date date = format3.parse(matcher.group(2));
            rawRecord = matcher.group(1) + "time\":\"" + format.format(date) + "\",\"speed" + matcher.group(3);
        } catch (ParseException ignored) {
        }
        return gson.fromJson(rawRecord, VehicleStatus.class);
    }
}
