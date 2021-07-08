package stream;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;

public class InputZtmRecordToZtmRecordDeserializer implements Deserializer<ZtmRecord> {
    private static final Charset CHARSET = StandardCharsets.UTF_8;
    static private final Gson gson = new GsonBuilder()
            .setDateFormat("yyyy-MM-dd HH:mm:ss")
            .create();
    private static int c = 0;

    @Override
    public ZtmRecord deserialize(String s, byte[] bytes) {
        String rawRecord = new String(bytes, CHARSET);
//        System.out.println("AAAAAAAAAAAAA");
//        System.out.println(rawRecord);
//        System.out.println("BBBBBBBBBBBBBBB");
//        c++;
//        if (c == 10) {
//            System.exit(0);
//        }
//        System.exit(0);
//        java.util.logging.Logger.getGlobal().log(Level.SEVERE, "INPUT " + rawRecord);
        InputZtmRecord inputRecord = gson.fromJson(rawRecord, InputZtmRecord.class);
        return new ZtmRecord(inputRecord);
    }
}
