package stream;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;

public class GenericSerializer<T> implements Serializer<T> {

    private static final Charset CHARSET = StandardCharsets.UTF_8;
    static private final Gson gson = new GsonBuilder()
            .setDateFormat("yyyy-MM-dd HH:mm:ss")
            .create();

    @Override
    public byte[] serialize(String s, T o) {
//        java.util.logging.Logger.getGlobal().log(Level.SEVERE, toString());
        String object = gson.toJson(o);
//        java.util.logging.Logger.getGlobal().log(Level.SEVERE, object);
        // Return the bytes from the String 'line'
        return object.getBytes(CHARSET);
    }
}
