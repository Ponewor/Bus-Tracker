package stream.serializers;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class GenericSerializer<T> implements Serializer<T> {

    private static final Charset CHARSET = StandardCharsets.UTF_8;
    static private final Gson gson = new Gson();

    @Override
    public byte[] serialize(String s, T o) {
        String object = gson.toJson(o);
        return object.getBytes(CHARSET);
    }
}
