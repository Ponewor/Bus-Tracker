package stream;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ZtmRecordDeserializer implements Deserializer<ZtmRecord> {

    private static final Charset CHARSET = StandardCharsets.UTF_8;
    static private final Gson gson = new GsonBuilder()
            .setDateFormat("yyyy-MM-dd HH:mm:ss")
            .create();

    @Override
    public ZtmRecord deserialize(String s, byte[] bytes) {
        String person = new String(bytes, CHARSET);
//        System.out.println("THIS IS WHAT I'M DEALING WITH: " + person);
        Pattern pattern = Pattern.compile("(.*)time\":\"(.*)\",\"speed(.*)");
        Matcher matcher = pattern.matcher(person);
        matcher.matches();
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        DateFormat format2 = new SimpleDateFormat("MMM d, yyyy, h:mm:ss a");
        DateFormat format3 = new SimpleDateFormat("MMM d, yyyy h:mm:ss a");
        try {
            Date date = format2.parse(matcher.group(2));
            person = matcher.group(1) + "time\":\"" + format.format(date) + "\",\"speed" + matcher.group(3);
//            System.out.println("TEN PASKUDNY PRZYPADEK");
//            return gson.fromJson(person, ZtmRecord.class);
        } catch (ParseException ignored) {
//            System.out.println("nie zmaczowało");
        }

        try {
            Date date = format3.parse(matcher.group(2));
            person = matcher.group(1) + "time\":\"" + format.format(date) + "\",\"speed" + matcher.group(3);
//            System.out.println("TEN PASKUDNY PRZYPADEK");
//            return gson.fromJson(person, ZtmRecord.class);
        } catch (ParseException ignored) {
//            System.out.println("nie zmaczowało");
        }

//        Date d = new Date(121, Calendar.JULY, 7, 6, 48, 43);
//        System.out.println(format2.format(d));
//        try {
//            format2.parse(format2.format(d));
//            System.out.println(format2.format(d) + " " + matcher.group(2));
//            System.out.println(format2.format(d).equals(matcher.group(2)));
//        } catch (ParseException e) {
//            System.out.println("TO NIE MA NAJMNIEJSZEGO SENSU");
//        }
//        System.out.println(person);
        return gson.fromJson(person, ZtmRecord.class);
    }
}
