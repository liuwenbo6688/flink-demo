import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class TestKafka001 {


    public static void main(String[] args) throws Exception {


        Properties kfkProperties = new Properties();

        kfkProperties.put("bootstrap.servers", "10.18.104.117:9092,10.18.104.118:9092,10.18.104.226:9092,10.18.104.227:9092");
        kfkProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kfkProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        /**
         * 必调参数
         */
        kfkProperties.put("buffer.memory", "63554432");
        kfkProperties.put("batch.size", "263840");
        kfkProperties.put("retries", "5");
        kfkProperties.put("linger.ms", "50");


        KafkaProducer<String, String> producer = new KafkaProducer<>(kfkProperties);


        String data0 = generate0400();
        System.out.println(data0);
        ProducerRecord<String, String> record0 =
                new ProducerRecord<>("fridge_reportData_0400", data0);


//        String data1 = generate0401();
//        ProducerRecord<String, String>  record1 =
//                new ProducerRecord<>("fridge_reportData_0401", data1);
//        producer.send(record1);


//        String data2 = generateBatch();
//        ProducerRecord<String, String> record2 =
//                new ProducerRecord<>("fridge_batch", data2);
//        producer.send(record2);


        producer.flush();

    }


    public static String generate0400() {
        JSONObject json = new JSONObject();
        json.put("fridgeId", "10995116466298");
        json.put("lcsmkgzt", "0");
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        json.put("time", sdf.format(new Date()));
        return json.toJSONString();
    }


    public static String generate0401() {
        JSONObject json = new JSONObject();
        json.put("fridgeId", "10995116466298");
        json.put("lcswd", "10");
        json.put("lccgqwd", "16");

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        json.put("time", sdf.format(new Date()));
        return json.toJSONString();
    }

    public static String generateBatch() {
        JSONObject json = new JSONObject();
        json.put("applianceId", "10995116466298");
        json.put("timestamp", System.currentTimeMillis() + 60 * 1000l);

        return json.toJSONString();
    }


}
