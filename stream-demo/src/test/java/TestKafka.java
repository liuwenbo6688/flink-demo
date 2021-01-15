import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

public class TestKafka {


    private static KafkaProducer<String, String> producer;


    static String[] dates = {
            "2019-01-01",
            "2019-02-01",
            "2019-03-01",
            "2019-04-01",
            "2019-05-01",
            "2019-06-01",
            "2019-07-01",
            "2019-08-01",
            "2019-09-01",
            "2019-10-01",
            "2019-11-01",
            "2019-12-01",
            "2020-01-01",
            "2020-02-01",
            "2020-03-01",
            "2020-04-01",
            "2020-05-01",
            "2020-06-01",
            "2020-07-01",
            "2020-08-01",
            "2020-09-01",
            "2020-10-01",
            "2020-11-01",
            "2020-12-01"};


    public static void main(String[] args)  throws Exception {


        Properties kfkProperties = new Properties();

//        kfkProperties.put("bootstrap.servers", "10.16.74.40:9092,10.16.74.39:9092,10.16.74.38:9092");
        kfkProperties.put("bootstrap.servers", "10.18.94.52:9092,10.18.94.53:9092,10.18.94.54:9092");
        kfkProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kfkProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        /**
         * 必调参数
         */
        kfkProperties.put("buffer.memory", "63554432");
        kfkProperties.put("batch.size", "263840");
        kfkProperties.put("retries", "5");
        kfkProperties.put("linger.ms", "50");




        producer = new KafkaProducer<>(kfkProperties);


        for (long i = 250000000; i < 300000000; i++) {
            String data = generateData(i);
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("my_topic", data);


            if(i % 100000 == 0){
                System.out.println("send 100000" + new Date());
            }
            producer.send(record);
//            producer.send(record, new Callback() {
//                @Override
//                public void onCompletion(RecordMetadata metadata, Exception exception) {
//                    if(exception != null){
//                        System.out.println(exception.getMessage());
//                    }
//                }
//            });




        }


        while(true){
            Thread.sleep(60* 1000l);
        }


    }


    public static String generateData(long i) {

        JSONObject json = new JSONObject();

        String sPrefix = "field_value_";

        json.put("s_1", sPrefix + new Random().nextInt(1000000));
        json.put("s_2", sPrefix + new Random().nextInt(10000));
        json.put("s_3", sPrefix + new Random().nextInt(10000));
        json.put("s_4", sPrefix + new Random().nextInt(10000));
        json.put("s_5", sPrefix + new Random().nextInt(10000));
        json.put("s_6", sPrefix + new Random().nextInt(10000));
        json.put("s_7", sPrefix + new Random().nextInt(10000));
        json.put("s_8", sPrefix + new Random().nextInt(10000));
        json.put("s_9", sPrefix + new Random().nextInt(10000000));
        json.put("s_10", sPrefix + new Random().nextInt(10000));
        json.put("s_11", sPrefix + new Random().nextInt(10000));
        json.put("s_12", sPrefix + new Random().nextInt(10000));
        json.put("s_13", sPrefix + new Random().nextInt(1000));
        json.put("s_14", sPrefix + new Random().nextInt(10000));
        json.put("s_15", sPrefix + new Random().nextInt(1000));
        json.put("s_16", sPrefix + new Random().nextInt(10000000));
        json.put("s_17", sPrefix + new Random().nextInt(1000));
        json.put("s_18", sPrefix + new Random().nextInt(1000));
        json.put("s_19", sPrefix + new Random().nextInt(1000));
        json.put("s_20", sPrefix + new Random().nextInt(1000));

        json.put("v_1", i); // 做分区

        json.put("v_2", new Random().nextInt(1000000000));
        json.put("v_3", new Random().nextInt(1000000000));
        json.put("v_4", new Random().nextFloat() * i);
        json.put("v_5", new Random().nextDouble() * i);


        json.put("part_dt", dates[(int) (i % dates.length)]);


//        System.out.println(json.toJSONString());

        return json.toJSONString();
    }


}
