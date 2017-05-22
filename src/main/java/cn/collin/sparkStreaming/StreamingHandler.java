package cn.collin.sparkStreaming;

import com.google.common.collect.Lists;
import kafka.producer.KeyedMessage;
import net.sf.json.JSONObject;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by collin on 17-5-12.
 */
public class StreamingHandler {
    public static RealTimePost realTimePost = new RealTimePost();
    public static String url = "http://localhost:8084/transData";
    static List list = new ArrayList();
    static JSONObject jsonObject = new JSONObject();
    static JSONObject results = new JSONObject();
    static long start = 0;
    static long end = 0;
//    static int j = 0;
    public static void main(String[] args) {
        final Pattern SPACE = Pattern.compile(" ");
        String zkQuorum = "localhost:2181";
        String group = "SparkConsumer";
        int numThreads = 2;
        // Create the context with 2 seconds batch size
        SparkConf sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(4000));

        Map<String, Integer> topicMap = new HashMap<>();
        topicMap.put("topic02", numThreads);


        JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, zkQuorum, group, topicMap);
        JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> stringStringTuple2) throws Exception {
                return stringStringTuple2._2();
            }
        });

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(SPACE.split(s)).iterator();
            }
        });


        JavaPairDStream<Long, String> composeData = words.mapToPair(
                new PairFunction<String, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(String s) throws Exception {
//                        System.out.println("j" + j++);
                        jsonObject = JSONObject.fromObject(s);
                        Long  key = jsonObject.getLong("id");
                        if (start == 0 && end == 0) {
                            start = key;
                            end = key;
                        } else if (key > end) {
                            end = key;
                        } else if (key < start) {
                            start = key;
                        }
                        return new Tuple2<>(key, jsonObject.toString());
                    }
        }).reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String s, String s2) throws Exception {
                JSONObject j1 = JSONObject.fromObject(s);
                JSONObject j2 = JSONObject.fromObject(s2);
                JSONObject j3 = new JSONObject();
                if (j1.getInt("dataType") == 0) {
                    Long interval = j2.getLong("timestamp") - j1.getLong("timestamp");
                    j3.put("startTime", j1.getLong("id"));
                    j3.put("interval", interval);
                    j3.put("serverId", j1.getString("serverId"));
                } else {
                    Long interval = j1.getLong("timestamp") - j2.getLong("timestamp");
                    j3.put("startTime", j1.getLong("id"));
                    j3.put("interval", interval);
                    j3.put("serverId", j1.getString("serverId"));
                }
                list.add(j3);
                return j3.toString();
            }
        });

        JavaDStream<String> temp = composeData.flatMap(new FlatMapFunction<Tuple2<Long, String>, String>() {
            @Override
            public Iterator<String> call(Tuple2<Long, String> longStringTuple2) throws Exception {
                return Arrays.asList(longStringTuple2._2()).iterator();
            }
        }).reduce(new Function2<String, String, String>() {
            @Override
            public String call(String s, String s2) throws Exception {
                return s;
            }
        });

        JavaPairDStream<Integer, String> finalData = temp.mapToPair(new PairFunction<String, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(String s) throws Exception {
//                System.out.println("start:"+start);
                System.out.println("listSize:"+list.size());
                System.out.println(start);
                System.out.println(end);
                if (start != end && start !=0 && end != 0) {
                    list.add(start);
                    list.add(end);
                }
                realTimePost.sendPost(url, list.toString());
                list.clear();
                start = 0;
                end = 0;
                return new Tuple2<>(1, s);
            }
        });
        finalData.print();


        /*words.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            public void call(final JavaRDD<String> v1) throws Exception {

                v1.foreachPartition(new VoidFunction<Iterator<String>>() {

                    public void call(Iterator<String> stringIterator) throws Exception {

                        // 得到单例的 kafka producer
                        // (是在每个executor上单例，job中producer数目与executor数目相同，并行输出，性能较好)
//                        KafkaProducer kafkaProducer = KafkaProducer.getInstance(brokerListBroadcast.getValue());

                        // 批量发送 推荐
//                        List<KeyedMessage<String, String>> messageList = Lists.newArrayList();
                        List list = new ArrayList();
                        JSONObject jsonObject = new JSONObject();
                        while (stringIterator.hasNext()) {
                            list.add(stringIterator.next());
//                            jsonObject.put()
                            *//*System.out.println("fuck");
                            System.out.println(stringIterator.next());*//*
//                            messageList.add(new KeyedMessage<String, String>(topicBroadcast.getValue(), stringIterator.next()));
                        }
                        System.out.println(list.size());
//                        kafkaProducer.send(messageList);

                        // 逐条发送
            *//*
            while (stringIterator.hasNext()) {
              kafkaProducer.send(new KeyedMessage<String, String>(topicBroadcast.getValue(), stringIterator.next()));
            }
            *//*
                    }

                });

            }
        });*/

        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
