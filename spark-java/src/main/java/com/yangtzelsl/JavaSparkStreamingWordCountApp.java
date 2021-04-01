package com.yangtzelsl;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.sources.In;
import org.apache.spark.streaming.Seconds$;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class JavaSparkStreamingWordCountApp {

    public static void main(String[] args) throws InterruptedException {

        SparkConf sparkConf = new SparkConf().setAppName("JavaSparkStreamingWordCountApp").setMaster("local[2]");

        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Seconds$.MODULE$.apply(3));

         jsc.checkpoint("file:///d:/checkpoint/");

        JavaReceiverInputDStream socket = jsc.socketTextStream("localhost", 9999);
        System.out.println("==========");
        socket.print();
        System.out.println("==========");

        JavaDStream wordsDS = socket.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator call(String str) throws Exception {
                ArrayList<String> stringArrayList = new ArrayList<>();
                String[] splitArr = str.split(" ");
                for (String s : splitArr) {
                    stringArrayList.add(s);
                }
                return stringArrayList.iterator();
            }
        });

        JavaPairDStream pairDStream = wordsDS.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2 call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairDStream jps = pairDStream.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            @Override
            public Optional<Integer> call(List<Integer> v1, Optional<Integer> v2) throws Exception {
                Integer newCount = v2.isPresent() ? v2.get() : 0;

                System.out.println("old value : " + newCount);
                for (Integer i : v1) {
                    System.out.println("new value : " + i);
                    newCount = newCount + i;
                }
                return Optional.of(newCount);
            }
        });

        JavaPairDStream countDStream = jps.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        countDStream.print();

        jsc.start();
        jsc.awaitTermination();
        jsc.stop();
    }
}
