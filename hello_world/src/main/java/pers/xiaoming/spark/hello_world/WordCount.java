package pers.xiaoming.spark.hello_world;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class WordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("WordCount");
        conf.setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // RDD stands for "Resilient Distributed Datasets"
        JavaRDD<String> fileContext = sc.textFile(args[0]);

        print(" Step by Step result", stepByStepImpl(fileContext));

        print("Java Lambda result", lambdaImpl(fileContext));
    }

    private static List<Tuple2<String, Integer>> stepByStepImpl(JavaRDD<String> fileContext) {

        JavaRDD<String> words = fileContext.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        JavaPairRDD<String, Integer> wordWithCount = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> result = wordWithCount.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        List<Tuple2<String, Integer>> results = result.collect();

        return results;
    }

    private static List<Tuple2<String, Integer>> lambdaImpl(JavaRDD<String> fileContext) {
        return fileContext
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((int1, int2) -> int1 + int2) // Integer::sum
                .collect();

    }


    private static void print(String jobName, List<Tuple2<String, Integer>> results) {
        System.out.println("Result for " + jobName);
        for (Tuple2<String, Integer> r : results) {
            System.out.println(r);
        }
        System.out.println();
    }
}
