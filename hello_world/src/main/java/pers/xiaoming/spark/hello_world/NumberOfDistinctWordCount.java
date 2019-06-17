package pers.xiaoming.spark.hello_world;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class NumberOfDistinctWordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("WordCount");
        conf.setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // RDD stands for "Resilient Distributed Datasets"
        JavaRDD<String> fileContext = sc.textFile(args[0]);

        int count = fileContext
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .distinct()
                .collect()
                .size();

        System.out.println("Total number of distinct word is: " + count);
    }
}
