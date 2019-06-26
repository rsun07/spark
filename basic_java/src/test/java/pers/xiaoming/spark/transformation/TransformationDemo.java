package pers.xiaoming.spark.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class TransformationDemo {
    private static JavaSparkContext sc;

    @BeforeClass
    public static void setup() {
        SparkConf config = new SparkConf().setAppName("TransformationDemo").setMaster("local");
        sc = new JavaSparkContext(config);
    }

    @AfterClass
    public static void close() {
        sc.close();
    }

    @Test
    public void mapDemo() {
        List<String> words = Arrays.asList("w1", "w2", "w3");
        JavaRDD<String> wordsRDD = sc.parallelize(words);

        wordsRDD.map(word -> "_" + word + "_")
                .foreach(word -> System.out.print(word + ", "));
        System.out.println();

        wordsRDD.map(String::toUpperCase)
                .foreach(word -> System.out.print(word + ", "));
        System.out.println();

        Random random = new Random();
        wordsRDD.map(word -> random.nextInt())
                .foreach(word -> System.out.print(word + ", "));
        System.out.println();

        wordsRDD.map(word -> new Tuple2<>(word, 1))
                .foreach(word -> System.out.print(word + ", "));
        System.out.println();
    }
}
