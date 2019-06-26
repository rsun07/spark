package pers.xiaoming.spark.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;

import java.util.ArrayList;
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

    // flatMap vs map
    // map: the new RDD has the same number of element as the old RDD
    //      each element is 1 to 1 mapping
    // flatMap: the new RDD could have different number of element as the old RDD
    //         1 to n mapping
    @Test
    public void flatMapDemo() {
        List<String> words = Arrays.asList("w1 w2 w3", "w2 w3", "w3 w1");
        JavaRDD<String> wordsRDD = sc.parallelize(words);

        wordsRDD.flatMap(word -> Arrays.asList(word.split(" ")).iterator())
                .foreach(word -> System.out.print(word + ","));
        System.out.println();

        List<String> words2 = Arrays.asList("w1", "word", "word3");
        JavaRDD<String> wordsRDD2 = sc.parallelize(words2);

        JavaRDD<Integer> integerRDD = wordsRDD2.flatMap(word -> {
            List<Integer> list = new ArrayList<>();
            for (Character c : word.toCharArray()) {
                list.add(c.hashCode());
            }
            return list.iterator();
        });
        integerRDD.foreach(word -> System.out.print(word + ","));
        System.out.println();
    }

    @Test
    public void filterDemo() {
        List<String> words = Arrays.asList("w1", "word", "word3", "a", "wrod");
        JavaRDD<String> wordsRDD = sc.parallelize(words);

        wordsRDD.filter(word -> word.startsWith("word"))
                .foreach(word -> System.out.print(word + ","));
        System.out.println();
    }

    private List<Tuple2<String, String>> employeeWithDepartment = Arrays.asList(
            new Tuple2<>("Dev", "A"),
            new Tuple2<>("HR", "B"),
            new Tuple2<>("Dev", "C"),
            new Tuple2<>("Finance", "D"),
            new Tuple2<>("HR", "E"),
            new Tuple2<>("Dev", "F"),
            new Tuple2<>("HR", "G"),
            new Tuple2<>("HR", "H"),
            new Tuple2<>("Finance", "I"));

    private JavaPairRDD<String, String> employees = sc.<String, String>parallelizePairs(employeeWithDepartment);

    @Test
    public void groupByKeyDemo() {
        employees.groupByKey()
                .foreach(tuple -> {
                    System.out.print(tuple._1 + " : " + tuple._2.toString());
                    System.out.println();
                });
    }

    @Test
    public void reduceByKeyDemo() {
        employees.reduceByKey((s1, s2) -> String.format("(%s + %s)", s1, s2))
                .foreach(tuple -> {
                    System.out.print(tuple._1 + " : " + tuple._2);
                    System.out.println();
                });
    }

    @Test
    public void sortByKeyDemo() {
        employees.sortByKey()
                .foreach(tuple -> System.out.println(tuple._1 + " : " + tuple._2));
    }
}
