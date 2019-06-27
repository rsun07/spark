package pers.xiaoming.spark.transformation_and_action;

import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;
import pers.xiaoming.spark.SparkCoreDemoTestBase;

import java.util.Arrays;
import java.util.List;

public class ActionDemo extends SparkCoreDemoTestBase {
    private List<String> words = Arrays.asList("A", "B", "C", "D", "E");
    private JavaRDD<String> wordsRDD = sc.parallelize(words);

    private List<Integer> nums = Arrays.asList(1, 2, 3, 4, 5);
    private JavaRDD<Integer> numsRDD = sc.parallelize(nums);

    @Test
    public void reduceDemo() {
        String result = wordsRDD.reduce((s1, s2) -> String.format("(%s + %s)", s1, s2));
        System.out.println(result);

        int sum = numsRDD.reduce(Integer::sum);
        System.out.println(sum);
    }

    @Test
    public void collectDemo() {
        List<String> collectedWords = wordsRDD.collect();
        System.out.println(collectedWords);

        List<Integer> collectedNums = numsRDD.collect();
        System.out.println(collectedNums);
    }

    @Test
    public void countDemo() {
        long numOfWords = wordsRDD.count();
        System.out.println(numOfWords);

        long numOfNums = numsRDD.count();
        System.out.println(numOfNums);
    }

    @Test
    public void takeDemo() {
        List<String> first3Words = wordsRDD.take(3);
        System.out.println(first3Words);

        List<Integer> first3Nums = numsRDD.take(3);
        System.out.println(first3Nums);
    }

    @Test
    public void saveAsTextFileDemo() {
        wordsRDD.saveAsTextFile("hdfs://localhost:9000/words.txt");

        numsRDD.saveAsTextFile("hdfs://localhost:9000/nums.txt");
    }
}
