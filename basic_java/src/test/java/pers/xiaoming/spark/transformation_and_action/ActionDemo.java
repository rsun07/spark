package pers.xiaoming.spark.transformation_and_action;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ActionDemo extends DemoBase {
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
        List<String> top3Words = wordsRDD.take(3);
        System.out.println(top3Words);

        List<Integer> top3Nums = numsRDD.take(3);
        System.out.println(top3Nums);
    }

    @Test
    public void saveAsTextFileDemo() {
        wordsRDD.saveAsTextFile("hdfs://localhost:9000/words.txt");

        numsRDD.saveAsTextFile("hdfs://localhost:9000/nums.txt");
    }

    @Test
    public void countByKeyDemo() {
        List<Tuple2<String, String>> employeeWithDepartment = Arrays.asList(
                new Tuple2<>("Dev", "A"),
                new Tuple2<>("HR", "B"),
                new Tuple2<>("Dev", "C"),
                new Tuple2<>("Finance", "D"),
                new Tuple2<>("HR", "E"),
                new Tuple2<>("Dev", "F"),
                new Tuple2<>("HR", "G"),
                new Tuple2<>("HR", "H"),
                new Tuple2<>("Finance", "I"));
        JavaPairRDD<String, String> employeesRDD = sc.<String, String>parallelizePairs(employeeWithDepartment);

        Map<String, Long> numOfEmployeeEachDep = employeesRDD.countByKey();
        System.out.println(numOfEmployeeEachDep);
    }
}
