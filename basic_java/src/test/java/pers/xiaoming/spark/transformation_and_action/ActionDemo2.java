package pers.xiaoming.spark.transformation_and_action;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ActionDemo2 extends DemoBase {
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

    @Test
    public void countByValueWrongUserCaseDemo() {
        List<Tuple2<String, String>> classTranscript = Arrays.asList(
                new Tuple2<>("Mike", "A"),
                new Tuple2<>("James", "B"),
                new Tuple2<>("Marry", "A"),
                new Tuple2<>("Bob", "A"),
                new Tuple2<>("Kate", "B"),
                new Tuple2<>("Jane", "C"),
                new Tuple2<>("Lala", "C"),
                new Tuple2<>("Obia", "B"),
                new Tuple2<>("Siew", "D"));
        JavaPairRDD<String, String> transcriptRDD = sc.<String, String>parallelizePairs(classTranscript);

        // Count by value treat the whole tuple as a value, rather than the value of a tuple/map
        Map<Tuple2<String, String>, Long> scoreDistribution = transcriptRDD.countByValue();
        System.out.println(scoreDistribution);

        // To count number of people with their grade, do the following
        Map<String, Long> result = transcriptRDD.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1)).countByKey();
        System.out.println(result);
    }

    @Test
    public void countByValueDemo() {
        List<String> context = Arrays.asList("w1 w2 w3", "w2 w1");
        JavaRDD<String> contextRDD = sc.parallelize(context);

        Map<String, Long> totalNumOfWords = contextRDD.flatMap(word -> Arrays.asList(word.split(" ")).iterator())
                .countByValue();
        System.out.println(totalNumOfWords);

    }
}
