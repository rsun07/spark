package pers.xiaoming.spark.transformation_and_action;

import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Test;
import pers.xiaoming.spark.SparkCoreDemoTestBase;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class TransformationDemo2 extends SparkCoreDemoTestBase {
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
