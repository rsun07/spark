package pers.xiaoming.spark.core;

import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;
import pers.xiaoming.spark.SparkCoreDemoTestBase;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class StudentScoreSortDemo extends SparkCoreDemoTestBase {
    @Test
    public void test() {
        List<StudentScore> studentScores = Arrays.asList(
                new StudentScore("A", 150, 80, 70),
                new StudentScore("B", 200, 100, 100),
                new StudentScore("C", 150, 80, 70),
                new StudentScore("D", 150, 70, 80),
                new StudentScore("E", 150, 75, 75),
                new StudentScore("F", 170, 100, 70),
                new StudentScore("G", 170, 90, 80),
                new StudentScore("H", 170, 80, 90),
                new StudentScore("I", 190, 90, 100));

        JavaRDD<StudentScore> studentScoreJavaRDD = sc.parallelize(studentScores);

        studentScoreJavaRDD.mapToPair(studentScore -> new Tuple2<>(studentScore, studentScore.getName()))
                .sortByKey(false).foreach(tuple -> System.out.println(tuple._1));
    }
}
