package pers.xiaoming.spark.core;

import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;
import pers.xiaoming.spark.SparkCoreDemoTestBase;

import java.util.Arrays;
import java.util.List;

public class StudentScoreSortDemo extends SparkCoreDemoTestBase {
    @Test
    public void sortByWithLambdaOrder() {
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

        // In this case, only one field could be used to sort
        studentScoreJavaRDD.sortBy(studentScore -> studentScore.total, false, 1)
                .foreach(studentScore -> System.out.println(studentScore));

        studentScoreJavaRDD.sortBy(studentScore -> studentScore.math, false, 1)
                .foreach(studentScore -> System.out.println(studentScore));

        studentScoreJavaRDD.sortBy(studentScore -> studentScore.name, true, 1)
                .foreach(studentScore -> System.out.println(studentScore));
    }

    @Test
    public void sortByImplOrderedInterface() {
        List<OrderedStudentScore> studentScores = Arrays.asList(
                new OrderedStudentScore("A", 150, 80, 70),
                new OrderedStudentScore("B", 200, 100, 100),
                new OrderedStudentScore("C", 150, 80, 70),
                new OrderedStudentScore("D", 150, 70, 80),
                new OrderedStudentScore("E", 150, 75, 75),
                new OrderedStudentScore("F", 170, 100, 70),
                new OrderedStudentScore("G", 170, 90, 80),
                new OrderedStudentScore("H", 170, 80, 90),
                new OrderedStudentScore("I", 190, 90, 100));
        JavaRDD<OrderedStudentScore> studentScoreJavaRDD = sc.parallelize(studentScores);

        studentScoreJavaRDD.sortBy(studentScore -> studentScore, false, 1)
                // cannot use System.out::println, 	- object not serializable (class: java.io.PrintStream, value: java.io.PrintStream@3e850122)
                // .foreach(System.out::println);
                .foreach(studentScore -> System.out.println(studentScore));
    }
}
