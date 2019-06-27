package pers.xiaoming.spark.core;

import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;
import pers.xiaoming.spark.SparkCoreDemoTestBase;

import java.util.Arrays;
import java.util.List;

public class StudentScoreSortDemo extends SparkCoreDemoTestBase {
    private List<OrderedStudentScore> studentScores = Arrays.asList(
            new OrderedStudentScore("A", 150, 80, 70),
            new OrderedStudentScore("B", 200, 100, 100),
            new OrderedStudentScore("C", 150, 80, 70),
            new OrderedStudentScore("D", 150, 70, 80),
            new OrderedStudentScore("E", 150, 75, 75),
            new OrderedStudentScore("F", 170, 100, 70),
            new OrderedStudentScore("G", 170, 90, 80),
            new OrderedStudentScore("H", 170, 80, 90),
            new OrderedStudentScore("I", 190, 90, 100));
    private JavaRDD<OrderedStudentScore> studentScoreJavaRDD = sc.parallelize(studentScores);

    @Test
    public void sortByWithLambdaOrder() {
        // In this case, only one field could be used to sort
        studentScoreJavaRDD.sortBy(studentScore -> studentScore.total, false, 1)
                .foreach(studentScore -> System.out.println(studentScore));

        studentScoreJavaRDD.sortBy(studentScore -> studentScore.math, false, 1)
                .foreach(studentScore -> System.out.println(studentScore));
    }

    @Test
    public void sortByImplOrderedInterface() {


        studentScoreJavaRDD.sortBy(studentScore -> studentScore, false, 1)
                // cannot use System.out::println, 	- object not serializable (class: java.io.PrintStream, value: java.io.PrintStream@3e850122)
                // .foreach(System.out::println);
                .foreach(studentScore -> System.out.println(studentScore));
    }
}
