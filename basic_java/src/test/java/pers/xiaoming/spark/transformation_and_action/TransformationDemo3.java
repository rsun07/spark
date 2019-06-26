package pers.xiaoming.spark.transformation_and_action;

import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class TransformationDemo3 extends DemoBase {
    private List<Tuple2<Integer, String>> listOfIdToName = Arrays.asList(
            new Tuple2<>(1, "A"),
            new Tuple2<>(2, "B"),
            new Tuple2<>(3, "C"));

    private List<Tuple2<Integer, String>> listOfIdToEmail = Arrays.asList(
            new Tuple2<>(1, "A@test.c"),
            new Tuple2<>(2, "B@test.c"),
            new Tuple2<>(3, "C@test.c"));

    private List<Tuple2<Integer, String>> listOfIdToEmails = Arrays.asList(
            new Tuple2<>(1, "A1@test.c"),
            new Tuple2<>(2, "B1@test.c"),
            new Tuple2<>(3, "C1@test.c"),
            new Tuple2<>(1, "A2@test.c"),
            new Tuple2<>(2, "B2@test.c"),
            new Tuple2<>(3, "C2@test.c"));

    private JavaPairRDD<Integer, String> idToNameRDD = sc.<Integer, String>parallelizePairs(listOfIdToName);
    private JavaPairRDD<Integer, String> idToEmailRDD = sc.<Integer, String>parallelizePairs(listOfIdToEmail);
    private JavaPairRDD<Integer, String> idToEmailsRDD = sc.<Integer, String>parallelizePairs(listOfIdToEmails);

    @Test
    public void joinDemo() {
        JavaPairRDD<Integer, Tuple2<String, String>> idWithNameAndEmail = idToNameRDD.<String>join(idToEmailRDD);
        idWithNameAndEmail.foreach(record -> System.out.println(record._1 + " : " + record._2._1 + ", " + record._2._2));

        JavaPairRDD<Integer, Tuple2<String, String>> idWithNameAndEmails = idToNameRDD.<String>join(idToEmailsRDD);
        idWithNameAndEmails.foreach(record -> System.out.println(record._1 + " : " + record._2._1 + ", " + record._2._2));
    }

    @Test
    public void cogroupDemo() {
        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<String>>> idWithNameAndEmail =
                idToNameRDD.<String>cogroup(idToEmailRDD);
        idWithNameAndEmail.foreach(record -> System.out.println(record._1 + " : " + record._2._1 + ", " + record._2._2));

        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<String>>> idWithNameAndEmails =
                idToNameRDD.<String>cogroup(idToEmailsRDD);
        idWithNameAndEmails.foreach(record -> System.out.println(record._1 + " : " + record._2._1 + ", " + record._2._2));
    }
}
