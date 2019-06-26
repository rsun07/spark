package pers.xiaoming.spark.transformation_and_action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class DemoBase {
    static JavaSparkContext sc;

    @BeforeClass
    public static void setup() {
        SparkConf config = new SparkConf().setAppName("BasicDemo").setMaster("local");
        sc = new JavaSparkContext(config);
    }

    @AfterClass
    public static void close() {
        sc.close();
    }
}
