package pers.xiaoming.spark.core;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import org.junit.Ignore;
import org.junit.Test;
import pers.xiaoming.spark.SparkCoreDemoTestBase;

// because it is easy to impl, so no scala version, java impl only
@Ignore
public class PersistDemo extends SparkCoreDemoTestBase {
    @Test
    public void demo() {

        // cache() is a special case of persist(), it call the default persist()
        // must call cache() or persist() when loading the file
        JavaRDD<String> context = sc.textFile("hdfs://spark1:9000/spark.txt").cache();

        // it doesn't work
        // context.cache();

        // For detail of StorageLevel choice, refer to https://spark.apache.org/docs/latest/rdd-programming-guide.html
        JavaRDD<String> context2 = sc.textFile("hdfs://spark1:9000/spark.txt").persist(StorageLevel.MEMORY_ONLY());
    }
}
