package pers.xiaoming.spark.core;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.junit.Test;
import pers.xiaoming.spark.SparkCoreDemoTestBase;

import java.util.Arrays;
import java.util.List;

public class VariableDemo extends SparkCoreDemoTestBase {
    private JavaRDD<Integer> numsRDD = sc.parallelize(Arrays.asList(1, 2, 3));

    @Test
    public void BroadcastVariableDemo() {
        final int base = 2;
        final Broadcast<Integer> baseBroadcast = sc.broadcast(3);

        // In this case, the integer 'base' will be copied to every node and every executor's tasks
        // If the object need to be shared is large, then it's a wast of resources
        List<Integer> result = numsRDD.filter(x -> x % base == 0).collect();

        // In this case, the 'baseBroadcast' will be copied only once in each node
        // And shared by all executor's tasks
        List<Integer> result2 = numsRDD.filter(x -> x % baseBroadcast.getValue() == 0).collect();
    }

    @Test
    public void AccumulatorVariableDemo() {
        final MyAccumulator sum = new MyAccumulator();
        sc.sc().register(sum, "SUM");

        numsRDD.foreach(x -> sum.add(x.longValue()));

        System.out.println(sum.value());
    }
}
