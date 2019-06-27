package pers.xiaoming.spark.core;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.AccumulatorV2;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public class TopNSelector implements Closeable {
    private final JavaSparkContext sc;

    public TopNSelector(JavaSparkContext sc) {
        this.sc = sc;
    }

    public List<Integer> getTopNSortImpl(List<Integer> input, int n) {
        return sc.parallelize(input).sortBy(x -> x, false, 1).take(n);
    }

    public List<Integer> getTopNHeapImpl(List<Integer> input, int n) {
        JavaRDD<Integer> inputRDD = sc.parallelize(input);
        AccumulatorV2<Integer, List<Integer>> heap = new HeapAccumulator(n);

        inputRDD.foreach(heap::add);
        return heap.value();
    }

    @Override
    public void close() throws IOException {
        sc.close();
    }
}
