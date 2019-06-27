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

    public <T> List<T> getTopNSortImpl(List<T> input, int n) {
        return sc.parallelize(input).sortBy(x -> x, false, 1).take(n);
    }

    public <T> List<T>getTopNHeapImpl(List<T> input, int n) {
        JavaRDD<T> inputRDD = sc.parallelize(input);

        final AccumulatorV2<T, List<T>> heap = new HeapAccumulator(n);

        sc.sc().register(heap, "Heap");

        inputRDD.foreach(heap::add);
        return heap.value();
    }

    @Override
    public void close() throws IOException {
        sc.close();
    }
}
