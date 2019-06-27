package pers.xiaoming.spark.core;

import org.apache.spark.util.AccumulatorV2;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public class HeapAccumulator extends AccumulatorV2<Integer, List<Integer>> {
    private final PriorityQueue<Integer> heap;
    private final int n;

    public HeapAccumulator(int n) {
        this.n = n;
        this.heap = new PriorityQueue<>(n, Comparator.comparingInt(i -> i));
    }

    @Override
    public boolean isZero() {
        return heap.isEmpty();
    }

    // not in use
    @Override
    public AccumulatorV2<Integer, List<Integer>> copy() {
        return this;
    }

    @Override
    public void reset() {
        heap.clear();
    }

    @Override
    public void add(Integer v) {
        if (heap.size() < n) {
            heap.add(v);
        } else {
            if (heap.peek() < v) {
                heap.poll();
                heap.add(v);
            }
        }
    }

    @Override
    public void merge(AccumulatorV2<Integer, List<Integer>> other) {
    }

    @Override
    public List<Integer> value() {
        return Arrays.asList(heap.toArray(new Integer[0]));
    }
}
