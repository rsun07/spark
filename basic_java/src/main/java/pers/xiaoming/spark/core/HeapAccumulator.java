package pers.xiaoming.spark.core;

import org.apache.spark.util.AccumulatorV2;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

class HeapAccumulator<T extends Comparable<T>> extends AccumulatorV2<T, List<T>> {
    private final PriorityQueue<T> heap;
    private final int n;

    public HeapAccumulator(int n) {
        this.n = n;
        this.heap = new PriorityQueue<>(n);
    }

    public HeapAccumulator(PriorityQueue<T> heap, int n) {
        this.heap = heap;
        this.n = n;
    }

    @Override
    public boolean isZero() {
        return heap.isEmpty();
    }

    @Override
    public AccumulatorV2<T, List<T>> copy() {
        return new HeapAccumulator<>(this.heap, this.n);
    }

    @Override
    public void reset() {
        heap.clear();
    }

    @Override
    public void add(T v) {
        if (heap.size() < n) {
            heap.add(v);
        } else {
            assert heap.peek() != null;
            if (heap.peek().compareTo(v) < 0) {
                heap.poll();
                heap.add(v);
            }
        }
    }

    @Override
    public void merge(AccumulatorV2<T, List<T>> other) {
        for (T v : other.value()) {
            this.add(v);
        }
    }

    @Override
    public List<T> value() {
        return new ArrayList<>(heap);
    }
}
