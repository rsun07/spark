package pers.xiaoming.spark.core;

import org.apache.spark.util.AccumulatorV2;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

class HeapAccumulator extends AccumulatorV2<Integer, List<Integer>> {
    private final PriorityQueue<Integer> heap;
    private final int n;

    public HeapAccumulator(int n) {
        this.n = n;
        this.heap = new PriorityQueue<>(n);
    }

    public HeapAccumulator(PriorityQueue<Integer> heap, int n) {
        this.heap = heap;
        this.n = n;
    }

    @Override
    public boolean isZero() {
        return heap.isEmpty();
    }

    @Override
    public AccumulatorV2<Integer, List<Integer>> copy() {
        return new HeapAccumulator(this.heap, this.n);
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
        for (int v : other.value()) {
            this.add(v);
        }
    }

    @Override
    public List<Integer> value() {
        return new ArrayList<>(heap);
    }
}
