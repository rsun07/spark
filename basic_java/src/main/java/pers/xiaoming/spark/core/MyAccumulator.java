package pers.xiaoming.spark.core;

import org.apache.spark.util.AccumulatorV2;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

class MyAccumulator extends AccumulatorV2<Long, Long> implements Serializable {
    private static final long serialVersionUID = 1L;

    private AtomicLong value;

    public MyAccumulator() {
        value = new AtomicLong(0);
    }

    public MyAccumulator(long value) {
        this.value = new AtomicLong(value);
    }

    @Override
    public boolean isZero() {
        return value.get() == 0;
    }

    @Override
    public AccumulatorV2<Long, Long> copy() {
        return new MyAccumulator(value.get());
    }

    @Override
    public void reset() {
        value.set(0L);
    }

    @Override
    public void add(Long v) {
        value.addAndGet(v);
    }

    @Override
    public void merge(AccumulatorV2<Long, Long> other) {
        value.addAndGet(other.value());
    }

    @Override
    public Long value() {
        return value.get();
    }
}
