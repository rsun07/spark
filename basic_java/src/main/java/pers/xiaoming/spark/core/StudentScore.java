package pers.xiaoming.spark.core;

import java.io.Serializable;

public class StudentScore implements Serializable {
    String name;
    int total;
    int math;
    int english;

    public StudentScore(String name, int total, int math, int english) {
        this.name = name;
        this.total = total;
        this.math = math;
        this.english = english;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "StudentScore{" +
                "name='" + name + '\'' +
                ", total=" + total +
                ", math=" + math +
                ", english=" + english +
                '}';
    }
}
