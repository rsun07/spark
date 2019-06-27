package pers.xiaoming.spark.core;

import scala.Serializable;
import scala.math.Ordered;

public class StudentScore implements Ordered<StudentScore>, Serializable {
    private String name;
    private int total;
    private int math;
    private int english;

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

    @Override
    public int compare(StudentScore that) {
        if (this.total - that.total != 0) {
            return this.total - that.total;
        } else {
            if (this.math - that.math != 0) {
                return this.math - that.math;
            } else {
                return this.english - that.english;
            }
        }
    }

    @Override
    public boolean $less(StudentScore that) {
        if (this.total < that.total) {
            return true;
        } else if (this.total == that.total) {
            return this.math < that.math;

            // here could be simplified because if total equals, math equals, english must equals
            // else if (this.math == that.math) {
            //    return this.english < that.english;
            // }
        }
        return false;
    }

    @Override
    public boolean $less$eq(StudentScore that) {
        if (this.$less(that)) {
            return true;
        }
        if ((this.total == that.total) && (this.math == that.math)) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater(StudentScore that) {
        if (this.total > that.total) {
            return true;
        } else if (this.total == that.total) {
            return this.math > that.math;
        }
        return false;
    }

    @Override
    public boolean $greater$eq(StudentScore that) {
        if (this.$greater(that)) {
            return true;
        }
        if ((this.total == that.total) && (this.math == that.math)) {
            return true;
        }
        return false;
    }

    @Override
    public int compareTo(StudentScore that) {
        if (this.total - that.total != 0) {
            return this.total - that.total;
        } else {
            return this.math - that.math;
        }
    }
}
