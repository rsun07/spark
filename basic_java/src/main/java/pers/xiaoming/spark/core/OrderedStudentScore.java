package pers.xiaoming.spark.core;

import scala.Serializable;
import scala.math.Ordered;

public class OrderedStudentScore extends StudentScore implements Ordered<OrderedStudentScore>, Serializable {

    public OrderedStudentScore(String name, int total, int math, int english) {
        super(name, total, math, english);
    }

    @Override
    public String toString() {
        return "OrderedStudentScore{" +
                "name='" + name + '\'' +
                ", total=" + total +
                ", math=" + math +
                ", english=" + english +
                '}';
    }

    @Override
    public int compare(OrderedStudentScore that) {
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
    public boolean $less(OrderedStudentScore that) {
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
    public boolean $less$eq(OrderedStudentScore that) {
        if (this.$less(that)) {
            return true;
        }
        if ((this.total == that.total) && (this.math == that.math)) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater(OrderedStudentScore that) {
        if (this.total > that.total) {
            return true;
        } else if (this.total == that.total) {
            return this.math > that.math;
        }
        return false;
    }

    @Override
    public boolean $greater$eq(OrderedStudentScore that) {
        if (this.$greater(that)) {
            return true;
        }
        if ((this.total == that.total) && (this.math == that.math)) {
            return true;
        }
        return false;
    }

    @Override
    public int compareTo(OrderedStudentScore that) {
        if (this.total - that.total != 0) {
            return this.total - that.total;
        } else {
            return this.math - that.math;
        }
    }
}
