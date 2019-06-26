package pers.xiaoming.spark.hello_world;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class WordCount {
    private JavaSparkContext sc;
    private JavaRDD<String> fileContext;

    public WordCount(String filePath) {
        SparkConf conf = new SparkConf();
        conf.setAppName("WordCount");
        conf.setMaster("local");

        this.sc = new JavaSparkContext(conf);

        this.fileContext = sc.textFile(filePath);
    }

    public List<Tuple2<String, Integer>> verboseStepByStepImpl() {

        JavaRDD<String> words = fileContext.flatMap(new FlatMapFunction<String, String>() {
            // avoid spark cannot serialize exception
            private static final long serialVersionUID = 1L;

            @Override
            public Iterator<String> call(String s)throws Exception{
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        JavaPairRDD<String, Integer> wordWithCount = words.mapToPair(new PairFunction<String, String, Integer>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> result = wordWithCount.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        return result.collect();
    }

    public List<Tuple2<String, Integer>> lambdaImpl() {
        return fileContext
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((int1, int2) -> int1 + int2) // Integer::sum
                .collect();
    }

    public int numOfDistinctWord() {
        return fileContext.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .distinct()
                .map(word -> 1)
                .reduce(Integer::sum);
    }

    public int totalNumOfWord() {
        return fileContext.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .map(word -> 1)
                .reduce(Integer::sum);
    }

    public void shutdown() {
        this.sc.close();
    }
}
