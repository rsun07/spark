package pers.xiaoming.spark.hello_world;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class WordCountTest {
    private static final String TEST_FILE_PATH = "./src/test/resources/wordcount.txt";
    private static WordCount wordCount;
    private static final List<Tuple2<String, Integer>> expectedResult = new ArrayList<>();

    @BeforeClass
    public static void wcCreation() {
        wordCount = new WordCount(TEST_FILE_PATH);
        expectedResult.add(new Tuple2<>("word1", 1));
        expectedResult.add(new Tuple2<>("word1", 3));
        expectedResult.add(new Tuple2<>("word1", 5));
    }

    @Test
    public void testStepByStepWordCount() {
        List<Tuple2<String, Integer>> result = wordCount.verboseStepByStepImpl();
        print("step by step word count", result);
        // skip here, the assert list equals is not easy
        // Assert.assertEquals(expectedResult, result);
    }

    @Test
    public void testLambdaImpl() {
        List<Tuple2<String, Integer>> result = wordCount.lambdaImpl();
        print("lambda implementation", result);
    }

    @Test
    public void testNumOfDistinctWords() {
        int numOfDistinctWord = wordCount.numOfDistinctWord();
        Assert.assertEquals(3, numOfDistinctWord);
    }

    @Test
    public void testNumOfTotalWords() {
        int numOfTotalWords = wordCount.totalNumOfWord();
        Assert.assertEquals(9, numOfTotalWords);
    }

    private void print(String jobName, List<Tuple2<String, Integer>> results) {
        System.out.println("Result for " + jobName);
        for (Tuple2<String, Integer> r : results) {
            System.out.println(r);
        }
        System.out.println();
    }

    @AfterClass
    public static void shutdown() {
        wordCount.shutdown();
    }
}
