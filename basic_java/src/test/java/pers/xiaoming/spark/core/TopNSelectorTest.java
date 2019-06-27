package pers.xiaoming.spark.core;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import pers.xiaoming.spark.SparkCoreDemoTestBase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class TopNSelectorTest extends SparkCoreDemoTestBase {
    private static final int N = 5;
    private static TopNSelector topNSelector;

    private static List<Integer> testInput;
    private static List<Integer> expectedResult;

    @BeforeClass
    public static void setupTestData() {
        int start = 0;
        int end = 100;

        topNSelector = new TopNSelector(sc);
        testInput = new ArrayList<>(100);

        for (int i = start; i <= end; i++) {
            testInput.add(i);
        }

        expectedResult = new ArrayList<>(N);

        for (int i = end; i > end - N; i--) {
            expectedResult.add(i);
        }
    }

    @AfterClass
    public static void close() {
        try {
            topNSelector.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void topNSortImplTest() {
        List<Integer> result = topNSelector.getTopNSortImpl(testInput, N);
        Assert.assertEquals(expectedResult, result);
    }

    @Test
    public void topNHeapImplTest() {
        List<Integer> result = topNSelector.getTopNHeapImpl(testInput, N);
        result.sort(Comparator.comparingInt(i -> -i));
        Assert.assertEquals(expectedResult, result);
    }
}
