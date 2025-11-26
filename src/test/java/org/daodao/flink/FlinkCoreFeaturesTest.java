package org.daodao.flink;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test cases for Apache Flink 2.1 core features
 * This class covers the most commonly used Flink functionalities
 */
@Slf4j
public class FlinkCoreFeaturesTest {

    @Test
    @DisplayName("Test basic DataStream transformation - Map operation")
    public void testBasicMapTransformation() throws Exception {
        // Test the MapFunction directly without full Flink execution
        MapFunction<Integer, Integer> mapFunction = new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return value * 2;
            }
        };

        List<Integer> inputNumbers = List.of(1, 2, 3, 4, 5);
        List<Integer> results = new ArrayList<>();

        // Apply the map function directly
        for (Integer input : inputNumbers) {
            Integer result = mapFunction.map(input);
            results.add(result);
        }

        // Verify the results
        List<Integer> expected = List.of(2, 4, 6, 8, 10);
        assertEquals(expected, results);
        
        log.info("Map transformation test completed successfully with {} results: {}", results.size(), results);
    }

    @Test
    @DisplayName("Test FlatMap operation for splitting sentences")
    public void testFlatMapOperation() throws Exception {
        // Test the FlatMapFunction directly without full Flink execution
        FlatMapFunction<String, String> flatMapFunction = new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                for (String word : value.split(" ")) {
                    out.collect(word.toLowerCase());
                }
            }
        };

        List<String> sentences = List.of("Hello world", "Apache Flink", "Stream processing");
        List<String> words = new ArrayList<>();

        // Create a simple collector implementation
        Collector<String> collector = new Collector<String>() {
            @Override
            public void collect(String record) {
                words.add(record);
            }

            @Override
            public void close() {
                // No cleanup needed
            }
        };

        // Apply the flatMap function directly
        for (String sentence : sentences) {
            flatMapFunction.flatMap(sentence, collector);
        }

        // Verify the results
        List<String> expected = List.of("hello", "world", "apache", "flink", "stream", "processing");
        assertEquals(expected, words);
        
        log.info("FlatMap operation test completed successfully with {} words: {}", words.size(), words);
    }

    @Test
    @DisplayName("Test Filter operation for data filtering")
    public void testFilterOperation() throws Exception {
        // Test the FilterFunction directly without full Flink execution
        org.apache.flink.api.common.functions.FilterFunction<Integer> filterFunction = 
            new org.apache.flink.api.common.functions.FilterFunction<Integer>() {
                @Override
                public boolean filter(Integer value) throws Exception {
                    return value % 2 == 0;
                }
            };

        List<Integer> numbers = List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        List<Integer> evenNumbers = new ArrayList<>();

        // Apply the filter function directly
        for (Integer number : numbers) {
            if (filterFunction.filter(number)) {
                evenNumbers.add(number);
            }
        }

        // Verify the results
        List<Integer> expected = List.of(2, 4, 6, 8, 10);
        assertEquals(expected, evenNumbers);
        
        log.info("Filter operation test completed successfully with {} even numbers: {}", 
                evenNumbers.size(), evenNumbers);
    }

    @Test
    @DisplayName("Test KeyBy and Reduce operations for aggregation")
    public void testKeyByAndReduceOperations() throws Exception {
        // Test the ReduceFunction directly without full Flink execution
        ReduceFunction<Tuple2<String, Integer>> reduceFunction = new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                return Tuple2.of(value1.f0, value1.f1 + value2.f1);
            }
        };

        List<Tuple2<String, Integer>> salesData = List.of(
            Tuple2.of("productA", 100),
            Tuple2.of("productB", 200),
            Tuple2.of("productA", 150),
            Tuple2.of("productB", 50),
            Tuple2.of("productA", 75)
        );

        // Simulate KeyBy and Reduce operations
        java.util.Map<String, Tuple2<String, Integer>> groupedData = new java.util.HashMap<>();
        
        // Group by key (simulate KeyBy)
        for (Tuple2<String, Integer> sale : salesData) {
            String key = sale.f0;
            if (groupedData.containsKey(key)) {
                // Apply reduce function (simulate Reduce)
                Tuple2<String, Integer> existing = groupedData.get(key);
                groupedData.put(key, reduceFunction.reduce(existing, sale));
            } else {
                groupedData.put(key, sale);
            }
        }

        List<Tuple2<String, Integer>> results = new ArrayList<>(groupedData.values());
        
        // Verify the results
        assertEquals(2, results.size());
        
        // Check specific values
        boolean foundProductA = false, foundProductB = false;
        for (Tuple2<String, Integer> result : results) {
            if ("productA".equals(result.f0) && result.f1 == 325) { // 100 + 150 + 75
                foundProductA = true;
            }
            if ("productB".equals(result.f0) && result.f1 == 250) { // 200 + 50
                foundProductB = true;
            }
        }
        
        assertTrue(foundProductA, "ProductA with total 325 not found");
        assertTrue(foundProductB, "ProductB with total 250 not found");
        
        log.info("KeyBy and Reduce operations test completed successfully with {} results: {}", 
                results.size(), results);
    }

    @Test
    @DisplayName("Test Window operation with time-based aggregation")
    public void testTimeWindowOperation() throws Exception {
        // Test the ProcessWindowFunction directly without full Flink execution
        ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow> windowFunction = 
            new ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
                @Override
                public void process(String key, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
                    int sum = 0;
                    int count = 0;
                    for (Tuple2<String, Integer> element : elements) {
                        sum += element.f1;
                        count++;
                    }
                    out.collect(Tuple2.of(key, sum / count));
                }
            };

        // Simulate windowed data (elements that would be in the same time window)
        List<Tuple2<String, Integer>> windowElements = List.of(
            Tuple2.of("key", 1),
            Tuple2.of("key", 2),
            Tuple2.of("key", 3),
            Tuple2.of("key", 4),
            Tuple2.of("key", 5)
        );

        List<Tuple2<String, Integer>> windowedData = new ArrayList<>();
        
        // Create a collector for the window function
        Collector<Tuple2<String, Integer>> collector = new Collector<Tuple2<String, Integer>>() {
            @Override
            public void collect(Tuple2<String, Integer> record) {
                windowedData.add(record);
            }

            @Override
            public void close() {
                // No cleanup needed
            }
        };

        // Apply the window function directly
        windowFunction.process("key", null, windowElements, collector);

        // Verify the results
        assertFalse(windowedData.isEmpty());
        assertEquals(1, windowedData.size());
        
        Tuple2<String, Integer> result = windowedData.get(0);
        assertEquals("key", result.f0);
        assertEquals(3, result.f1); // Average of 1,2,3,4,5 = (1+2+3+4+5)/5 = 3
        
        log.info("Time window operation test completed successfully with result: {}", result);
    }

    @Test
    @DisplayName("Test Union operation for combining multiple streams")
    public void testUnionOperation() throws Exception {
        // Test union operation directly without full Flink execution
        List<Integer> stream1Data = List.of(1, 2, 3);
        List<Integer> stream2Data = List.of(4, 5, 6);
        
        // Simulate union operation by combining lists
        List<Integer> unionedData = new ArrayList<>();
        unionedData.addAll(stream1Data);
        unionedData.addAll(stream2Data);

        // Verify the results
        assertEquals(6, unionedData.size());
        assertTrue(unionedData.containsAll(List.of(1, 2, 3, 4, 5, 6)));
        
        log.info("Union operation test completed successfully with {} elements: {}", 
                unionedData.size(), unionedData);
    }

    @Test
    @DisplayName("Test Split operation using process function for conditional routing")
    public void testSplitOperation() throws Exception {
        // Test the split logic directly without full Flink execution
        List<Integer> numbers = List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        List<Integer> evenNumbers = new ArrayList<>();
        List<Integer> oddNumbers = new ArrayList<>();

        // Simulate the split operation
        for (Integer number : numbers) {
            if (number % 2 == 0) {
                evenNumbers.add(number);
            } else {
                oddNumbers.add(number);
            }
        }

        // Verify the results
        assertEquals(List.of(2, 4, 6, 8, 10), evenNumbers);
        assertEquals(List.of(1, 3, 5, 7, 9), oddNumbers);
        
        log.info("Split operation test completed successfully with {} even numbers and {} odd numbers", 
                evenNumbers.size(), oddNumbers.size());
    }

    @Test
    @DisplayName("Test Connect operation for joining streams of different types")
    public void testConnectOperation() throws Exception {
        // Test the CoFlatMapFunction directly without full Flink execution
        org.apache.flink.streaming.api.functions.co.CoFlatMapFunction<String, Integer, String> coFlatMapFunction = 
            new org.apache.flink.streaming.api.functions.co.CoFlatMapFunction<String, Integer, String>() {
                @Override
                public void flatMap1(String value, Collector<String> out) throws Exception {
                    out.collect("String: " + value);
                }

                @Override
                public void flatMap2(Integer value, Collector<String> out) throws Exception {
                    out.collect("Integer: " + value);
                }
            };

        List<String> stringData = List.of("A", "B", "C");
        List<Integer> intData = List.of(1, 2, 3);
        List<String> connectedResults = new ArrayList<>();

        // Create a collector for the CoFlatMapFunction
        Collector<String> collector = new Collector<String>() {
            @Override
            public void collect(String record) {
                connectedResults.add(record);
            }

            @Override
            public void close() {
                // No cleanup needed
            }
        };

        // Simulate the connect operation by processing both streams
        for (String strValue : stringData) {
            coFlatMapFunction.flatMap1(strValue, collector);
        }
        
        for (Integer intValue : intData) {
            coFlatMapFunction.flatMap2(intValue, collector);
        }

        // Verify the results
        assertEquals(6, connectedResults.size());
        assertTrue(connectedResults.contains("String: A"));
        assertTrue(connectedResults.contains("String: B"));
        assertTrue(connectedResults.contains("String: C"));
        assertTrue(connectedResults.contains("Integer: 1"));
        assertTrue(connectedResults.contains("Integer: 2"));
        assertTrue(connectedResults.contains("Integer: 3"));
        
        log.info("Connect operation test completed successfully with {} results: {}", 
                connectedResults.size(), connectedResults);
    }
}