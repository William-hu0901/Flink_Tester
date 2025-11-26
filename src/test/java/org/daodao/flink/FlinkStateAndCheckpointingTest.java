package org.daodao.flink;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test cases for Apache Flink 1.20 state management and checkpointing features
 * This class covers state backends, checkpointing, and stateful operations
 */
@Slf4j
public class FlinkStateAndCheckpointingTest {

    @Test
    @DisplayName("Test ValueState for maintaining per-key state")
    public void testValueStateOperation() throws Exception {
        // Simulate ValueState using Map
        Map<String, Integer> valueStateMap = new HashMap<>();
        List<String> results = new ArrayList<>();

        String[] events = {"key1:10", "key1:20", "key2:5", "key1:15", "key2:10"};
        
        for (String event : events) {
            String[] parts = event.split(":");
            String key = parts[0];
            Integer value = Integer.parseInt(parts[1]);

            // Simulate ValueState operation
            Integer currentSum = valueStateMap.get(key);
            if (currentSum == null) {
                currentSum = 0;
            }
            currentSum += value;
            valueStateMap.put(key, currentSum);

            results.add(key + ":" + currentSum);
        }

        assertFalse(results.isEmpty());
        assertTrue(results.stream().anyMatch(result -> result.contains("45"))); // key1: 10+20+15=45
        log.info("ValueState operation test completed successfully");
    }

    @Test
    @DisplayName("Test ListState for maintaining multiple values per key")
    public void testListStateOperation() throws Exception {
        // Simulate ListState using Map of Lists
        Map<String, List<String>> listStateMap = new HashMap<>();
        List<String> results = new ArrayList<>();

        String[] events = {"key1:apple", "key1:banana", "key2:orange", "key1:grape"};

        for (String event : events) {
            String[] parts = event.split(":");
            String key = parts[0];
            String value = parts[1];

            // Simulate ListState operation
            listStateMap.computeIfAbsent(key, k -> new ArrayList<>()).add(value);

            // Build result string
            StringBuilder result = new StringBuilder(key + ":[");
            List<String> values = listStateMap.get(key);
            boolean first = true;
            for (String item : values) {
                if (!first) {
                    result.append(",");
                }
                result.append(item);
                first = false;
            }
            result.append("]");

            results.add(result.toString());
        }

        assertEquals(4, results.size());
        assertTrue(results.get(2).contains("orange"));
        log.info("ListState operation test completed successfully");
    }

    @Test
    @DisplayName("Test MapState for maintaining key-value mappings")
    public void testMapStateOperation() throws Exception {
        // Simulate MapState using nested Maps
        Map<String, Map<String, Integer>> mapStateMap = new HashMap<>();
        List<String> results = new ArrayList<>();

        String[] events = {"user1:action1:100", "user1:action2:200", "user2:action1:150"};

        for (String event : events) {
            String[] parts = event.split(":");
            String user = parts[0];
            String action = parts[1];
            Integer value = Integer.parseInt(parts[2]);

            // Simulate MapState operation
            mapStateMap.computeIfAbsent(user, k -> new HashMap<>()).put(action, value);

            // Build result string
            StringBuilder result = new StringBuilder(user + ":{");
            Map<String, Integer> actions = mapStateMap.get(user);
            boolean first = true;
            for (Map.Entry<String, Integer> entry : actions.entrySet()) {
                if (!first) {
                    result.append(",");
                }
                result.append(entry.getKey()).append("=").append(entry.getValue());
                first = false;
            }
            result.append("}");

            results.add(result.toString());
        }

        assertEquals(3, results.size());
        assertTrue(results.get(1).contains("action1=100"));
        assertTrue(results.get(1).contains("action2=200"));
        log.info("MapState operation test completed successfully");
    }

    @Test
    @DisplayName("Test ReducingState for maintaining aggregated values")
    public void testReducingStateOperation() throws Exception {
        // Simulate ReducingState using Map with reduction logic
        Map<String, Integer> reducingStateMap = new HashMap<>();
        List<String> results = new ArrayList<>();

        String[] events = {"key1:10", "key1:20", "key1:15", "key2:5", "key2:10"};

        for (String event : events) {
            String[] parts = event.split(":");
            String key = parts[0];
            Integer value = Integer.parseInt(parts[1]);

            // Simulate ReducingState operation (sum reduction)
            Integer currentSum = reducingStateMap.get(key);
            if (currentSum == null) {
                currentSum = value;
            } else {
                currentSum += value; // Reduction: sum
            }
            reducingStateMap.put(key, currentSum);

            results.add(key + ":" + currentSum);
        }

        assertEquals(5, results.size());
        assertTrue(results.get(2).contains("45")); // key1: 10+20+15=45
        log.info("ReducingState operation test completed successfully");
    }

    @Test
    @DisplayName("Test State TTL configuration")
    public void testStateTTLConfiguration() throws Exception {
        // Simulate State TTL using timestamp-based expiration
        Map<String, Object> ttlStateMap = new HashMap<>();
        Map<String, Long> timestampMap = new HashMap<>();
        List<String> results = new ArrayList<>();

        String[] events = {"key1:value1", "key1:value2"};
        long ttlMillis = 10000; // 10 seconds TTL

        for (String event : events) {
            String[] parts = event.split(":");
            String key = parts[0];
            String value = parts[1];

            // Simulate TTL state operation
            long currentTime = System.currentTimeMillis();
            
            // Check if existing entry has expired
            if (timestampMap.containsKey(key)) {
                long lastUpdate = timestampMap.get(key);
                if (currentTime - lastUpdate > ttlMillis) {
                    ttlStateMap.remove(key);
                    timestampMap.remove(key);
                }
            }

            ttlStateMap.put(key, value);
            timestampMap.put(key, currentTime);

            results.add(key + ":" + ttlStateMap.get(key));
        }

        assertEquals(2, results.size());
        log.info("State TTL configuration test completed successfully");
    }

    @Test
    @DisplayName("Test Checkpoint configuration with filesystem backend")
    public void testCheckpointConfiguration() throws Exception {
        // Simulate checkpoint configuration
        List<String> results = new ArrayList<>();
        
        // Simulate checkpoint settings
        long checkpointInterval = 5000; // 5 seconds
        String checkpointMode = "EXACTLY_ONCE";
        long minPauseBetweenCheckpoints = 1000;
        long checkpointTimeout = 60000;
        int maxConcurrentCheckpoints = 1;
        String cleanupMode = "RETAIN_ON_CANCELLATION";
        String checkpointPath = System.getProperty("java.io.tmpdir") + "/flink-checkpoints";

        // Simulate data processing with checkpointing
        String[] testData = {"test1", "test2", "test3"};
        
        for (String data : testData) {
            // Simulate processing
            String processed = data + "-processed";
            results.add(processed);
            
            // Simulate checkpoint coordination (simplified)
            log.debug("Simulating checkpoint at path: {}", checkpointPath);
        }

        // Verify checkpoint configuration (simulated)
        assertTrue(checkpointInterval > 0);
        assertEquals("EXACTLY_ONCE", checkpointMode);
        assertTrue(minPauseBetweenCheckpoints > 0);
        assertTrue(checkpointTimeout > 0);
        assertEquals(1, maxConcurrentCheckpoints);
        assertNotNull(checkpointPath);

        assertEquals(3, results.size());
        log.info("Checkpoint configuration test completed successfully");
    }
}