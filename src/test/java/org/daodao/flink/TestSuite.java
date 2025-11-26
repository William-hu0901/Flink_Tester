package org.daodao.flink;

import lombok.extern.slf4j.Slf4j;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * Comprehensive Test Suite for Apache Flink Testing Project
 * 
 * This test suite includes all test classes covering various aspects of Apache Flink:
 * - Core DataStream operations and transformations
 * - State management and checkpointing mechanisms
 * - Table API and SQL functionality
 * - Connectors for external systems
 * - Advanced features like CEP and custom functions
 * - Performance testing and benchmarking
 * 
 * The suite provides comprehensive coverage of Flink's most commonly used features
 * and ensures the stability and correctness of the testing framework.
 */
@Slf4j
@RunWith(Suite.class)
@SuiteClasses({
//     Core functionality tests
    FlinkCoreFeaturesTest.class,
    
//     State management and fault tolerance tests
    FlinkStateAndCheckpointingTest.class,

    // Table API and SQL tests
    FlinkTableAPITest.class,

    // Connector tests for external systems
    FlinkConnectorsTest.class,

    // Advanced features tests
    FlinkAdvancedFeaturesTest.class,

    // Performance and benchmarking tests
    FlinkPerformanceTest.class
})
public class TestSuite {
    // Test suite class - no implementation needed
    // All configuration is done through annotations
    // Static initializer provides logging for test suite composition
    static {
        log.info("=== Apache Flink Test Suite Starting ===");
        log.info("Test Suite Configuration:");
        log.info("  - FlinkCoreFeaturesTest: Testing basic DataStream transformations (map, filter, reduce, window operations)");
        log.info("  - FlinkStateAndCheckpointingTest: Testing state management (ValueState, ListState) and checkpointing mechanisms");
        log.info("  - FlinkTableAPITest: Testing Table API operations, SQL queries, and stream-table conversions");
        log.info("  - FlinkConnectorsTest: Testing file source connectors and sink operations");
        log.info("  - FlinkAdvancedFeaturesTest: Testing async I/O operations, restart strategies, and performance optimizations");
        log.info("  - FlinkPerformanceTest: Testing throughput, latency, and performance monitoring");
        log.info("========================================");
    }

}