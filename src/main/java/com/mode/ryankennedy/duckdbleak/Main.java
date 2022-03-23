package com.mode.ryankennedy.duckdbleak;

import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.duckdb.DuckDBConnection;

import java.sql.DriverManager;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A simple driver program that uses the org.duckdb:duckdb_jdbc Maven package to
 * see if we can reproduce what looks like a memory leak in our production application.
 */
@SuppressWarnings("UnstableApiUsage")
public class Main {
    // The maximum number of tables to keep in DuckDB. Above this amount we start dropping tables
    // to remain at this limit.
    private static final long RESIDENT_TABLE_LIMIT = 100;

    // How long the test should run.
    private static final Duration DEFAULT_TEST_DURATION = Duration.ofHours(1);

    // How rapidly we should create and ingest new tables.
    private static final RateLimiter INGESTION_RATE = RateLimiter.create(5);

    // How long to wait for the thread pool to shut down when quitting.
    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(5);

    public static void main(String[] args) throws Exception {
        // Determine how long the test is going to run.
        Duration testDuration = getTestDuration(args);

        // A thread pool to process all table creations and ingestions in the background.
        ExecutorService threadPool = Executors.newFixedThreadPool(
                4, new ThreadFactoryBuilder().setNameFormat("background-%d").setDaemon(true).build());

        // Do the date math to figure out when the test should shut down.
        ZonedDateTime startingAt = ZonedDateTime.now();
        ZonedDateTime endingAt = startingAt.plus(testDuration);

        // Keep a monotonic counter to track each table creation and ingestion.
        AtomicLong counter = new AtomicLong();

        // Create an in-memory DuckDB database.
        try (DuckDBConnection connection = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:")) {
            // Run table creations and ingestions until our time is up.
            while (ZonedDateTime.now().isBefore(endingAt)) {
                // Acquire a token from the rate limiter.
                INGESTION_RATE.acquire();

                // Get an identifier for this ingestion.
                long ingestionCounter = counter.getAndIncrement();

                // If we're at or above the table limit, drop an earlier table to remain at the limit.
                if (ingestionCounter >= RESIDENT_TABLE_LIMIT) {
                    threadPool.submit(new DropTableTask(connection, getTableNameForCounter(ingestionCounter - RESIDENT_TABLE_LIMIT)));
                }

                // Submit a new ingestion task.
                threadPool.submit(new IngestionTask(connection, getTableNameForCounter(ingestionCounter)));
            }

            // Shut down the thread pool and wait a while for it to complete any in-flight work.
            threadPool.shutdown();
            if (!threadPool.awaitTermination(SHUTDOWN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                System.err.printf("Thread pool has taken longer than %s to shut down%n", SHUTDOWN_TIMEOUT);
            }
        }
    }

    /**
     * Parse the test duration from the command line arguments. If no duration was supplied
     * then use the default. Otherwise parse the duration using {@link Duration}.parse(String).
     *
     * @param args The command line arguments to parse.
     * @return The parsed duration or the default if no duration was given.
     */
    private static Duration getTestDuration(String[] args) {
        if (args.length == 0) {
            return DEFAULT_TEST_DURATION;
        }

        return Duration.parse(args[0]);
    }

    /**
     * Generates the table name for the given ingestion number.
     *
     * @param ingestionCounter The ingestion number.
     * @return The table name.
     */
    private static String getTableNameForCounter(long ingestionCounter) {
        return String.format("T%09d", ingestionCounter);
    }
}
