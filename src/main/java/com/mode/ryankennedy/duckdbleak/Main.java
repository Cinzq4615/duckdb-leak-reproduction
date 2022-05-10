package com.mode.ryankennedy.duckdbleak;

import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBDatabase;

import java.sql.*;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.LinkedList;
import java.util.Queue;
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
    // How long the test should run.
    private static final Duration DEFAULT_TEST_DURATION = Duration.ofHours(1);

    // How long to wait for the thread pool to shut down when quitting.
    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(5);

    private static final String DATABASE_SIZE_QUERY = "pragma database_size";
    private static final String TABLE_COUNT_QUERY =
	"select count(*) as table_count from information_schema.tables where table_schema = 'main'";

    public static void main(String[] args) throws Exception {
        // Determine how long the test is going to run.
        Duration testDuration = getTestDuration(args);

        // Do the date math to figure out when the test should shut down.
        ZonedDateTime startingAt = ZonedDateTime.now();
        ZonedDateTime endingAt = startingAt.plus(testDuration);

        while (ZonedDateTime.now().isBefore(endingAt)) {
	    DuckDBConnection connection = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
	    try (Statement statement = connection.createStatement()) {
		statement.execute("PRAGMA threads=1;");
	    }

	    for (int ii = 0; ii < 10; ++ii) {
		Thread.sleep(1000);
		// Try to clear out any JVM noise.
		System.gc();

		try (Statement statement = connection.createStatement()) {
		    try (ResultSet results = statement.executeQuery(DATABASE_SIZE_QUERY)) {
			if (!results.next()) {
			    throw new Exception("Empty result set from 'pragma database_size'");
			}
		    }

		    try (ResultSet results = statement.executeQuery(TABLE_COUNT_QUERY)) {
			if (!results.next()) {
			    throw new Exception("Empty result set when determining table count");
			}
		    }
		}
	    }

	    DuckDBDatabase db = connection.getDatabase();
	    connection.close();
	    db.shutdown();
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
}
