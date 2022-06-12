package com.mode.ryankennedy.duckdbleak;

import org.duckdb.DuckDBAppender;
import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBDatabase;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Random;

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
    private static final String TABLE_COUNT_QUERY = "select count(id) as count from mydb.test";


    private static final String SCHEMA_CREATE_SQL = "CREATE SCHEMA IF NOT EXISTS mydb;";

    private static final String TABEL_CREATE_SQL = "CREATE TABLE IF NOT EXISTS mydb.test(id INTEGER, name VARCHAR);";

    private static final String TABEL_DROP_SQL = "DROP TABLE IF EXISTS mydb.test;";

    private static final String TABEL_QUERY_SQL = "SELECT * FROM mydb.test where id=%d";

    private static Random random = new Random();


    public static void main(String[] args) throws Exception {
        // Determine how long the test is going to run.
        Duration testDuration = getTestDuration(args);

        // Do the date math to figure out when the test should shut down.
        ZonedDateTime startingAt = ZonedDateTime.now();
        ZonedDateTime endingAt = startingAt.plus(testDuration);


        DuckDBConnection connection = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb::memory:");
        try (Statement statement = connection.createStatement()) {
            statement.execute("PRAGMA threads=1;");
        }

        try (Statement statement = connection.createStatement()) {
            statement.execute(SCHEMA_CREATE_SQL);
        }

        try (Statement statement = connection.createStatement()) {
            statement.execute(TABEL_CREATE_SQL);
        }
        System.out.println("append data start ......");
        for (int ii = 0; ii < 1000; ++ii) {
            DuckDBAppender appender = connection.createAppender("mydb", "test");
            for (int i = 0; i < 10000; i++) {
                appender.beginRow();
                appender.append(i);
                appender.append("name-" + i);
                //appender.ap
                appender.endRow();
            }
            appender.close();
        }
        System.out.println("append data end ......");

        try (Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery(TABLE_COUNT_QUERY);
            rs.next();
            System.out.println("count=" + rs.getInt(1));
        }

//        System.out.println("drop table start ......");
//        try (Statement statement = connection.createStatement()) {
//            statement.execute(TABEL_DROP_SQL);
//        }
//        System.out.println("drop table end......");
        while (ZonedDateTime.now().isBefore(endingAt)) {
            try (Statement statement = connection.createStatement();ResultSet rs = statement.executeQuery(String.format(TABEL_QUERY_SQL,random.nextInt(10000)))) {
                if(rs.next()){
                    System.out.println(rs.getString(2));
                }
            }
        }

        DuckDBDatabase db = connection.getDatabase();
        connection.close();
        db.shutdown();


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
