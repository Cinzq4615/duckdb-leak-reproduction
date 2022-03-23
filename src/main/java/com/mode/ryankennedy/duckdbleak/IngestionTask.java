package com.mode.ryankennedy.duckdbleak;

import org.duckdb.DuckDBAppender;
import org.duckdb.DuckDBConnection;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * A {@link Runnable} to perform a single shot `CREATE TABLE …` followed by inserting
 * 10k rows of data into the table.
 */
public class IngestionTask implements Runnable {
    public static final int TEST_ROW_COUNT = 10_000;
    private final DuckDBConnection connection;
    private final String tableName;

    public IngestionTask(DuckDBConnection connection, String tableName) {
        this.connection = connection;
        this.tableName = tableName;
    }

    @Override
    public void run() {
        System.out.printf("Loading table %s...%n", tableName);

        try (DuckDBConnection runConnection = ((DuckDBConnection) connection.duplicate())) {
            createTable(runConnection);
            loadTable(runConnection);

            int tableCount = getTableCount(runConnection);
            if (tableCount != TEST_ROW_COUNT) {
                System.err.printf("Table %s count is wrong, actual = %,d%n", tableName, tableCount);
            }
        } catch (SQLException e) {
            System.err.printf("Error during ingestion %s%n", tableName);
            e.printStackTrace(System.err);
        }
    }

    private void createTable(Connection runConnection) throws SQLException {
        try (Statement statement = runConnection.createStatement()) {
            statement.executeUpdate(String.format("CREATE TABLE %s (id BIGINT, message VARCHAR)", tableName));
        }
    }

    private void loadTable(DuckDBConnection runConnection) throws SQLException {
        try (DuckDBAppender appender = runConnection.createAppender("main", tableName)) {
            for (long i = 0; i < TEST_ROW_COUNT; i++) {
                appender.beginRow();
                appender.append(i);
                appender.append(String.format("Hello, %09d!", i));
                appender.endRow();
            }
        }
    }

    private int getTableCount(Connection runConnection) throws SQLException {
        try (Statement statement = runConnection.createStatement();
             ResultSet results = statement.executeQuery(String.format("SELECT count(*) FROM %s", tableName))) {
            results.next();
            return results.getInt(1);
        }
    }
}
