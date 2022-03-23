package com.mode.ryankennedy.duckdbleak;

import org.duckdb.DuckDBConnection;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * A {@link Runnable} to perform a single shot `DROP TABLE IF EXISTS …`.
 */
public class DropTableTask implements Runnable {
    private final DuckDBConnection connection;
    private final String tableName;

    public DropTableTask(DuckDBConnection connection, String tableName) {
        this.connection = connection;
        this.tableName = tableName;
    }

    @Override
    public void run() {
        try (Connection runConnection = connection.duplicate();
             Statement statement = runConnection.createStatement()) {
            System.out.printf("Dropping table %s%n", tableName);

            statement.executeUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        } catch (SQLException e) {
            System.err.printf("Error dropping table %s%n", tableName);
            e.printStackTrace(System.err);
        }
    }
}
