package com.techatpark.sql;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * The Sql interface represents a generic SQL operation that can be executed
 * using a JDBC Connection.
 *
 * @param <R> the type of the result returned by the SQL operation
 */
public interface Sql<R> {
    /**
     * Executes the SQL operation using the provided JDBC connection.
     * @param connection the JDBC connection to use for executing the operation
     * @return the result of the SQL operation
     * @throws SQLException if an SQL error occurs during the execution
     */
    R execute(Connection connection) throws SQLException;

    /**
     * Executes the SQL operation using the provided JDBC dataSource.
     * @param dataSource the JDBC dataSource to use for executing the operation
     * @return the result of the SQL operation
     * @throws SQLException if an SQL error occurs during the execution
     */
    default R execute(final DataSource dataSource) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return execute(conn);
        }
    }
}
