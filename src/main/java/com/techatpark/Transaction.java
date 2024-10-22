package com.techatpark;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * The {@code Transaction} class represents a transactional context
 * for executing SQL statements.
 * It allows for batching multiple SQL operations into a single transaction,
 * ensuring that either
 * all operations are committed or none are applied in case of an error.
 *
 * <p>This class is designed to be used in a try-with-resources block
 * to ensure that the
 * {@code Connection} is properly managed.</p>
 *
 * <p>Example usage:</p>
 * <pre>
 *     try {
 *         Transaction.begin()
 *             .perform(new Sql<>("INSERT INTO ..."))
 *             .perform(new Sql<>("UPDATE ..."))
 *             .commit(dataSource);
 *     } catch (SQLException e) {
 *         // Handle exception
 *     }
 * </pre>
 */
public final class Transaction<T>  {

    /**
     * Begins a new transaction.
     * @param sql the sql
     * @return a new instance of {@code Transaction}
     */
    public static <T> Transaction<T>  begin(final Sql<T> sql) {
        return new Transaction<>(sql);
    }

    /**
     * SQL Statements.
     */
    private final Sql<T> sql;

    /**
     * SQL Functions.
     */
    private final List<Function<Object, Sql<T>>> sqlFunctions;

    /**
     * Private constructor to initialize the list of SQL statements.
     * @param theSql
     */
    private Transaction(final Sql<T> theSql) {
        this.sql = theSql;
        this.sqlFunctions = new ArrayList<>();
    }

    /**
     * Commits the transaction, executing all registered SQL statements.
     *
     * @param dataSource
     * @throws SQLException if an error occurs during
     *                  SQL execution or committing the transaction
     */
    public void commit(final DataSource dataSource) throws SQLException {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            Object value = sql.execute(connection);
            for (Function<Object, Sql<T>> sqlFunction : sqlFunctions) {
                value = sqlFunction
                        .apply(value)
                        .execute(connection);
            }
            connection.commit();
        } catch (SQLException sqlException) {
            if (connection != null) {
                connection.rollback();
            }
            throw sqlException;
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    /**
     * Registers a SQL statement to be executed as part of this transaction.
     *
     * @param statement the SQL statement to perform
     * @return this {@code Transaction} instance for method chaining
     */
    public Transaction<T> thenApply(final Function<T, Sql<T>> statement) {
        sqlFunctions.add((Function<Object, Sql<T>>) statement);
        return this;
    }
}
