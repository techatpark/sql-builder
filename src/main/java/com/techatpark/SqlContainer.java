package com.techatpark;

import com.techatpark.sql.Sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.function.Function;

/**
 * Contains Connected SQL.
 * @param <T>
 */
public sealed class SqlContainer<T> implements Sql<T>
                    permits Transaction {

    /** The SQL operation to be executed in this transaction stage. */
    private final Sql<T> sql;

    /**
     * Constructs a transaction stage with the given SQL operation.
     *
     * @param theSql the SQL operation to be executed
     */
    protected SqlContainer(final Sql<T> theSql) {
        this.sql = theSql;
    }

    /**
     * Gets Sql.
     * @return sql
     */
    protected Sql<T> getSql() {
        return sql;
    }

    /**
     * Chains another SQL operation to be executed after this transaction stage.
     *
     * <p>The result of the current SQL operation is passed into the
     * provided function, which returns the next SQL operation to execute.</p>
     *
     * @param tSqlFunction function mapping the result of this SQL to next SQL
     * @param <R>          the result type of the next SQL operation
     * @return a new {@code Transaction} instance
     */
    public <R> SqlContainer<R> thenApply(
            final Function<T, Sql<R>> tSqlFunction) {
        return new SqlContainer<>(connection -> {
            T t = sql.execute(connection);
            return tSqlFunction.apply(t).execute(connection);
        });
    }

    /**
     * Executes the transaction on the given database connection.
     *
     * <p>Manages transaction boundaries automatically:
     * <ul>
     *   <li>Disables auto-commit before execution</li>
     *   <li>Commits the transaction upon success</li>
     *   <li>Restores auto-commit afterward</li>
     * </ul></p>
     *
     * @param connection the database connection
     * @return the result of the SQL execution
     * @throws SQLException if a database error occurs
     */
    @Override
    public T execute(final Connection connection) throws SQLException {
        connection.setAutoCommit(false);
        T t = sql.execute(connection);
        connection.commit();
        connection.setAutoCommit(true);
        return t;
    }

    /**
     * Creates a savepoint within the current transaction.
     * <p>
     * Savepoints allow you to mark a specific point in a transaction
     * that you can roll back to later without affecting the entire transaction.
     * This is useful when you want to group a set of operations together,
     * but still have the option to undo them if needed, while keeping earlier
     * successful operations intact.
     * </p>
     *
     * <pre>
     * Transaction
     *     .begin(...)
     *     .savePoint("sp1")
     *     .thenApply(...)
     *     .rollBackTo("sp1")
     *     .execute(dataSource);
     * </pre>
     *
     * @param savePointId a unique identifier for the savepoint
     * @param tSqlFunction function mapping the result of this SQL to next SQL
     * @param <R>          the result type of the next SQL operation
     * @return the current {@link SqlContainer} instance for fluent chaining
     */
    public  <R> SqlContainer<R> savePoint(final String savePointId,
                  final Function<T, Sql<R>> tSqlFunction) {
        return new SqlContainer<>(connection -> {
            T t = sql.execute(connection);
            Savepoint savepoint = connection.setSavepoint(savePointId);
            R r = null;
            try {
                r = tSqlFunction.apply(t).execute(connection);
            } catch (SQLException sqlException) {
                connection.rollback(savepoint);
            }
            return r;
        });
    }

}
