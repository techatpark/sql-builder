package com.techatpark;

import com.techatpark.sql.Sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Represents a database transaction as a composable unit of work.
 *
 * <p>This class implements the {@link Sql} interface, enabling execution
 * of SQL operations within a transactional context. It allows chaining
 * multiple SQL operations together using a functional style, where the
 * output of one operation can be passed as input to the next.</p>
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * Transaction<Integer> tx = Transaction.begin(insertUserSql)
 *     .thenApply(userId -> updateAccountSql(userId))
 *     .thenApply(accountId -> fetchDetailsSql(accountId));
 *      .execute(dataSource)
 * }</pre>
 *
 * <h2>Transaction Management</h2>
 * <ul>
 *   <li>Before execution, {@code autoCommit} is disabled.</li>
 *   <li>The SQL chain is executed atomically.</li>
 *   <li>After successful execution, the transaction is committed.</li>
 *   <li>On exceptions, the caller must handle rollback (if needed).</li>
 *   <li>{@code autoCommit} is restored to {@code true} at the end.</li>
 * </ul>
 *
 * @param <T> the result type of the SQL operation at the current stage.
 */
public final class Transaction<T> implements Sql<T> {

    /** The SQL operation to be executed in this transaction stage. */
    private final Sql<T> sql;

    private final Map<String, Savepoint> savepointMap;

    private String savePoint;

    private String rollBackSavePoint;

    /**
     * Constructs a transaction stage with the given SQL operation.
     *
     * @param theSql the SQL operation to be executed
     */
    private Transaction(final Sql<T> theSql, Map<String, Savepoint> savepointMap) {
        this.sql = theSql;
        this.savepointMap = savepointMap;
    }

    /**
     * Begins a new transaction with the initial SQL operation.
     *
     * @param sql the first SQL operation
     * @param <T> the result type of the SQL operation
     * @return a new {@code Transaction} instance
     */
    public static <T> Transaction<T> begin(final Sql<T> sql) {
        return new Transaction<>(sql, new HashMap<>());
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
    public <R> Transaction<R> thenApply(
            final Function<T, Sql<R>> tSqlFunction) {
        return new Transaction<>(connection -> {
            T t = sql.execute(connection);
            R r = tSqlFunction.apply(t).execute(connection);
            if(this.savePoint != null) {
                this.savepointMap.put(this.savePoint,
                        connection.setSavepoint(this.savePoint));
            }
            return r;
        }, this.savepointMap);
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
        if (this.rollBackSavePoint != null ) {
            Savepoint savepoint = this.savepointMap.get(this.rollBackSavePoint);
            if(savepoint!= null) {
                connection.rollback(savepoint);
            }
        }
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
     * @return the current {@link Transaction} instance for fluent chaining
     */
    public Transaction<?> savePoint(final String savePointId) {
        this.savePoint = savePointId;
        return this;
    }

    /**
     * Rolls back the transaction to a previously defined savepoint.
     * <p>
     * This undoes all operations performed after the given savepoint,
     * while preserving all operations performed before it. If the savepoint
     * does not exist or is invalid, the behavior depends on the database driver
     * and transaction manager.
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
     * @param savePointId the identifier of the savepoint to roll back to
     * @return the current {@link Transaction} instance for fluent chaining
     */
    public Transaction<?> rollBackTo(final String savePointId) {
        this.rollBackSavePoint = savePointId;
        return this;
    }

}
