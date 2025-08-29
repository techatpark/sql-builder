package com.techatpark;

import com.techatpark.sql.Sql;

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
public final class Transaction<T> extends SqlContainer<T> {

    /**
     * Constructs a transaction stage with the given SQL operation.
     *
     * @param theSql the SQL operation to be executed
     */
    private Transaction(final Sql<T> theSql) {
        super(theSql);
    }

    /**
     * Begins a new transaction with the initial SQL operation.
     *
     * @param sql the first SQL operation
     * @param <T> the result type of the SQL operation
     * @return a new {@code Transaction} instance
     */
    public static <T> Transaction<T> begin(final Sql<T> sql) {
        return new Transaction<>(sql);
    }

}
