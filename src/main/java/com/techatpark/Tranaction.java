package com.techatpark;

import com.techatpark.sql.Sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Function;

public class Tranaction<T> implements Sql<T> {

    private final Sql<T> sql;
    private final Function<Object, Sql<T>> mapper;
    private final Tranaction<?> parent;

    private Tranaction(Sql<T> sql, Function<Object, Sql<T>> mapper, Tranaction<?> parent) {
        this.sql = sql;
        this.mapper = mapper;
        this.parent = parent;
    }

    private Tranaction(Sql<T> sql) {
        this(sql, null, null);
    }

    public static <T> Tranaction<T> begin(Sql<T> sql) {
        return new Tranaction<>(sql);
    }

    public <R> Tranaction<R> thenApply(Function<T, Sql<R>> tSqlFunction) {
        return new Tranaction<>(null, (Function<Object, Sql<R>>) tSqlFunction, this);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T execute(Connection connection) throws SQLException {
        if (parent == null) {
            // Root: just run its SQL
            return sql.execute(connection);
        } else {
            // Resolve parent first
            Object parentResult = parent.execute(connection);

            if (sql != null) {
                return sql.execute(connection);
            } else if (mapper != null) {
                Sql<T> nextSql = ((Function<Object, Sql<T>>) mapper).apply(parentResult);
                return nextSql.execute(connection);
            }
            throw new IllegalStateException("Invalid transaction chain");
        }
    }
}
