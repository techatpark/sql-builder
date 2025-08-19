package com.techatpark.sql;

import java.sql.CallableStatement;
import java.sql.SQLException;

/**
 * StatementMapper is an interface that defines how to map statement
 * to a Java object.
 *
 * @param <T> the type of object to map the result set to
 */
@FunctionalInterface
public interface StatementMapper<T> {
    /**
     * Gets Value from Statement.
     * @param statement
     * @return result
     */
    T get(CallableStatement statement) throws SQLException;
}
