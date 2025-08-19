package com.techatpark.sql;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Functional interface representing a parameter mapper
 * that binds parameters
 * to a {@link PreparedStatement}. Implementations of this interface are
 * responsible for mapping a specific parameter
 * to the appropriate placeholder
 * in the SQL query.
 */
public interface ParamMapper {

    /**
     * Binds the provided parameters to the placeholders in the given
     * {@link PreparedStatement}. This method is called to map and set
     * parameter values for the SQL query.
     *
     * @param ps the {@link PreparedStatement}
     *                          to bind parameters to
     * @param index index of the parameter.
     * @throws SQLException if a database access error occurs or if
     *                      parameter binding fails
     */
    void set(PreparedStatement ps,
             int index) throws SQLException;
}
