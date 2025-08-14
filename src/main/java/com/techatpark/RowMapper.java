package com.techatpark;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * RowMapper is an interface that defines how to map each row of a ResultSet
 * to a Java object.
 *
 * @param <T> the type of object to map the result set to
 */
public interface RowMapper<T> {



    /**
     * Maps a single row of the result set to an object.
     *
     * @param rs the result set obtained from executing the SQL query
     * @return the mapped object
     * @throws SQLException if an SQL error occurs during mapping
     */
    T get(ResultSet rs) throws SQLException;
}
