package com.techatpark.sql;

import java.math.BigDecimal;
import java.net.URL;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * RowMapper is an interface that defines how to map each row of a ResultSet
 * to a Java object.
 *
 * @param <T> the type of object to map the result set to
 */
public interface RowMapper<T> {

    /**
     * Mapper for String.
     */
    RowMapper<String> STRING_MAPPER
            = rs -> rs.getString(1);
    /**
     * Mapper for Integer.
     */
    RowMapper<Integer> INTEGER_MAPPER
            = rs -> rs.getInt(1);
    /**
     * Mapper for BYTE.
     */
    RowMapper<Byte> BYTE_MAPPER
            = rs -> rs.getByte(1);
    /**
     * Mapper for bytes.
     */
    RowMapper<byte[]> BYTES_MAPPER
            = rs -> rs.getBytes(1);
    /**
     * Mapper for Short.
     */
    RowMapper<Short> SHORT_MAPPER
            = rs -> rs.getShort(1);
    /**
     * Mapper for URL.
     */
    RowMapper<URL> URL_MAPPER
            = rs -> rs.getURL(1);
    /**
     * Mapper for Double.
     */
    RowMapper<Double> DOUBLE_MAPPER
            = rs -> rs.getDouble(1);
    /**
     * Mapper for Float.
     */
    RowMapper<Float> FLOAT_MAPPER
            = rs -> rs.getFloat(1);
    /**
     * Mapper for BigDecimal.
     */
    RowMapper<BigDecimal> BIG_DECIMAL_MAPPER
            = rs -> rs.getBigDecimal(1);
    /**
     * Mapper for Boolean.
     */
    RowMapper<Boolean> BOOLEAN_MAPPER
            = rs -> rs.getBoolean(1);
    /**
     * Mapper for Long.
     */
    RowMapper<Long> LONG_MAPPER
            = rs -> rs.getLong(1);
    /**
     * Mapper for Date.
     */
    RowMapper<Date> DATE_MAPPER
            = rs -> rs.getDate(1);
    /**
     * Mapper for Time.
     */
    RowMapper<Time> TIME_MAPPER
            = rs -> rs.getTime(1);
    /**
     * Mapper for TimeStamp.
     */
    RowMapper<Timestamp> TIMESTAMP_MAPPER
            = rs -> rs.getTimestamp(1);
    /**
     * Mapper for Object.
     */
    RowMapper<Object> OBJECT_MAPPER
            = rs -> rs.getObject(1);


    /**
     * Maps a single row of the result set to an object.
     *
     * @param rs the result set obtained from executing the SQL query
     * @return the mapped object
     * @throws SQLException if an SQL error occurs during mapping
     */
    T get(ResultSet rs) throws SQLException;
}
