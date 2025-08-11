package com.techatpark;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static com.techatpark.SqlBuilder.RowMapper.TIME_MAPPER;
import static com.techatpark.SqlBuilder.RowMapper.TIMESTAMP_MAPPER;
import static com.techatpark.SqlBuilder.RowMapper.BOOLEAN_MAPPER;
import static com.techatpark.SqlBuilder.RowMapper.BIG_DECIMAL_MAPPER;
import static com.techatpark.SqlBuilder.RowMapper.DATE_MAPPER;
import static com.techatpark.SqlBuilder.RowMapper.DOUBLE_MAPPER;
import static com.techatpark.SqlBuilder.RowMapper.FLOAT_MAPPER;
import static com.techatpark.SqlBuilder.RowMapper.LONG_MAPPER;
import static com.techatpark.SqlBuilder.RowMapper.OBJECT_MAPPER;
import static com.techatpark.SqlBuilder.RowMapper.STRING_MAPPER;
import static com.techatpark.SqlBuilder.RowMapper.INTEGER_MAPPER;
import static com.techatpark.SqlBuilder.RowMapper.BYTE_MAPPER;
import static com.techatpark.SqlBuilder.RowMapper.BYTES_MAPPER;
import static com.techatpark.SqlBuilder.RowMapper.URL_MAPPER;
import static com.techatpark.SqlBuilder.RowMapper.SHORT_MAPPER;


/**
 * SqlBuilder is a utility class that simplifies the process of constructing
 * SQL queries with dynamic parameters and executing them. It helps developers
 * manage SQL queries more efficiently with less boilerplate code, supporting
 * both query execution and parameterized updates.
 */
public sealed class SqlBuilder implements Sql<Integer> {

    /**
     * The SQL query to be executed.
     */
    private final String sql;

    /**
     * Builds Callable Sql Builder from Sql.
     *
     * @param theSql the SQL query to be prepared and executed
     * @return sqlBuilder
     */
    public static CallableSqlBuilder prepareCall(final String theSql) {
        return new CallableSqlBuilder(theSql);
    }

    /**
     * Builds Prerpared Sql Builder from Sql.
     *
     * @param theSql the SQL query to be prepared and executed
     * @return sqlBuilder
     */
    public static PreparedSqlBuilder prepareSql(final String theSql) {
        return new PreparedSqlBuilder(theSql);
    }

    /**
     * Builds Sql Builder from Sql.
     *
     * @param theSql the SQL query to be prepared and executed
     * @return sqlBuilder
     */
    public static SqlBuilder sql(final String theSql) {
        return new SqlBuilder(theSql);
    }

    /**
     * Constructor that initializes the SqlBuilder with a given SQL query.
     *
     * @param theSql the SQL query to be prepared and executed
     */
    protected SqlBuilder(final String theSql) {
        this.sql = theSql;
    }

    /**
     * Get the SQL Query.
     * @return sql
     */
    protected String getSql() {
        return sql;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Integer execute(final Connection connection) throws SQLException {
        int updatedRows;
        try (Statement stmt = connection.createStatement()) {
            updatedRows = stmt.executeUpdate(getSql());
        }
        return updatedRows;
    }

    /**
     * Provides if record exists.
     *
     * @return a new Query instance for execution
     */
    public Sql<Boolean> queryForExists() {
        return this::exists;
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to a Byte.
     *
     * @return a new Query instance for execution
     */
    public Sql<Byte> queryForByte() {
        return connection -> getResult(BYTE_MAPPER, connection);
    }


    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set the List of Byte.
     *
     * @return a new Query instance for execution
     */
    public Sql<List<Byte>> queryForListOfByte() {
        return connection -> getResultAsList(BYTE_MAPPER, connection);
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to a Byte Array.
     *
     * @return a new Query instance for execution
     */
    public Sql<byte[]> queryForBytes() {
        return connection -> getResult(BYTES_MAPPER, connection);
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to a Byte Array.
     *
     * @return a new Query instance for execution
     */
    public Sql<List<byte[]>> queryForListOfBytes() {
        return connection -> getResultAsList(BYTES_MAPPER, connection);
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to an Integer.
     *
     * @return a new Query instance for execution
     */
    public Sql<Integer> queryForInt() {
        return connection -> getResult(INTEGER_MAPPER, connection);
    }


    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to List of Integer.
     *
     * @return a new Query instance for execution
     */
    public Sql<List<Integer>> queryForListOfInt() {
        return connection -> getResultAsList(INTEGER_MAPPER, connection);
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to a Short.
     *
     * @return a new Query instance for execution
     */
    public Sql<Short> queryForShort() {
        return connection -> getResult(SHORT_MAPPER, connection);
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set the List of Short.
     *
     * @return a new Query instance for execution
     */
    public Sql<List<Short>> queryForListOfShort() {
        return connection -> getResultAsList(SHORT_MAPPER, connection);
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to a String.
     *
     * @return a new Query instance for execution
     */
    public Sql<String> queryForString() {
        return connection -> getResult(STRING_MAPPER, connection);
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to a String.
     *
     * @return a new Query instance for execution
     */
    public Sql<List<String>> queryForListOfString() {
        return connection -> getResultAsList(STRING_MAPPER, connection);
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to a URL.
     *
     * @return a new Query instance for execution
     */
    public Sql<URL> queryForURL() {
        return connection -> getResult(URL_MAPPER, connection);
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set the List of URL.
     *
     * @return a new Query instance for execution
     */
    public Sql<List<URL>> queryForListOfURL() {
        return connection -> getResultAsList(URL_MAPPER, connection);
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to a Double.
     *
     * @return a new Query instance for execution
     */
    public Sql<Double> queryForDouble() {
        return connection -> getResult(DOUBLE_MAPPER, connection);
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set the List of Double.
     *
     * @return a new Query instance for execution
     */
    public Sql<List<Double>> queryForListOfDouble() {
        return connection -> getResultAsList(DOUBLE_MAPPER, connection);
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to a Float.
     *
     * @return a new Query instance for execution
     */
    public Sql<Float> queryForFloat() {
        return connection -> getResult(FLOAT_MAPPER, connection);
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set the List of Float.
     *
     * @return a new Query instance for execution
     */
    public Sql<List<Float>> queryForListOfFloat() {
        return connection -> getResultAsList(FLOAT_MAPPER, connection);
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to a BigDecimal.
     *
     * @return a new Query instance for execution
     */
    public Sql<BigDecimal> queryForBigDecimal() {
        return connection -> getResult(BIG_DECIMAL_MAPPER, connection);
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set the List of BigDecimal.
     *
     * @return a new Query instance for execution
     */
    public Sql<List<BigDecimal>> queryForListOfBigDecimal() {
        return connection -> getResultAsList(BIG_DECIMAL_MAPPER, connection);
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to a Boolean.
     *
     * @return a new Query instance for execution
     */
    public Sql<Boolean> queryForBoolean() {
        return connection -> getResult(BOOLEAN_MAPPER, connection);
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set the List of Boolean.
     *
     * @return a new Query instance for execution
     */
    public Sql<List<Boolean>> queryForListOfBoolean() {
        return connection -> getResultAsList(BOOLEAN_MAPPER, connection);
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to a Long.
     *
     * @return a new Query instance for execution
     */
    public Sql<Long> queryForLong() {
        return connection -> getResult(LONG_MAPPER, connection);
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set the List of Long.
     *
     * @return a new Query instance for execution
     */
    public Sql<List<Long>> queryForListOfLong() {
        return connection -> getResultAsList(LONG_MAPPER, connection);
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to a Date.
     *
     * @return a new Query instance for execution
     */
    public Sql<java.sql.Date> queryForDate() {
        return connection -> getResult(DATE_MAPPER, connection);
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set List of Date.
     *
     * @return a new Query instance for execution
     */
    public Sql<List<java.sql.Date>> queryForListOfDate() {
        return connection -> getResultAsList(DATE_MAPPER, connection);
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to a Time.
     *
     * @return a new Query instance for execution
     */
    public Sql<java.sql.Time> queryForTime() {
        return connection -> getResult(TIME_MAPPER, connection);
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set List of Time.
     *
     * @return a new Query instance for execution
     */
    public Sql<List<java.sql.Time>> queryForListOfTime() {
        return connection -> getResultAsList(TIME_MAPPER, connection);
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to a Timestamp.
     *
     * @return a new Query instance for execution
     */
    public Sql<java.sql.Timestamp> queryForTimestamp() {
        return connection -> getResult(TIMESTAMP_MAPPER, connection);
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set List of Timestamp.
     *
     * @return a new Query instance for execution
     */
    public Sql<List<java.sql.Timestamp>> queryForListOfTimestamp() {
        return connection -> getResultAsList(TIMESTAMP_MAPPER, connection);
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to an Object.
     *
     * @return a new Query instance for execution
     */
    public Sql<Object> queryForObject() {
        return connection -> getResult(OBJECT_MAPPER, connection);
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set List of Object.
     *
     * @return a new Query instance for execution
     */
    public Sql<List<Object>> queryForListOfObject() {
        return connection -> getResultAsList(OBJECT_MAPPER, connection);
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to a specific object type
     * using the provided RowMapper.
     *
     * @param <T> the type of object to map the result set to
     * @param rowMapper an implementation of
     *                  RowMapper to map each row of the result set
     * @return a new Query instance for execution
     */
    public <T> Sql<T> queryForOne(
            final RowMapper<T> rowMapper) {
        return connection -> getResult(rowMapper, connection);
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to list of a specific object type
     * using the provided RowMapper.
     *
     * @param <T> the type of object to map the result set to
     * @param rowMapper an implementation of
     *                  RowMapper to map each row of the result set
     * @return a new Query instance for execution
     */
    public <T> Sql<List<T>> queryForList(
            final RowMapper<T> rowMapper) {
        return connection -> getResultAsList(rowMapper, connection);
    }

    /**
     * Checks if Record Exists.
     * @param connection
     * @return exists
     * @throws SQLException
     */
    protected boolean exists(final Connection connection) throws SQLException {
        boolean exists;
        try (Statement stmt = connection.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(getSql())) {
                exists = rs.next();
            }
        }
        return exists;
    }

    /**
     * Get Result for a Query.
     * @param query
     * @param connection
     * @return result
     * @param <T>
     * @throws SQLException
     */
    protected <T> T getResult(final RowMapper<T> query,
                        final Connection connection) throws SQLException {
        T result = null;
        try (Statement stmt = connection.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(getSql())) {
                if (rs.next()) {
                    result = query.get(rs);
                }
            }
        }
        return result;
    }
    /**
     * {@inheritDoc}
     */
    protected <T> List<T> getResultAsList(final RowMapper<T> query,
                        final Connection connection) throws SQLException {
        List<T> result = new ArrayList<>();
        try (Statement stmt = connection.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(getSql())) {
                while (rs.next()) {
                    result.add(query.get(rs));
                }
            }
        }
        return result;
    }
    /**
     * {@inheritDoc}
     */
    protected <T> T getGeneratedKeys(final RowMapper<T> query,
                             final Connection connection) throws SQLException {
        T result = null;
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(getSql(), Statement.RETURN_GENERATED_KEYS);
            try (ResultSet rs = stmt.getGeneratedKeys()) {
                if (rs.next()) {
                    result = query.get(rs);
                }
            }
        }
        return result;
    }
    /**
     * Get Generated Keys as List for a Query.
     * {@inheritDoc}
     */
    protected <T> List<T> getGeneratedKeysAsList(final RowMapper<T> query,
                         final Connection connection) throws SQLException {
        List<T> result = new ArrayList<>();
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(getSql(), Statement.RETURN_GENERATED_KEYS);
            try (ResultSet rs = stmt.getGeneratedKeys()) {
                while (rs.next()) {
                    result.add(query.get(rs));
                }
            }
        }
        return result;
    }
    /**
     * add Batch.
     * @param sqlQuery
     * @return new Batch
     */
    public Batch addBatch(final String sqlQuery) {
        return new Batch(sqlQuery);
    }

    /**
     * inner Batch class of SqlBuilder.
     */
    public class Batch {
        /**
         * List of sqls using String.
         */
        private final List<String> sqls;

        /**
         * constructor of Batch and it takes the sql query.
         * @param sqlQuery
         */
        public Batch(final String sqlQuery) {
            this.sqls = new ArrayList<>();
            addBatch(sqlQuery);
        }
        /**
         * add Batch.
         * @param sqlQuery
         * @return batch
         */
        public Batch addBatch(final String sqlQuery) {
            this.sqls.add(sqlQuery);
            return this;
        }

        /**
         * executeBatch of the no of querys.
         * @param dataSource
         * @return updatedRows
         * @throws SQLException
         */
        public int[] executeBatch(final DataSource dataSource)
                throws SQLException {
            int[] updatedRows;
            try (Connection connection = dataSource.getConnection();
                    Statement statement = connection.createStatement()) {
                statement.addBatch(SqlBuilder.this.getSql());
                for (String batchSql : this.sqls) {
                    statement.addBatch(batchSql);
                }
                updatedRows = statement.executeBatch();
            }
            return updatedRows;
        }
    }

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
        RowMapper<String> STRING_MAPPER = rs -> rs.getString(1);
        /**
         * Mapper for Integer.
         */
       RowMapper<Integer> INTEGER_MAPPER = rs -> rs.getInt(1);
        /**
         * Mapper for BYTE.
         */
       RowMapper<Byte> BYTE_MAPPER = rs -> rs.getByte(1);
        /**
         * Mapper for bytes.
         */
       RowMapper<byte[]> BYTES_MAPPER = rs -> rs.getBytes(1);
        /**
         * Mapper for Short.
         */
       RowMapper<Short> SHORT_MAPPER = rs -> rs.getShort(1);
        /**
         * Mapper for URL.
         */
       RowMapper<URL> URL_MAPPER = rs -> rs.getURL(1);
        /**
         * Mapper for Double.
         */
       RowMapper<Double> DOUBLE_MAPPER = rs -> rs.getDouble(1);
        /**
         * Mapper for Float.
         */
       RowMapper<Float> FLOAT_MAPPER = rs -> rs.getFloat(1);
        /**
         * Mapper for BigDecimal.
         */
       RowMapper<BigDecimal> BIG_DECIMAL_MAPPER = rs -> rs.getBigDecimal(1);
        /**
         * Mapper for Boolean.
         */
       RowMapper<Boolean> BOOLEAN_MAPPER = rs -> rs.getBoolean(1);
        /**
         * Mapper for Long.
         */
       RowMapper<Long> LONG_MAPPER = rs -> rs.getLong(1);
        /**
         * Mapper for Date.
         */
       RowMapper<Date> DATE_MAPPER = rs -> rs.getDate(1);
        /**
         * Mapper for Time.
         */
       RowMapper<Time> TIME_MAPPER = rs -> rs.getTime(1);
        /**
         * Mapper for TimeStamp.
         */
       RowMapper<Timestamp> TIMESTAMP_MAPPER = rs -> rs.getTimestamp(1);
        /**
         * Mapper for Object.
         */
       RowMapper<Object> OBJECT_MAPPER = rs -> rs.getObject(1);

        /**
         * Maps a single row of the result set to an object.
         *
         * @param rs the result set obtained from executing the SQL query
         * @return the mapped object
         * @throws SQLException if an SQL error occurs during mapping
         */
        T get(ResultSet rs) throws SQLException;
    }

    /**
     * Creates a new Generated Keys object that can be used to execute
     * a SELECT query and map the result set to a specific object type
     * using the provided RowMapper.
     *
     * @param <T> the type of object to map the result set to
     * @param rowMapper an implementation of
     *                  RowMapper to map each row of the result set
     * @return a new Query instance for execution
     */
    public <T> Sql<T> queryGeneratedKeys(
            final RowMapper<T> rowMapper) {
        return connection -> getGeneratedKeys(rowMapper, connection);
    }

    /**
     * Creates a new Generated Keys object that can be used to execute
     * a SELECT query and map the result set to list of a specific object type
     * using the provided RowMapper.
     *
     * @param <T> the type of object to map the result set to
     * @param rowMapper an implementation of
     *                  RowMapper to map each row of the result set
     * @return a new Query instance for execution
     */
    public <T> Sql<List<T>>
            queryGeneratedKeysAsList(final RowMapper<T> rowMapper) {
        return connection -> getGeneratedKeysAsList(rowMapper, connection);
    }

    public static final class PreparedSqlBuilder extends SqlBuilder {
        /**
         * A list of parameters for the query.
         */
        private final List<ParamMapper> paramMappers;

        /**
         * Constructor that initializes the SqlBuilder with a given SQL query.
         *
         * @param theSql the SQL query to be prepared and executed
         */
        private PreparedSqlBuilder(final String theSql) {
            super(theSql);
            this.paramMappers = new ArrayList<>();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Integer execute(final Connection connection)
                throws SQLException {
            int updatedRows;
            try (PreparedStatement ps = getStatement(connection, getSql())) {
                updatedRows = ps.executeUpdate();
            }
            return updatedRows;
        }

        /**
         * Adds a parameter with a null.
         *
         * @return the current SqlBuilder instance, for method chaining
         */
        public PreparedSqlBuilder paramNull() {
            return param((ps, index) -> ps.setObject(index, null));
        }

        /**
         * Adds a parameter with a specific SQL type and type name as `NULL`
         * to the SQL query.
         * This method is used when the SQL parameter should be set to `NULL`
         * for types
         * that require a type name in addition to the SQL type, such as SQL
         * `STRUCT` or `ARRAY`.
         *
         * @param sqlType  the SQL type of the parameter,
         *                 as defined in {@link java.sql.Types}
         * @param typeName the type name of the parameter,
*                 used for SQL types that require specific type information
         * @return the current SqlBuilder instance, for method chaining
         */
        public PreparedSqlBuilder paramNull(final int sqlType,
                                    final String typeName) {
            return param((ps, index) -> ps.setNull(index, sqlType, typeName));
        }

        /**
         * Adds a parameter to the SQL query. The method allows chaining
         * and is used to bind values to placeholders in the SQL query.
         *
         * @param value the value of the parameter to be added
         * @return the current SqlBuilder instance, for method chaining
         */
        public PreparedSqlBuilder param(final Integer value) {
            return param((ps, index) -> ps.setInt(index, value));
        }

        /**
         * Adds a Short parameter to the SQL query.
         *
         * @param value the Short value to be added
         * @return the current SqlBuilder instance, for method chaining
         */
        public PreparedSqlBuilder param(final Short value) {
            return param((ps, index) -> ps.setShort(index, value));
        }

        /**
         * Adds a parameter to the SQL query. The method allows chaining
         * and is used to bind values to placeholders in the SQL query.
         *
         * @param value the value of the parameter to be added
         * @return the current SqlBuilder instance, for method chaining
         */
        public PreparedSqlBuilder param(final String value) {
            return param((ps, index) -> ps.setString(index, value));
        }

        /**
         * Adds a Double parameter to the SQL query.
         *
         * @param value the Double value to be added
         * @return the current SqlBuilder instance, for method chaining
         */
        public PreparedSqlBuilder param(final Double value) {
            return param((ps, index) -> ps.setDouble(index, value));
        }

        /**
         * Adds a Boolean parameter to the SQL query.
         *
         * @param value the Boolean value to be added
         * @return the current SqlBuilder instance, for method chaining
         */
        public PreparedSqlBuilder param(final Boolean value) {
            return param((ps, index) -> ps.setBoolean(index, value));
        }

        /**
         * Adds a Long parameter to the SQL query.
         *
         * @param value the Long value to be added
         * @return the current SqlBuilder instance, for method chaining
         */
        public PreparedSqlBuilder param(final Long value) {
            return param((ps, index) -> ps.setLong(index, value));
        }

        /**
         * Adds a Date parameter to the SQL query.
         *
         * @param value the Date value to be added
         * @return the current SqlBuilder instance, for method chaining
         */
        public PreparedSqlBuilder param(final java.sql.Date value) {
            return param((ps, index) -> ps.setDate(index, value));
        }

        /**
         * Adds a Float parameter to the SQL query.
         *
         * @param value the Float value to be added
         * @return the current SqlBuilder instance, for method chaining
         */
        public PreparedSqlBuilder param(final Float value) {
            return param((ps, index) -> ps.setFloat(index, value));
        }

        /**
         * Adds a byte array parameter to the SQL query.
         *
         * @param value the byte array to be added
         * @return the current SqlBuilder instance, for method chaining
         */
        public PreparedSqlBuilder param(final byte[] value) {
            return param((ps, index) -> ps.setBytes(index, value));
        }

        /**
         * Adds a BigDecimal parameter to the SQL query.
         *
         * @param value the BigDecimal value to be added
         * @return the current SqlBuilder instance, for method chaining
         */
        public PreparedSqlBuilder param(final BigDecimal value) {
            return param((ps, index) -> ps.setBigDecimal(index, value));
        }

        /**
         * Adds a Time parameter to the SQL query.
         *
         * @param value the Time value to be added
         * @return the current SqlBuilder instance, for method chaining
         */
        public PreparedSqlBuilder param(final java.sql.Time value) {
            return param((ps, index) -> ps.setTime(index, value));
        }

        /**
         * Adds a Timestamp parameter to the SQL query.
         *
         * @param value the Timestamp value to be added
         * @return the current SqlBuilder instance, for method chaining
         */
        public PreparedSqlBuilder param(final java.sql.Timestamp value) {
            return param((ps, index) -> ps.setTimestamp(index, value));
        }

        /**
         * Adds an Object parameter to the SQL query.
         *
         * @param value the Object value to be added
         * @return the current SqlBuilder instance, for method chaining
         */
        public PreparedSqlBuilder param(final Object value) {
            return param((ps, index) -> ps.setObject(index, value));
        }

        /**
         * Adds an Object parameter to the SQL query with targetSqlType.
         *
         * @param value the Object value to be added
         * @param targetSqlType the targeted SqlType.
         * @return the current SqlBuilder instance, for method chaining
         */
        public PreparedSqlBuilder param(final Object value,
                                        final int targetSqlType) {
            return param((ps, index)
                    -> ps.setObject(index, value, targetSqlType));
        }

        /**
         * Add new Param Mapper.
         * @param paramMapper
         * @return sqlbuilder
         */
        private PreparedSqlBuilder param(final ParamMapper paramMapper) {
            this.paramMappers.add(paramMapper);
            return this;
        }

        /**
         * Prepares the PreparedStatement by binding all the parameters
         * to their respective
         * positions in the SQL query.
         *
         * @param ps the PreparedStatement to bind parameters to
         * @throws SQLException if a database access error occurs
         * during parameter
         * binding
         * @return prepare
         */
        private PreparedStatement prepare(final PreparedStatement ps)
                throws SQLException {
            return prepare(ps, this.paramMappers);
        }

        /**
         * Prepare Statement with Parameters.
         * @param ps
         * @param pMappers
         * @throws SQLException
         * @return ps
         */
        private PreparedStatement prepare(final PreparedStatement ps,
                                          final List<ParamMapper> pMappers)
                throws SQLException {
            for (int i = 0; i < pMappers.size(); i++) {
                pMappers.get(i).set(ps, (i + 1));
            }
            return ps;
        }

        /**
         * Get the Statement for Query.
         * @param connection
         * @param theSql
         * @return statement to be executed
         * @throws SQLException
         */
        private PreparedStatement getStatement(final Connection connection,
                                                 final String theSql)
                throws SQLException {
            PreparedStatement ps = connection.prepareStatement(theSql);
            prepare(ps);
            return ps;
        }

        /**
          * @param connection .
          * @param theSql
         * @param resultSetType
         * @return statement to be executed
         * @throws SQLException
         */
        private PreparedStatement getStatement(final Connection connection,
                                                 final String theSql,
                                                 final int resultSetType)
                throws SQLException {
            PreparedStatement ps = connection
                    .prepareStatement(theSql, resultSetType);
            prepare(ps);
            return ps;
        }

        /**
         * Checks if Record Exists.
         * @param connection
         * @return exists
         * @throws SQLException
         */
        @Override
        protected boolean exists(final Connection connection)
                throws SQLException {
            boolean exists;
            try (PreparedStatement ps = getStatement(connection, getSql())) {
                try (ResultSet rs = ps.executeQuery()) {
                    exists = rs.next();
                }
            }
            return exists;
        }

        /**
         * Get Result for a Query.
         * @param query
         * @param connection
         * @return result
         * @param <T>
         * @throws SQLException
         */
        @Override
        protected <T> T getResult(final RowMapper<T> query,
                          final Connection connection) throws SQLException {
            T result = null;
            try (PreparedStatement ps
                         = getStatement(connection, getSql())) {
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        result = query.get(rs);
                    }
                }
            }
            return result;
        }
        /**
         * Get Result as a List for a Query.
         * {@inheritDoc}
         */
        protected <T> List<T> getResultAsList(final RowMapper<T> query,
                        final Connection connection) throws SQLException {
            List<T> result = new ArrayList<>();
            try (PreparedStatement ps = getStatement(connection, getSql())) {
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        result.add(query.get(rs));
                    }
                }
            }
            return result;
        }
        /**
         * Get Generated Keys for a Query.
         * {@inheritDoc}
         */
        protected <T> T getGeneratedKeys(final RowMapper<T> query,
                     final Connection connection) throws SQLException {
            T result = null;
            try (PreparedStatement ps = getStatement(connection, this.getSql(),
                    java.sql.Statement.RETURN_GENERATED_KEYS)) {
                ps.executeUpdate();
                try (ResultSet rs = ps.getGeneratedKeys()) {
                    if (rs.next()) {
                        result = query.get(rs);
                    }
                }
            }
            return result;
        }
        /**
         * Get Generated Keys as List for a Query.
         * {@inheritDoc}
         */
        protected <T> List<T> getGeneratedKeysAsList(final RowMapper<T> query,
                             final Connection connection) throws SQLException {
            List<T> result = new ArrayList<>();
            try (PreparedStatement ps = getStatement(connection, this.getSql(),
                    java.sql.Statement.RETURN_GENERATED_KEYS)) {
                ps.executeUpdate();
                try (ResultSet rs = ps.getGeneratedKeys()) {
                    while (rs.next()) {
                        result.add(query.get(rs));
                    }
                }
            }
            return result;
        }

        /**
         * Builds JDBC Batch Builder.
         * @return batch
         */
        public PreparedBatch addBatch() {
            return new PreparedBatch();
        }

        /**
         * JDBC Batch Builder.
         */
        public final class PreparedBatch {

            /**
             * No ofParams in Batch Statement.
             */
            private final int paramsPerBatch;
            /**
             * No ofParams in Batch Statement.
             */
            private int capacity;

            /**
             * SQL Builder to Hold Batch }Parameters.
             */
            private final PreparedSqlBuilder preparedSqlBuilder;

            /**
             * SQL Builder for the query.
             */

            private PreparedBatch() {
                this.paramsPerBatch = PreparedSqlBuilder
                        .this.paramMappers.size();
                this.capacity = paramsPerBatch;
                this.preparedSqlBuilder = new PreparedSqlBuilder(
                        PreparedSqlBuilder.this.getSql());
            }

            /**
             * Adds JDBC Batch Builder.
             * @return batch
             */
            public PreparedBatch addBatch() throws SQLException {
                validate();
                capacity = capacity + paramsPerBatch;
                return this;
            }

            private void validate() throws SQLException {
                if (this.preparedSqlBuilder.paramMappers.size() != capacity) {
                    throw new SQLException(
                            "Parameters do not match "
                                    + "with first set of parameters");
                }
            }

            /**
             * executes the Batch.
             * @param dataSource
             * @return an array of update counts
             */
            public int[] executeBatch(final DataSource dataSource)
                    throws SQLException {
                validate();
                int[] updatedRows;
                try (Connection connection = dataSource.getConnection();
                     PreparedStatement ps = connection
                             .prepareStatement(getSql())) {
                    prepare(ps);

                    updatedRows = ps.executeBatch();
                }
                return updatedRows;
            }

            private void prepare(final PreparedStatement ps)
                    throws SQLException {
                int batchCount = (this.preparedSqlBuilder.paramMappers.size()
                        / this.paramsPerBatch);
                PreparedSqlBuilder.this.prepare(ps, PreparedSqlBuilder.this
                        .paramMappers).addBatch();
                for (int i = 0; i < batchCount; i++) {
                    int from = i * this.paramsPerBatch;
                    PreparedSqlBuilder.this.prepare(ps, this.preparedSqlBuilder
                            .paramMappers.subList(from,
                                    from + this.paramsPerBatch)).addBatch();
                }
            }
            /**
             * Adds a parameter with a null.
             * @return the current SqlBuilder instance, for method chaining
             */
            public PreparedBatch paramNull() {
                this.preparedSqlBuilder.paramNull();
                return this;
            }

            /**
             * Adds a parameter with a specific SQL type and type name as `NULL`
             * to the SQL query.
             * This method is used when the SQL parameter should be set to
             * `NULL`for types
             * that require a type name in addition to the SQL type, such as SQL
             * `STRUCT` or `ARRAY`.
             * @param sqlType  the SQL type of the parameter,
     *                 as defined in {@link java.sql.Types}
             * @param typeName the type name of the parameter,
     *                 used for SQL types that require specific type information
             * @return the current SqlBuilder instance, for method chaining
             */
            public PreparedBatch paramNull(final int sqlType,
                                           final String typeName) {
                this.preparedSqlBuilder.paramNull(sqlType, typeName);
                return this;
            }
            /**
             * Adds a parameter to the SQL query. The method allows chaining
             * and is used to bind values to placeholders in the SQL query.
             * @param <T>
             * @param value the value of the parameter to be added
             * @return the current SqlBuilder instance, for method chaining
             */
            public <T> PreparedBatch preparedParam(final T value) {
                this.preparedSqlBuilder.param(value);
                return this;
            }
            /**
             * Adds a Short parameter to the SQL query.
             *
             * @param value the Short value to be added
             * @return the current SqlBuilder instance, for method chaining
             */
            public PreparedBatch param(final Short value) {
                return preparedParam(value);
            }
            /**
             * Adds a parameter to the SQL query. The method allows chaining
             * and is used to bind values to placeholders in the SQL query.
             *
             * @param value the value of the parameter to be added
             * @return the current SqlBuilder instance, for method chaining
             */
            public PreparedBatch param(final String value) {
                return preparedParam(value);
            }
            /**
             * {@inheritDoc}
             */
            public PreparedBatch param(final Double value) {
                return preparedParam(value);
            }

            /**
             * {@inheritDoc}
             */
            public PreparedBatch param(final Boolean value) {
                return preparedParam(value);
            }
            /**
             * {@inheritDoc}
             */
            public PreparedBatch param(final Long value) {
                return preparedParam(value);
            }
            /**
             * {@inheritDoc}
             */
            public PreparedBatch param(final Date value) {
                return preparedParam(value);
            }
            /**
             * {@inheritDoc}
             */
            public PreparedBatch param(final Float value) {
                return preparedParam(value);
            }
            /**
             * {@inheritDoc}
             */
            public PreparedBatch param(final byte[] value) {
                return preparedParam(value);
            }
            /**
             * {@inheritDoc}
             */
            public PreparedBatch param(final BigDecimal value) {
                return preparedParam(value);
            }
            /**
             * {@inheritDoc}
             */
            public PreparedBatch param(final Time value) {
                return preparedParam(value);
            }
            /**
             * {@inheritDoc}
             */
            public PreparedBatch param(final Timestamp value) {
                return preparedParam(value);
            }
            /**
             * {@inheritDoc}
             */
            public PreparedBatch param(final Object value) {
                return preparedParam(value);
            }
            /**
             * Adds an Object parameter to the SQL query with targetSqlType.
             *
             * @param value the Object value to be added
             * @param targetSqlType the targeted SqlType.
             * @return the current SqlBuilder instance, for method chaining
             */
            public PreparedBatch param(final Object value,
                                       final int targetSqlType) {
                this.preparedSqlBuilder.param(value, targetSqlType);
                return this;
            }
        }
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

    }

    public static final class CallableSqlBuilder implements Sql<Boolean> {
        /**
         * The SQL query to be executed.
         */
        private final PreparedSqlBuilder preparedSqlBuilder;

        /**
         * Wrapper to hide Batch for INm INOUT.
         */
        private final CallableSqlBuilderWrapper callableSqlBuilderWrapper;

        /**
         * Creates Callable Sql Builder.
         * @param theSql
         */
        public CallableSqlBuilder(final String theSql) {
            this.preparedSqlBuilder = new PreparedSqlBuilder(theSql);
            this.callableSqlBuilderWrapper = new CallableSqlBuilderWrapper();
        }

        /**
         * Wrapper for CallableSqlBuilder to hide Batch Operations.
         * for INOUT,OUT parameters.
         */
        public final class CallableSqlBuilderWrapper implements Sql<Boolean> {
            /**
             * this is callableStatement.
             */
            private final CallableSqlBuilder callableSqlBuilder;

            private CallableSqlBuilderWrapper() {
                this.callableSqlBuilder = CallableSqlBuilder.this;
            }

            /**
             * Adds a Short parameter to the SQL query.
             *
             * @param value the Short value to be added
             * @return the current SqlBuilder instance, for method chaining
             */
            public CallableSqlBuilder param(final Short value) {
                return callableSqlBuilder.param(value);
            }

            /**
             * {@inheritDoc}
             *
             * @param connection
             */
            public Boolean execute(final Connection connection)
                    throws SQLException {
                return callableSqlBuilder.execute(connection);
            }

            /**
             * Adds a parameter to the SQL query. The method allows chaining
             * and is used to bind values to placeholders in the SQL query.
             *
             * @param type
             * @param value the value of the parameter to be added
             * @return the current SqlBuilder instance, for method chaining
             */
            public CallableSqlBuilderWrapper outParam(final int type,
                                                      final String value) {
                return callableSqlBuilder.outParam(type, value);
            }

            /**
             * Adds a parameter with a null.
             *
             * @return the current SqlBuilder instance, for method chaining
             */
            public CallableSqlBuilder paramNull() {
                return callableSqlBuilder.paramNull();
            }

            /**
             * Adds a parameter to the SQL query. The method allows chaining
             * and is used to bind values to placeholders in the SQL query.
             *
             * @param type
             * @param value the value of the parameter to be added
             * @return the current SqlBuilder instance, for method chaining
             */
            public CallableSqlBuilderWrapper outParam(final int type,
                                                      final Float value) {
                return callableSqlBuilder.outParam(type, value);
            }

            /**
             * Adds a parameter to the SQL query. The method allows chaining
             * and is used to bind values to placeholders in the SQL query.
             *
             * @param value the value of the parameter to be added
             * @return the current SqlBuilder instance, for method chaining
             */
            public CallableSqlBuilder param(final Float value) {
                return callableSqlBuilder.param(value);
            }

            /**
             * Adds a parameter to the SQL query. The method allows chaining
             * and is used to bind values to placeholders in the SQL query.
             *
             * @param value the value of the parameter to be added
             * @return the current SqlBuilder instance, for method chaining
             */
            public CallableSqlBuilder param(final Date value) {
                return callableSqlBuilder.param(value);
            }

            /**
             * Adds a parameter with a specific SQL type and type name as `NULL`
             * to the SQL query.
             * This method is used when the SQL parameter should be set to
             * `NULL`for types
             * that require a type name in addition to the SQL type, such as SQL
             * `STRUCT` or `ARRAY`.
             *
             * @param sqlType  the SQL type of the parameter,
             *                 as defined in {@link Types}
             * @param typeName the type name of the parameter,
             * used for SQL types that require specific type information
             * @return the current SqlBuilder instance, for method chaining
             */
            public CallableSqlBuilder paramNull(final int sqlType,
                                                final String typeName) {
                return callableSqlBuilder.paramNull(sqlType, typeName);
            }

            /**
             * Adds a parameter to the SQL query. The method allows chaining
             * and is used to bind values to placeholders in the SQL query.
             *
             * @param value the value of the parameter to be added
             * @return the current SqlBuilder instance, for method chaining
             */
            public CallableSqlBuilder param(final Integer value) {
                return callableSqlBuilder.param(value);
            }

            /**
             * Adds a parameter to the SQL query. The method allows chaining
             * and is used to bind values to placeholders in the SQL query.
             *
             * @param value the value of the parameter to be added
             * @return the current SqlBuilder instance, for method chaining
             */
            public CallableSqlBuilder param(final BigDecimal value) {
                return callableSqlBuilder.param(value);
            }

            /**
             * Adds a parameter to the SQL query. The method allows chaining
             * and is used to bind values to placeholders in the SQL query.
             *
             * @param type
             * @param value the value of the parameter to be added
             * @return the current SqlBuilder instance, for method chaining
             */
            public CallableSqlBuilderWrapper outParam(final int type,
                                                      final Date value) {
                return callableSqlBuilder.outParam(type, value);
            }

            /**
             * Adds a parameter to the SQL query. The method allows chaining
             * and is used to bind values to placeholders in the SQL query.
             *
             * @param type
             * @param value the value of the parameter to be added
             * @return the current SqlBuilder instance, for method chaining
             */
            public CallableSqlBuilderWrapper outParam(final int type,
                                                      final Time value) {
                return callableSqlBuilder.outParam(type, value);
            }

            /**
             * Adds a parameter to the SQL query. The method allows chaining
             * and is used o bind values to placeholders in the SQL query.
             *
             * @param value the value of the parameter to be added
             * @return the current SqlBuilder instance, for method chaining
             */
            public CallableSqlBuilder param(final Long value) {
                return callableSqlBuilder.param(value);
            }

            /**
             * Adds a parameter to the SQL query. The method allows chaining
             * and is used to bind values to placeholders in the SQL query.
             *
             * @param value the value of the parameter to be added
             * @return the current SqlBuilder instance, for method chaining
             */
            public CallableSqlBuilder param(final Time value) {
                return callableSqlBuilder.param(value);
            }

            /**
             * Set Out Parameter.
             *
             * @param type
             * @return the current SqlBuilder instance, for method chaining
             */
            public CallableSqlBuilderWrapper outParam(final int type) {
                return callableSqlBuilder.outParam(type);
            }

            /**
             * Adds a parameter to the SQL query. The method allows chaining
             * and is used to bind values to placeholders in the SQL query.
             *
             * @param type
             * @param value the value of the parameter to be added
             * @return the current SqlBuilder instance, for method chaining
             */
            public CallableSqlBuilderWrapper outParam(final int type,
                                                      final BigDecimal value) {
                return callableSqlBuilder.outParam(type, value);
            }

            /**
             * Adds a parameter to the SQL query. The method allows chaining
             * and is used to bind values to placeholders in the SQL query.
             *
             * @param type
             * @param value the value of the parameter to be added
             * @return the current SqlBuilder instance, for method chaining
             */
            public CallableSqlBuilderWrapper outParam(final int type,
                                                      final Timestamp value) {
                return callableSqlBuilder.outParam(type, value);
            }

            /**
             * Adds a parameter to the SQL query. The method allows chaining
             * and is used to bind values to placeholders in the SQL query.
             *
             * @param value the value of the parameter to be added
             * @return the current SqlBuilder instance, for method chaining
             */
            public CallableSqlBuilder param(final String value) {
                return callableSqlBuilder.param(value);
            }

            /**
             * Adds a parameter to the SQL query. The method allows chaining
             * and is used to bind values to placeholders in the SQL query.
             *
             * @param type
             * @param value the value of the parameter to be added
             * @return the current SqlBuilder instance, for method chaining
             */
            public CallableSqlBuilderWrapper outParam(final int type,
                                                      final byte[] value) {
                return callableSqlBuilder.outParam(type, value);
            }

            /**
             * Adds a parameter to the SQL query. The method allows chaining
             * and is used to bind values to placeholders in the SQL query.
             *
             * @param type
             * @param value the value of the parameter to be added
             * @return the current SqlBuilder instance, for method chaining
             */
            public CallableSqlBuilderWrapper outParam(final int type,
                                                      final Object value) {
                return callableSqlBuilder.outParam(type, value);
            }

            /**
             * Adds a parameter to the SQL query. The method allows chaining and
             * is used to bind values to placeholders in the SQL query.
             *
             * @param type
             * @param value the value of the parameter to be added
             * @return the current SqlBuilder instance, for method chaining
             */
            public CallableSqlBuilderWrapper outParam(final int type,
                                                      final Double value) {
                return callableSqlBuilder.outParam(type, value);
            }

            /**
             * Adds a parameter to the SQL query. The method allows chaining
             * and is used to bind values to placeholders in the SQL query.
             *
             * @param value the value of the parameter to be added
             * @return the current SqlBuilder instance, for method chaining
             */
            public CallableSqlBuilder param(final Object value) {
                return callableSqlBuilder.param(value);
            }

            /**
             * Adds a parameter to the SQL query. The method allows chaining
             * and is used to bind values to placeholders in the SQL query.
             *
             * @param type
             * @param value the value of the parameter to be added
             * @return the current SqlBuilder instance, for method chaining
             */
            public CallableSqlBuilderWrapper outParam(final int type,
                                                      final Boolean value) {
                return callableSqlBuilder.outParam(type, value);
            }

            /**
             * Adds a parameter to the SQL query. The method allows chaining
             * and is used o bind values to placeholders in the SQL query.
             *
             * @param type
             * @param value the value of the parameter to be added
             * @return the current SqlBuilder instance, for method chaining
             */
            public CallableSqlBuilderWrapper outParam(final int type,
                                                      final Long value) {
                return callableSqlBuilder.outParam(type, value);
            }

            /**
             * Query for Out Parameters.
             * @param mapper
             * @param <T>
             * @return sql
             */
            public <T> Sql<T> queryOutParams(final StatementMapper<T> mapper) {
                return callableSqlBuilder.queryOutParams(mapper);
            }

            /**
             * Adds an Object parameter to the SQL query with targetSqlType.
             *
             * @param value         the Object value to be added
             * @param targetSqlType the targeted SqlType.
             * @return the current SqlBuilder instance, for method chaining
             */
            public CallableSqlBuilder param(final Object value,
                                            final int targetSqlType) {
                return callableSqlBuilder.param(value, targetSqlType);
            }

            /**
             * Adds a parameter to the SQL query. The method allows chaining and
             * is used to bind values to placeholders in the SQL query.
             *
             * @param value the value of the parameter to be added
             * @return the current SqlBuilder instance, for method chaining
             */
            public CallableSqlBuilder param(final Double value) {
                return callableSqlBuilder.param(value);
            }

            /**
             * Adds a parameter to the SQL query. The method allows chaining
             * and is used to bind values to placeholders in the SQL query.
             *
             * @param value the value of the parameter to be added
             * @return the current SqlBuilder instance, for method chaining
             */
            public CallableSqlBuilder param(final byte[] value) {
                return callableSqlBuilder.param(value);
            }

            /**
             * Adds a parameter to the SQL query. The method allows chaining
             * and is used to bind values to placeholders in the SQL query.
             *
             * @param type
             * @param value the value of the parameter to be added
             * @return the current SqlBuilder instance, for method chaining
             */
            public CallableSqlBuilderWrapper outParam(final int type,
                                                      final Integer value) {
                return callableSqlBuilder.outParam(type, value);
            }

            /**
             * Adds a Short parameter to the SQL query.
             *
             * @param type
             * @param value the Short value to be added
             * @return the current SqlBuilder instance, for method chaining
             */
            public CallableSqlBuilderWrapper outParam(final int type,
                                                      final Short value) {
                return callableSqlBuilder.outParam(type, value);
            }

            /**
             * Adds a parameter to the SQL query. The method allows chaining
             * and is used to bind values to placeholders in the SQL query.
             *
             * @param value the value of the parameter to be added
             * @return the current SqlBuilder instance, for method chaining
             */
            public CallableSqlBuilder param(final Boolean value) {
                return callableSqlBuilder.param(value);
            }

            /**
             * Adds a parameter to the SQL query. The method allows chaining
             * and is used to bind values to placeholders in the SQL query.
             *
             * @param value the value of the parameter to be added
             * @return the current SqlBuilder instance, for method chaining
             */
            public CallableSqlBuilder param(final Timestamp value) {
                return callableSqlBuilder.param(value);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Boolean execute(final Connection connection)
                throws SQLException {
            boolean success;
            try (CallableStatement ps = getStatement(connection,
                            this.preparedSqlBuilder.getSql())) {
                success = ps.execute();
            }
            return success;
        }

        /**
         * Get the Statement for Query.
         * @param connection
         * @param theSql
         * @return statement to be executed
         * @throws SQLException
         */
        private CallableStatement getStatement(final Connection connection,
                                                         final String theSql)
                throws SQLException {
            CallableStatement ps = connection.prepareCall(theSql);
            prepare(ps);
            return ps;
        }

        /**
         * Prepares the PreparedStatement by binding all the parameters
         * to their respective positions in the SQL query.
         * @param ps the PreparedStatement to bind parameters to
         * @throws SQLException if a database access error occurs
         * during parameter
         * binding
         * @return prepare
         */
        private PreparedStatement prepare(final PreparedStatement ps)
                throws SQLException {
            return prepare(ps, this.preparedSqlBuilder.paramMappers);
        }

        /**
         * Prepare Statement with Parameters.
         * @param ps
         * @param pMappers
         * @throws SQLException
         * @return ps
         */
        private PreparedStatement prepare(final PreparedStatement ps,
                          final List<PreparedSqlBuilder.ParamMapper> pMappers)
                throws SQLException {
            for (int i = 0; i < pMappers.size(); i++) {
                pMappers.get(i).set(ps, (i + 1));
            }
            return ps;
        }

        /**
         * Adds a parameter with a null.
         * @return the current SqlBuilder instance, for method chaining
         */
        public CallableSqlBuilder paramNull() {
            this.preparedSqlBuilder.paramNull();
            return this;
        }

        /**
         * Adds a parameter with a specific SQL type and type name as `NULL`
         * to the SQL query.
         * This method is used when the SQL parameter should be set to
         * `NULL`for types
         * that require a type name in addition to the SQL type, such as SQL
         * `STRUCT` or `ARRAY`.
         * @param sqlType  the SQL type of the parameter,
         *                 as defined in {@link java.sql.Types}
         * @param typeName the type name of the parameter,
 *                 used for SQL types that require specific type information
         * @return the current SqlBuilder instance, for method chaining
         */
        public CallableSqlBuilder paramNull(final int sqlType,
                                            final String typeName) {
            this.preparedSqlBuilder.paramNull(sqlType, typeName);
            return this;
        }
        /**
         * Adds a parameter to the SQL query. The method allows chaining
         * and is used to bind values to placeholders in the SQL query.
         * @param value the value of the parameter to be added
         * @return the current SqlBuilder instance, for method chaining
         */
        public CallableSqlBuilder param(final Integer value) {
            return inParam(value);
        }
        /**
         * Adds a Short parameter to the SQL query.
         * @param value the Short value to be added
         * @return the current SqlBuilder instance, for method chaining
         */
        public CallableSqlBuilder param(final Short value) {
            return inParam(value);
        }
        /**
         * Adds a parameter to the SQL query. The method allows chaining
         * and is used to bind values to placeholders in the SQL query.
         * @param value the value of the parameter to be added
         * @return the current SqlBuilder instance, for method chaining
         */
        public CallableSqlBuilder param(final String value) {
            return inParam(value);
        }
        /**
         * Adds a parameter to the SQL query. The method allows chaining and
         * is used to bind values to placeholders in the SQL query.
         * @param value the value of the parameter to be added
         * @return the current SqlBuilder instance, for method chaining
         */
        public CallableSqlBuilder param(final Double value) {
            return inParam(value);
        }

        /**
         * Adds a parameter to the SQL query. The method allows chaining
         * and is used to bind values to placeholders in the SQL query.
         * @param value the value of the parameter to be added
         * @return the current SqlBuilder instance, for method chaining
         */
        public CallableSqlBuilder param(final Boolean value) {
            return inParam(value);
        }
        /**
         * Adds a parameter to the SQL query. The method allows chaining
         * and is used o bind values to placeholders in the SQL query.
         * @param value the value of the parameter to be added
         * @return the current SqlBuilder instance, for method chaining
         */
        public CallableSqlBuilder param(final Long value) {
            return inParam(value);
        }
        /**
         * Adds a parameter to the SQL query. The method allows chaining
         * and is used to bind values to placeholders in the SQL query.
         * @param value the value of the parameter to be added
         * @return the current SqlBuilder instance, for method chaining
         */
        public CallableSqlBuilder param(final Date value) {
            return inParam(value);
        }
        /**
         * Adds a parameter to the SQL query. The method allows chaining
         * and is used to bind values to placeholders in the SQL query.
         * @param value the value of the parameter to be added
         * @return the current SqlBuilder instance, for method chaining
         */
        public CallableSqlBuilder param(final Float value) {
            return inParam(value);
        }
        /**
         * Adds a parameter to the SQL query. The method allows chaining
         * and is used to bind values to placeholders in the SQL query.
         * @param value the value of the parameter to be added
         * @return the current SqlBuilder instance, for method chaining
         */
        public CallableSqlBuilder param(final byte[] value) {
            return inParam(value);
        }
        /**
         * Adds a parameter to the SQL query. The method allows chaining
         * and is used to bind values to placeholders in the SQL query.
         * @param value the value of the parameter to be added
         * @return the current SqlBuilder instance, for method chaining
         */
        public CallableSqlBuilder param(final BigDecimal value) {
            return inParam(value);
        }
        /**
         * Adds a parameter to the SQL query. The method allows chaining
         * and is used to bind values to placeholders in the SQL query.
         * @param value the value of the parameter to be added
         * @return the current SqlBuilder instance, for method chaining
         */
        public CallableSqlBuilder param(final Time value) {
            return inParam(value);
        }
        /**
         * Adds a parameter to the SQL query. The method allows chaining
         * and is used to bind values to placeholders in the SQL query.
         *
         * @param value the value of the parameter to be added
         * @return the current SqlBuilder instance, for method chaining
         */
        public CallableSqlBuilder param(final Timestamp value) {
            return inParam(value);
        }
        /**
         * Adds a parameter to the SQL query. The method allows chaining
         * and is used to bind values to placeholders in the SQL query.
         *
         * @param value the value of the parameter to be added
         * @return the current SqlBuilder instance, for method chaining
         */
        public CallableSqlBuilder param(final Object value) {
            return inParam(value);
        }
        /**
         * Adds an Object parameter to the SQL query with targetSqlType.
         *
         * @param value the Object value to be added
         * @param targetSqlType the targeted SqlType.
         * @return the current SqlBuilder instance, for method chaining
         */
        public CallableSqlBuilder param(final Object value,
                                        final int targetSqlType) {
            this.preparedSqlBuilder.param(value, targetSqlType);
            return this;
        }

        /**
         * Set Out Parameter.
         * @param type
         * @return the current SqlBuilder instance, for method chaining
         */
        public CallableSqlBuilderWrapper outParam(final int type) {
            this.preparedSqlBuilder.param((ps, index) -> {
                ((CallableStatement) ps).registerOutParameter(index, type);
            });
            return this.callableSqlBuilderWrapper;
        }

        /**
         * Adds INOUT Parameter.
         * @param type
         * @param value
         * @return the current SqlBuilder instance, for method chaining
         * @param <T>
         */
        private <T> CallableSqlBuilderWrapper inOutParam(final int type,
                                                  final T value) {
            this.preparedSqlBuilder.param((ps, index) -> {
                new PreparedSqlBuilder(this.preparedSqlBuilder.getSql())
                        .param(value).paramMappers.get(0).set(ps, index);
                ((CallableStatement) ps).registerOutParameter(index, type);
            });
            return this.callableSqlBuilderWrapper;
        }

        /**
         * Adds IN Parameter.
         * @param value
         * @return the current SqlBuilder instance, for method chaining
         * @param <T>
         */
        private <T> CallableSqlBuilder inParam(final T value) {
            this.preparedSqlBuilder.param(value);
            return this;
        }

        /**
         * Adds a parameter to the SQL query. The method allows chaining
         * and is used to bind values to placeholders in the SQL query.
         * @param type
         * @param value the value of the parameter to be added
         * @return the current SqlBuilder instance, for method chaining
         */
        public CallableSqlBuilderWrapper outParam(final int type,
                                           final Integer value) {
            return inOutParam(type, value);
        }
        /**
         * Adds a Short parameter to the SQL query.
         * @param type
         * @param value the Short value to be added
         * @return the current SqlBuilder instance, for method chaining
         */
        public CallableSqlBuilderWrapper outParam(final int type,
                                      final Short value) {
            return inOutParam(type, value);
        }
        /**
         * Adds a parameter to the SQL query. The method allows chaining
         * and is used to bind values to placeholders in the SQL query.
         * @param type
         * @param value the value of the parameter to be added
         * @return the current SqlBuilder instance, for method chaining
         */
        public CallableSqlBuilderWrapper outParam(final int type,
                      final String value) {
            return inOutParam(type, value);
        }
        /**
         * Adds a parameter to the SQL query. The method allows chaining and
         * is used to bind values to placeholders in the SQL query.
         * @param type
         * @param value the value of the parameter to be added
         * @return the current SqlBuilder instance, for method chaining
         */
        public CallableSqlBuilderWrapper outParam(final int type,
                                  final Double value) {
            return inOutParam(type, value);
        }

        /**
         * Adds a parameter to the SQL query. The method allows chaining
         * and is used to bind values to placeholders in the SQL query.
         * @param type
         * @param value the value of the parameter to be added
         * @return the current SqlBuilder instance, for method chaining
         */
        public CallableSqlBuilderWrapper outParam(final int type,
                                           final Boolean value) {
            return inOutParam(type, value);
        }
        /**
         * Adds a parameter to the SQL query. The method allows chaining
         * and is used o bind values to placeholders in the SQL query.
         * @param type
         * @param value the value of the parameter to be added
         * @return the current SqlBuilder instance, for method chaining
         */
        public CallableSqlBuilderWrapper outParam(final int type,
                          final Long value) {
            return inOutParam(type, value);
        }
        /**
         * Adds a parameter to the SQL query. The method allows chaining
         * and is used to bind values to placeholders in the SQL query.
         *@param type
         * @param value the value of the parameter to be added
         * @return the current SqlBuilder instance, for method chaining
         */
        public CallableSqlBuilderWrapper outParam(final int type,
                                  final Date value) {
            return inOutParam(type, value);
        }
        /**
         * Adds a parameter to the SQL query. The method allows chaining
         * and is used to bind values to placeholders in the SQL query.
         * @param type
         * @param value the value of the parameter to be added
         * @return the current SqlBuilder instance, for method chaining
         */
        public CallableSqlBuilderWrapper outParam(final int type,
                          final Float value) {
            return inOutParam(type, value);
        }
        /**
         * Adds a parameter to the SQL query. The method allows chaining
         * and is used to bind values to placeholders in the SQL query.
         * @param type
         * @param value the value of the parameter to be added
         * @return the current SqlBuilder instance, for method chaining
         */
        public CallableSqlBuilderWrapper outParam(final int type,
                          final byte[] value) {
            return inOutParam(type, value);
        }
        /**
         * Adds a parameter to the SQL query. The method allows chaining
         * and is used to bind values to placeholders in the SQL query.
         * @param type
         * @param value the value of the parameter to be added
         * @return the current SqlBuilder instance, for method chaining
         */
        public CallableSqlBuilderWrapper outParam(final int type,
                                           final BigDecimal value) {
            return inOutParam(type, value);
        }
        /**
         * Adds a parameter to the SQL query. The method allows chaining
         * and is used to bind values to placeholders in the SQL query.
         * @param type
         * @param value the value of the parameter to be added
         * @return the current SqlBuilder instance, for method chaining
         */
        public CallableSqlBuilderWrapper outParam(final int type,
                              final Time value) {
            return inOutParam(type, value);
        }
        /**
         * Adds a parameter to the SQL query. The method allows chaining
         * and is used to bind values to placeholders in the SQL query.
         * @param type
         * @param value the value of the parameter to be added
         * @return the current SqlBuilder instance, for method chaining
         */
        public CallableSqlBuilderWrapper outParam(final int type,
                                           final Timestamp value) {

            return inOutParam(type, value);
        }
        /**
         * Adds a parameter to the SQL query. The method allows chaining
         * and is used to bind values to placeholders in the SQL query.
         * @param type
         * @param value the value of the parameter to be added
         * @return the current SqlBuilder instance, for method chaining
         */
        public CallableSqlBuilderWrapper outParam(final int type,
                                  final Object value) {
            return inOutParam(type, value);
        }

        /**
         * Query for Out Parameters.
         * @param mapper
         * @return sql
         * @param <T>
         */
        public <T> Sql<T> queryOutParams(final StatementMapper<T> mapper) {
            return connection -> {
                T result;
                try (CallableStatement ps = getStatement(connection,
                        this.preparedSqlBuilder.getSql())) {
                    ps.execute();
                    result = mapper.get(ps);
                }
                return result;
            };
        }

        /**
         *
         * @return callableBatch
         */
        public CallableBatch addBatch() {
            return new CallableBatch();
        }

        /**
         * JDBC Batch Builder.
         */
        public final class CallableBatch {

            /**
             * No ofParams in Batch Statement.
             */
            private final int paramsPerBatch;
            /**
             * No ofParams in Batch Statement.
             */
            private int capacity;

            /**
             * SQL Builder to Hold Batch }Parameters.
             */
            private final PreparedSqlBuilder preparedSqlBuilder;

            /**
             * SQL Builder for the query.
             */

            private CallableBatch() {
                this.paramsPerBatch = CallableSqlBuilder
                        .this.preparedSqlBuilder.paramMappers.size();
                this.capacity = paramsPerBatch;
                this.preparedSqlBuilder = new PreparedSqlBuilder(
                        CallableSqlBuilder.this.preparedSqlBuilder.getSql());
            }

            /**
             * Adds JDBC Batch Builder.
             * @return batch
             */
            public CallableBatch addBatch() throws SQLException {
                validate();
                capacity = capacity + paramsPerBatch;
                return this;
            }

            private void validate() throws SQLException {
                if (this.preparedSqlBuilder.paramMappers.size() != capacity) {
                    throw new SQLException(
                            "Parameters do not match "
                                    + "with first set of parameters");
                }
            }

            /**
             * executes the Batch.
             * @param dataSource
             * @return an array of update counts
             */
            public int[] executeBatch(final DataSource dataSource)
                    throws SQLException {
                validate();
                int[] updatedRows;
                try (Connection connection = dataSource.getConnection();
                     PreparedStatement ps = connection
                             .prepareStatement(preparedSqlBuilder.getSql())) {
                    prepare(ps);

                    updatedRows = ps.executeBatch();
                }
                return updatedRows;
            }

            private void prepare(final PreparedStatement ps)
                    throws SQLException {

                int batchCount = (this.preparedSqlBuilder.paramMappers.size()
                        / this.paramsPerBatch);

                CallableSqlBuilder.this.prepare(ps, CallableSqlBuilder.this
                        .preparedSqlBuilder.paramMappers).addBatch();

                for (int i = 0; i < batchCount; i++) {
                    int from = i * this.paramsPerBatch;
                    CallableSqlBuilder.this.prepare(ps, this.preparedSqlBuilder
                            .paramMappers.subList(from,
                                    from + this.paramsPerBatch)).addBatch();
                }
            }

            /**
             *
             * @return callableBatch
             */
            public CallableBatch paramNull() {
                this.preparedSqlBuilder.paramNull();
                return this;
            }

            /**
             * Adds a parameter with a specific SQL type and type name as `NULL`
             * to the SQL query.
             * This method is used when the SQL parameter should be set to
             * `NULL`for types
             * that require a type name in addition to the SQL type, such as SQL
             * `STRUCT` or `ARRAY`.
             * @param sqlType  the SQL type of the parameter,
             *                 as defined in {@link java.sql.Types}
             * @param typeName the type name of the parameter,
             *                 used for SQL types that require
             *                 specific type information
             * @return the current SqlBuilder instance, for method chaining
             */
            public CallableBatch paramNull(final int sqlType,
                                           final String typeName) {
                this.preparedSqlBuilder.paramNull(sqlType, typeName);
                return this;
            }
            /**
             * Adds a parameter to the SQL query. The method allows chaining
             * and is used to bind values to placeholders in the SQL query.
             * @param <T>
             * @param value the value of the parameter to be added
             * @return the current SqlBuilder instance, for method chaining
             */
            public <T> CallableBatch callableParam(final T value) {
                this.preparedSqlBuilder.param(value);
                return this;
            }
            /**
             * Adds a Short parameter to the SQL query.
             *
             * @param value the Short value to be added
             * @return the current SqlBuilder instance, for method chaining
             */
            public CallableBatch param(final Short value) {
                return callableParam(value);
            }
            /**
             * Adds a parameter to the SQL query. The method allows chaining
             * and is used to bind values to placeholders in the SQL query.
             *
             * @param value the value of the parameter to be added
             * @return the current SqlBuilder instance, for method chaining
             */
            public CallableBatch param(final String value) {
                return callableParam(value);
            }
            /**
             * {@inheritDoc}
             */
            public CallableBatch param(final Double value) {
                return callableParam(value);
            }

            /**
             * {@inheritDoc}
             */
            public CallableBatch param(final Boolean value) {
                return callableParam(value);
            }
            /**
             * {@inheritDoc}
             */
            public CallableBatch param(final Long value) {
                return callableParam(value);
            }
            /**
             * {@inheritDoc}
             */
            public CallableBatch param(final Date value) {
                return callableParam(value);
            }
            /**
             * {@inheritDoc}
             */
            public CallableBatch param(final Float value) {
                return callableParam(value);
            }
            /**
             * {@inheritDoc}
             */
            public CallableBatch param(final byte[] value) {
                return callableParam(value);
            }
            /**
             * {@inheritDoc}
             */
            public CallableBatch param(final BigDecimal value) {
                return callableParam(value);
            }
            /**
             * {@inheritDoc}
             */
            public CallableBatch param(final Time value) {
                return callableParam(value);
            }
            /**
             * {@inheritDoc}
             */
            public CallableBatch param(final Timestamp value) {
                return callableParam(value);
            }
            /**
             * {@inheritDoc}
             */
            public CallableBatch param(final Object value) {
                return callableParam(value);
            }
            /**
             * Adds an Object parameter to the SQL query with targetSqlType.
             *
             * @param value the Object value to be added
             * @param targetSqlType the targeted SqlType.
             * @return the current SqlBuilder instance, for method chaining
             */
            public CallableBatch param(final Object value,
                                       final int targetSqlType) {
                this.preparedSqlBuilder.param(value, targetSqlType);
                return this;
            }
        }


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
    }
}
