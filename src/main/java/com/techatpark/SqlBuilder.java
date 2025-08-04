package com.techatpark;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
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
     * A list of parameters for the query.
     */
    private final List<ParamMapper> paramMappers;

    /**
     * Builds Sql Builder from Sql.
     *
     * @param theSql the SQL query to be prepared and executed
     * @return sqlBuilder
     */
    public static PreparedSqlBuilder prepareSql(final String theSql) {
        return new PreparedSqlBuilder(theSql);
    }

    /**
     * Constructor that initializes the SqlBuilder with a given SQL query.
     *
     * @param theSql the SQL query to be prepared and executed
     */
    protected SqlBuilder(final String theSql) {
        this.sql = theSql;
        this.paramMappers = new ArrayList<>();
    }

    /**
     * Executes an update (such as INSERT, UPDATE, DELETE) using the prepared
     * SQL query
     * and the bound parameters.
     *
     * @param connection the database connection used to execute the query
     * @return the number of rows affected by the update
     * @throws SQLException if a database access error occurs
     */
    @Override
    public Integer execute(final Connection connection) throws SQLException {
        int updatedRows;
        try (PreparedStatement ps = getStatement(connection, sql)) {
            updatedRows = ps.executeUpdate();
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
    public SingleRecordQuery<Byte> queryForByte() {
        return new SingleRecordQuery<>(rs -> rs.getByte(1));
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to a Byte Array.
     *
     * @return a new Query instance for execution
     */
    public SingleRecordQuery<byte[]> queryForBytes() {
        return new SingleRecordQuery<>(rs  -> rs.getBytes(1));
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to an Integer.
     *
     * @return a new Query instance for execution
     */
    public SingleRecordQuery<Integer> queryForInt() {
        return new SingleRecordQuery<>(rs -> rs.getInt(1));
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to a Short.
     *
     * @return a new Query instance for execution
     */
    public SingleRecordQuery<Short> queryForShort() {
        return new SingleRecordQuery<>(rs -> rs.getShort(1));
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to a String.
     *
     * @return a new Query instance for execution
     */
    public SingleRecordQuery<String> queryForString() {
        return new SingleRecordQuery<>(rs -> rs.getString(1));
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to a URL.
     *
     * @return a new Query instance for execution
     */
    public SingleRecordQuery<URL> queryForURL() {
        return new SingleRecordQuery<>(rs -> rs.getURL(1));
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to a Double.
     *
     * @return a new Query instance for execution
     */
    public SingleRecordQuery<Double> queryForDouble() {
        return new SingleRecordQuery<>(rs -> rs.getDouble(1));
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to a Float.
     *
     * @return a new Query instance for execution
     */
    public SingleRecordQuery<Float> queryForFloat() {
        return new SingleRecordQuery<>(rs -> rs.getFloat(1));
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to a BigDecimal.
     *
     * @return a new Query instance for execution
     */
    public SingleRecordQuery<BigDecimal> queryForBigDecimal() {
        return new SingleRecordQuery<>(rs -> rs.getBigDecimal(1));
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to a Boolean.
     *
     * @return a new Query instance for execution
     */
    public SingleRecordQuery<Boolean> queryForBoolean() {
        return new SingleRecordQuery<>(rs -> rs.getBoolean(1));
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to a Long.
     *
     * @return a new Query instance for execution
     */
    public SingleRecordQuery<Long> queryForLong() {
        return new SingleRecordQuery<>(rs -> rs.getLong(1));
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to a Date.
     *
     * @return a new Query instance for execution
     */
    public SingleRecordQuery<java.sql.Date> queryForDate() {
        return new SingleRecordQuery<>(rs -> rs.getDate(1));
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to a Time.
     *
     * @return a new Query instance for execution
     */
    public SingleRecordQuery<java.sql.Time> queryForTime() {
        return new SingleRecordQuery<>(rs -> rs.getTime(1));
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to a Timestamp.
     *
     * @return a new Query instance for execution
     */
    public SingleRecordQuery<java.sql.Timestamp> queryForTimestamp() {
        return new SingleRecordQuery<>(rs -> rs.getTimestamp(1));
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to an Object.
     *
     * @return a new Query instance for execution
     */
    public SingleRecordQuery<Object> queryForObject() {
        return new SingleRecordQuery<>(rs -> rs.getObject(1));
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
    public <T> SqlBuilder.SingleRecordQuery<T> queryForOne(
            final RowMapper<T> rowMapper) {
        return this.new SingleRecordQuery<>(rowMapper);
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
    public <T> SqlBuilder.MultipleRecordQuery<T> queryForList(
            final RowMapper<T> rowMapper) {
        return this.new MultipleRecordQuery<>(rowMapper);
    }

    /**
     * Checkes if Record Exists.
     * @param connection
     * @return exists
     * @throws SQLException
     */
    protected boolean exists(final Connection connection) throws SQLException {
        boolean exists;
        try (PreparedStatement ps = getStatement(connection, sql)) {
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
    protected <T> T getResult(final Query<T> query,
                        final Connection connection) throws SQLException {
        T result = null;
        try (PreparedStatement ps
                     = getStatement(connection, sql)) {
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    result = query.mapRow(rs);
                }
            }
        }
        return result;
    }
    /**
     * Get Result as a List for a Query.
     * @param query
     * @param connection
     * @return result
     * @param <T>
     * @throws SQLException
     */
    protected <T> List<T> getResultAsList(final Query<T> query,
                        final Connection connection) throws SQLException {
        List<T> result = new ArrayList<>();
        try (PreparedStatement ps = getStatement(connection, sql)) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.add(query.mapRow(rs));
                }
            }
        }
        return result;
    }
    /**
     * Get Generated Keys for a Query.
     * @param query
     * @param connection
     * @return result
     * @param <T>
     * @throws SQLException
     */
    protected <T> T getGeneratedKeys(final Query<T> query,
                             final Connection connection) throws SQLException {
        T result = null;
        try (PreparedStatement ps = getStatement(connection, sql,
                java.sql.Statement.RETURN_GENERATED_KEYS)) {
            ps.executeUpdate();
            try (ResultSet rs = ps.getGeneratedKeys()) {
                if (rs.next()) {
                    result = query.mapRow(rs);
                }
            }
        }
        return result;
    }
    /**
     * Get Generated Keys as List for a Query.
     * @param query
     * @param connection
     * @return result
     * @param <T>
     * @throws SQLException
     */
    protected <T> List<T> getGeneratedKeysAsList(final Query<T> query,
                         final Connection connection) throws SQLException {
        List<T> result = new ArrayList<>();
        try (PreparedStatement ps = getStatement(connection, sql,
                java.sql.Statement.RETURN_GENERATED_KEYS)) {
            ps.executeUpdate();
            try (ResultSet rs = ps.getGeneratedKeys()) {
                while (rs.next()) {
                    result.add(query.mapRow(rs));
                }
            }
        }
        return result;
    }

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

    /**
     * The Query class encapsulates the logic for executing
     * SELECT queries and mapping
     * the results to Java objects using a RowMapper.
     *
     * @param <T> the type of object to map the result set to
     */
    public static class Query<T> {

        /**
         * Mapper for Result set.
         */
        private final RowMapper<T> rowMapper;

        /**
         * Private constructor for creating a Query instance with
         * the specified RowMapper.
         *
         * @param theRowMapper the RowMapper used to map the ResultSet rows
         */
        private Query(final RowMapper<T> theRowMapper) {
            this.rowMapper = theRowMapper;
        }

        T mapRow(final ResultSet rs) throws SQLException {
            return rowMapper.get(rs);
        }
    }

    public final class SingleRecordQuery<T> extends Query<T> implements Sql<T> {

        /**
         * Private constructor for creating a Query instance with
         * the specified RowMapper.
         *
         * @param theRowMapper the RowMapper used to map the ResultSet rows
         */
        private SingleRecordQuery(final RowMapper<T> theRowMapper) {
            super(theRowMapper);
        }

        /**
         * Executes the SQL query and returns a single mapped result
         * from the ResultSet.
         *
         * @param connection the database connection used to execute the query
         * @return the single mapped result, or null if no result is found
         * @throws SQLException if a database access error occurs
         */
        @Override
        public T execute(final Connection connection) throws SQLException {
            return getResult(this, connection);
        }
    }

    public final class MultipleRecordQuery<T>
            extends Query<T> implements Sql<List<T>> {

        /**
         * Private constructor for creating a Query instance with
         * the specified RowMapper.
         *
         * @param theRowMapper the RowMapper used to map the ResultSet rows
         */
        private MultipleRecordQuery(final RowMapper<T> theRowMapper) {
            super(theRowMapper);
        }

        /**
         * Executes the SQL query and returns a list of mapped result
         * from the ResultSet.
         *
         * @param connection the database connection used to execute the query
         * @return the list of mapped result, or empty if no result is found
         * @throws SQLException if a database access error occurs
         */
        @Override
        public List<T> execute(final Connection connection)
                throws SQLException {
            return getResultAsList(this, connection);
        }
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
    public <T> SqlBuilder.SingleGeneratedKeysQuery<T> queryGeneratedKeys(
            final RowMapper<T> rowMapper) {
        return this.new SingleGeneratedKeysQuery<>(rowMapper);
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
    public <T> SqlBuilder.MultipleGeneratedKeysQuery<T>
            queryGeneratedKeysAsList(final RowMapper<T> rowMapper) {
        return this.new MultipleGeneratedKeysQuery<>(rowMapper);
    }

    /**
     * Single GeneratedKeys Query.
     * @param <T>
     */
    public final class SingleGeneratedKeysQuery<T>
            extends Query<T> implements Sql<T> {

        /**
         * Private constructor for creating a Query instance with
         * the specified RowMapper.
         *
         * @param theRowMapper the RowMapper used to map the ResultSet rows
         */
        private SingleGeneratedKeysQuery(final RowMapper<T> theRowMapper) {
            super(theRowMapper);
        }

        /**
         * Executes the SQL query and returns a single mapped result
         * from the ResultSet.
         *
         * @param connection the database connection used to execute the query
         * @return the single mapped result, or null if no result is found
         * @throws SQLException if a database access error occurs
         */
        @Override
        public T execute(final Connection connection) throws SQLException {
            return getGeneratedKeys(this, connection);
        }
    }

    /**
     * Multiple Generated Keys Query.
     * @param <T>
     */
    public final class MultipleGeneratedKeysQuery<T>
            extends Query<T> implements Sql<List<T>> {

        /**
         * Private constructor for creating a Query instance with
         * the specified RowMapper.
         *
         * @param theRowMapper the RowMapper used to map the ResultSet rows
         */
        private MultipleGeneratedKeysQuery(final RowMapper<T> theRowMapper) {
            super(theRowMapper);
        }

        /**
         * Executes the SQL query and returns a list of mapped result
         * from the ResultSet.
         *
         * @param connection the database connection used to execute the query
         * @return the list of mapped result, or empty if no result is found
         * @throws SQLException if a database access error occurs
         */
        @Override
        public List<T> execute(final Connection connection)
                throws SQLException {
            return getGeneratedKeysAsList(this, connection);
        }
    }
    /**
     * Prepares the PreparedStatement by binding all the parameters
     * to their respective
     * positions in the SQL query.
     *
     * @param ps the PreparedStatement to bind parameters to
     * @throws SQLException if a database access error occurs during parameter
     * binding
     */
    private void prepare(final PreparedStatement ps)
            throws SQLException {
        prepareWithMappers(ps, this.paramMappers);
    }

    /**
     * Prepare Statement with Parameters.
     * @param ps
     * @param pMappers
     * @throws SQLException
     */
    protected void prepareWithMappers(final PreparedStatement ps,
                                    final List<ParamMapper> pMappers)
            throws SQLException {
        for (int i = 0; i < pMappers.size(); i++) {
            pMappers.get(i).set(ps, (i + 1));
        }
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
     * Get the Statement for Query.
     * @param connection
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

    public static final class PreparedSqlBuilder extends SqlBuilder {
        /**
         * Constructor that initializes the SqlBuilder with a given SQL query.
         *
         * @param theSql the SQL query to be prepared and executed
         */
        private PreparedSqlBuilder(final String theSql) {
            super(theSql);
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
            super.paramMappers.add(paramMapper);
            return this;
        }

        /**
         * Builds JDBC Batch Builder.
         * @return batch
         */
        public Batch addBatch() {
            return this.new Batch();
        }

        /**
         * JDBC Batch Builder.
         */
        public final class Batch {

            /**
             * No ofParams in Batch Statement.
             */
            private final int paramsPerBatch;
            /**
             * No ofParams in Batch Statement.
             */
            private int capacity;

            /**
             * SQL Builder for the query.
             */

            private Batch() {
                this.paramsPerBatch = PreparedSqlBuilder
                        .super.paramMappers.size();
                this.capacity = 2 * paramsPerBatch;
            }

            /**
             * Adds JDBC Batch Builder.
             * @return batch
             */
            public Batch addBatch() throws SQLException {
                if (PreparedSqlBuilder.super.paramMappers.size() == capacity) {
                    capacity = capacity + paramsPerBatch;
                    return this;
                }
                throw new SQLException(
                        "Parameters do not match with first set of parameters");
            }

            /**
             * executes the Batch.
             * @param dataSource
             * @return an array of update counts
             */
            public int[] executeBatch(final DataSource dataSource)
                    throws SQLException {
                int[] updatedRows;
                try (Connection connection = dataSource.getConnection();
                     PreparedStatement ps = connection
                             .prepareStatement(PreparedSqlBuilder.super.sql)) {
                    prepare(ps);

                    updatedRows = ps.executeBatch();
                }
                return updatedRows;
            }

            private void prepare(final PreparedStatement ps)
                    throws SQLException {

                int batchCount = (PreparedSqlBuilder.super.paramMappers.size()
                        / this.paramsPerBatch);

                for (int i = 0; i < batchCount; i++) {
                    int from = i * this.paramsPerBatch;
                    prepareWithMappers(ps, PreparedSqlBuilder.super
                            .paramMappers.subList(from,
                                    from + this.paramsPerBatch));
                    ps.addBatch();
                }
            }
            /**
             * Adds a parameter with a null.
             *
             * @return the current SqlBuilder instance, for method chaining
             */
            public Batch paramNull() {
                PreparedSqlBuilder.this.paramNull();
                return this;
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
     *                 as defined in {@link java.sql.Types}
             * @param typeName the type name of the parameter,
     *                 used for SQL types that require specific type information
             * @return the current SqlBuilder instance, for method chaining
             */
            public Batch paramNull(final int sqlType,
                                   final String typeName) {
                PreparedSqlBuilder.this.paramNull(sqlType, typeName);
                return this;
            }
            /**
             * Adds a parameter to the SQL query. The method allows chaining
             * and is used to bind values to placeholders in the SQL query.
             *
             * @param value the value of the parameter to be added
             * @return the current SqlBuilder instance, for method chaining
             */
            public Batch param(final Integer value) {
                PreparedSqlBuilder.this.param(value);
                return this;
            }
            /**
             * Adds a Short parameter to the SQL query.
             *
             * @param value the Short value to be added
             * @return the current SqlBuilder instance, for method chaining
             */
            public Batch param(final Short value) {
                PreparedSqlBuilder.this.param(value);
                return this;
            }
            /**
             * Adds a parameter to the SQL query. The method allows chaining
             * and is used to bind values to placeholders in the SQL query.
             *
             * @param value the value of the parameter to be added
             * @return the current SqlBuilder instance, for method chaining
             */
            public Batch param(final String value) {
                PreparedSqlBuilder.this.param(value);
                return this;
            }
            /**
             * Adds a parameter to the SQL query. The method allows chaining and
             * is used to bind values to placeholders in the SQL query.
             *
             * @param value the value of the parameter to be added
             * @return the current SqlBuilder instance, for method chaining
             */
            public Batch param(final Double value) {
                PreparedSqlBuilder.this.param(value);
                return this;
            }

            /**
             * Adds a parameter to the SQL query. The method allows chaining
             * and is used to bind values to placeholders in the SQL query.
             *
             * @param value the value of the parameter to be added
             * @return the current SqlBuilder instance, for method chaining
             */
            public Batch param(final Boolean value) {
                PreparedSqlBuilder.this.param(value);
                return this;
            }
            /**
             * Adds a parameter to the SQL query. The method allows chaining
             * tand is used o bind values to placeholders in the SQL query.
             *
             * @param value the value of the parameter to be added
             * @return the current SqlBuilder instance, for method chaining
             */
            public Batch param(final Long value) {
                PreparedSqlBuilder.this.param(value);
                return this;
            }
            /**
             * Adds a parameter to the SQL query. The method allows chaining
             * and is used to bind values to placeholders in the SQL query.
             *
             * @param value the value of the parameter to be added
             * @return the current SqlBuilder instance, for method chaining
             */
            public Batch param(final Date value) {
                PreparedSqlBuilder.this.param(value);
                return this;
            }
            /**
             * Adds a parameter to the SQL query. The method allows chaining
             * and is used to bind values to placeholders in the SQL query.
             *
             * @param value the value of the parameter to be added
             * @return the current SqlBuilder instance, for method chaining
             */
            public Batch param(final Float value) {
                PreparedSqlBuilder.this.param(value);
                return this;
            }
            /**
             * Adds a parameter to the SQL query. The method allows chaining
             * and is used to bind values to placeholders in the SQL query.
             *
             * @param value the value of the parameter to be added
             * @return the current SqlBuilder instance, for method chaining
             */
            public Batch param(final byte[] value) {
                PreparedSqlBuilder.this.param(value);
                return this;
            }
            /**
             * Adds a parameter to the SQL query. The method allows chaining
             * and is used to bind values to placeholders in the SQL query.
             *
             * @param value the value of the parameter to be added
             * @return the current SqlBuilder instance, for method chaining
             */
            public Batch param(final BigDecimal value) {
                PreparedSqlBuilder.this.param(value);
                return this;
            }
            /**
             * Adds a parameter to the SQL query. The method allows chaining
             * and is used to bind values to placeholders in the SQL query.
             *
             * @param value the value of the parameter to be added
             * @return the current SqlBuilder instance, for method chaining
             */
            public Batch param(final Time value) {
                PreparedSqlBuilder.this.param(value);
                return this;
            }
            /**
             * Adds a parameter to the SQL query. The method allows chaining
             * and is used to bind values to placeholders in the SQL query.
             *
             * @param value the value of the parameter to be added
             * @return the current SqlBuilder instance, for method chaining
             */
            public Batch param(final Timestamp value) {
                PreparedSqlBuilder.this.param(value);
                return this;
            }
            /**
             * Adds a parameter to the SQL query. The method allows chaining
             * and is used to bind values to placeholders in the SQL query.
             *
             * @param value the value of the parameter to be added
             * @return the current SqlBuilder instance, for method chaining
             */
            public Batch param(final Object value) {
                PreparedSqlBuilder.this.param(value);
                return this;
            }
            /**
             * Adds an Object parameter to the SQL query with targetSqlType.
             *
             * @param value the Object value to be added
             * @param targetSqlType the targeted SqlType.
             * @return the current SqlBuilder instance, for method chaining
             */
            public Batch param(final Object value, final int targetSqlType) {
                PreparedSqlBuilder.this.param(value, targetSqlType);
                return this;
            }
        }
    }
}
