package com.techatpark;

import java.math.BigDecimal;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
/**
 * SqlBuilder is a utility class that simplifies the process of constructing
 * SQL queries with dynamic parameters and executing them. It helps developers
 * manage SQL queries more efficiently with less boilerplate code, supporting
 * both query execution and parameterized updates.
 */
public class SqlBuilder implements Sql<Integer> {

    /**
     * The SQL query to be executed.
     */
    private final String sql;
    /**
     * A list of parameters for the query.
     */
    private final List<ParamMapper<?>> paramMappers;

    /**
     * Constructor that initializes the SqlBuilder with a given SQL query.
     *
     * @param theSql the SQL query to be prepared and executed
     */
    public SqlBuilder(final String theSql) {
        this.sql = theSql;
        this.paramMappers = new ArrayList<>();
    }

    /**
     * Adds a parameter with a null.
     *
     * @return the current SqlBuilder instance, for method chaining
     */
    public SqlBuilder paramNull() {
        final int index = this.paramMappers.size() + 1;
        this.paramMappers
                .add(preparedStatement
                        -> preparedStatement.setObject(index, null));
        return this;
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
    public SqlBuilder paramNull(final int sqlType, final String typeName) {
        final int index = this.paramMappers.size() + 1;
        this.paramMappers
                .add(preparedStatement
                        -> preparedStatement.setNull(index, sqlType, typeName));
        return this;
    }

    /**
     * Adds a parameter to the SQL query. The method allows chaining and is used
     * to bind values to placeholders in the SQL query.
     *
     * @param value the value of the parameter to be added
     * @return the current SqlBuilder instance, for method chaining
     */
    public SqlBuilder param(final Integer value) {
        final int index = this.paramMappers.size() + 1;
        this.paramMappers
                .add(preparedStatement
                        -> preparedStatement.setInt(index, value));
        return this;
    }

    /**
     * Adds a Short parameter to the SQL query.
     *
     * @param value the Short value to be added
     * @return the current SqlBuilder instance, for method chaining
     */
    public SqlBuilder param(final Short value) {
        final int index = this.paramMappers.size() + 1;
        this.paramMappers.add(preparedStatement
                -> preparedStatement.setShort(index, value));
        return this;
    }

    /**
     * Adds a parameter to the SQL query. The method allows chaining and is used
     * to bind values to placeholders in the SQL query.
     *
     * @param value the value of the parameter to be added
     * @return the current SqlBuilder instance, for method chaining
     */
    public SqlBuilder param(final String value) {
        final int index = this.paramMappers.size() + 1;
        this.paramMappers
                .add(preparedStatement
                        -> preparedStatement.setString(index, value));
        return this;
    }

    /**
     * Adds a Double parameter to the SQL query.
     *
     * @param value the Double value to be added
     * @return the current SqlBuilder instance, for method chaining
     */
    public SqlBuilder param(final Double value) {
        final int index = this.paramMappers.size() + 1;
        this.paramMappers.add(preparedStatement
                -> preparedStatement.setDouble(index, value));
        return this;
    }

    /**
     * Adds a Boolean parameter to the SQL query.
     *
     * @param value the Boolean value to be added
     * @return the current SqlBuilder instance, for method chaining
     */
    public SqlBuilder param(final Boolean value) {
        final int index = this.paramMappers.size() + 1;
        this.paramMappers.add(preparedStatement
                -> preparedStatement.setBoolean(index, value));
        return this;
    }

    /**
     * Adds a Long parameter to the SQL query.
     *
     * @param value the Long value to be added
     * @return the current SqlBuilder instance, for method chaining
     */
    public SqlBuilder param(final Long value) {
        final int index = this.paramMappers.size() + 1;
        this.paramMappers.add(preparedStatement
                -> preparedStatement.setLong(index, value));
        return this;
    }

    /**
     * Adds a Date parameter to the SQL query.
     *
     * @param value the Date value to be added
     * @return the current SqlBuilder instance, for method chaining
     */
    public SqlBuilder param(final java.sql.Date value) {
        final int index = this.paramMappers.size() + 1;
        this.paramMappers.add(preparedStatement
                -> preparedStatement.setDate(index, value));
        return this;
    }

    /**
     * Adds a Float parameter to the SQL query.
     *
     * @param value the Float value to be added
     * @return the current SqlBuilder instance, for method chaining
     */
    public SqlBuilder param(final Float value) {
        final int index = this.paramMappers.size() + 1;
        this.paramMappers.add(preparedStatement
                -> preparedStatement.setFloat(index, value));
        return this;
    }

    /**
     * Adds a byte array parameter to the SQL query.
     *
     * @param value the byte array to be added
     * @return the current SqlBuilder instance, for method chaining
     */
    public SqlBuilder param(final byte[] value) {
        final int index = this.paramMappers.size() + 1;
        this.paramMappers.add(preparedStatement
                -> preparedStatement.setBytes(index, value));
        return this;
    }

    /**
     * Adds a BigDecimal parameter to the SQL query.
     *
     * @param value the BigDecimal value to be added
     * @return the current SqlBuilder instance, for method chaining
     */
    public SqlBuilder param(final BigDecimal value) {
        final int index = this.paramMappers.size() + 1;
        this.paramMappers.add(preparedStatement
                -> preparedStatement.setBigDecimal(index, value));
        return this;
    }

    /**
     * Adds a Time parameter to the SQL query.
     *
     * @param value the Time value to be added
     * @return the current SqlBuilder instance, for method chaining
     */
    public SqlBuilder param(final java.sql.Time value) {
        final int index = this.paramMappers.size() + 1;
        this.paramMappers.add(preparedStatement
                -> preparedStatement.setTime(index, value));
        return this;
    }

    /**
     * Adds a Timestamp parameter to the SQL query.
     *
     * @param value the Timestamp value to be added
     * @return the current SqlBuilder instance, for method chaining
     */
    public SqlBuilder param(final java.sql.Timestamp value) {
        final int index = this.paramMappers.size() + 1;
        this.paramMappers.add(preparedStatement
                -> preparedStatement.setTimestamp(index, value));
        return this;
    }

    /**
     * Adds an Object parameter to the SQL query.
     *
     * @param value the Object value to be added
     * @return the current SqlBuilder instance, for method chaining
     */
    public SqlBuilder param(final Object value) {
        final int index = this.paramMappers.size() + 1;
        this.paramMappers.add(preparedStatement
                -> preparedStatement.setObject(index, value));
        return this;
    }

    /**
     * Adds an Object parameter to the SQL query with targetSqlType.
     *
     * @param value the Object value to be added
     * @param targetSqlType the targeted SqlType.
     * @return the current SqlBuilder instance, for method chaining
     */
    public SqlBuilder param(final Object value, final int targetSqlType) {
        final int index = this.paramMappers.size() + 1;
        this.paramMappers.add(preparedStatement
                -> preparedStatement.setObject(index, value, targetSqlType));
        return this;
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
        try (PreparedStatement preparedStatement
                     = connection.prepareStatement(sql)) {
            prepare(preparedStatement);
            updatedRows = preparedStatement.executeUpdate();
        }
        return updatedRows;
    }

    /**
     * Provides if record exists.
     *
     * @return a new Query instance for execution
     */
    public SingleValueQuery<Boolean> queryForExists() {
        return new SingleValueQuery<>() {

            @Override
            public Boolean execute(final Connection connection)
                    throws SQLException {
                boolean exists;
                try (PreparedStatement preparedStatement
                             = connection.prepareStatement(sql)) {
                    prepare(preparedStatement);
                    ResultSet resultSet = preparedStatement.executeQuery();

                    exists = resultSet.next();
                }
                return exists;
            }

            @Override
            Boolean getValue(final ResultSet resultSet) throws SQLException {
                return resultSet.next();
            }
        };
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to a Byte.
     *
     * @return a new Query instance for execution
     */
    public SingleValueQuery<Byte> queryForByte() {
        return new SingleValueQuery<>() {
            @Override
            Byte getValue(final ResultSet resultSet) throws SQLException {
                return resultSet.getByte(1);
            }
        };
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to a Byte Array.
     *
     * @return a new Query instance for execution
     */
    public SingleValueQuery<byte[]> queryForBytes() {
        return new SingleValueQuery<>() {
            @Override
            byte[] getValue(final ResultSet resultSet) throws SQLException {
                return resultSet.getBytes(1);
            }
        };
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to an Integer.
     *
     * @return a new Query instance for execution
     */
    public SingleValueQuery<Integer> queryForInt() {
        return new SingleValueQuery<>() {
            @Override
            Integer getValue(final ResultSet resultSet) throws SQLException {
                return resultSet.getInt(1);
            }
        };
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to a Short.
     *
     * @return a new Query instance for execution
     */
    public SingleValueQuery<Short> queryForShort() {
        return new SingleValueQuery<>() {
            @Override
            Short getValue(final ResultSet resultSet) throws SQLException {
                return resultSet.getShort(1);
            }
        };
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to a String.
     *
     * @return a new Query instance for execution
     */
    public SingleValueQuery<String> queryForString() {
        return new SingleValueQuery<>() {
            @Override
            String getValue(final ResultSet resultSet) throws SQLException {
                return resultSet.getString(1);
            }
        };
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to a URL.
     *
     * @return a new Query instance for execution
     */
    public SingleValueQuery<URL> queryForURL() {
        return new SingleValueQuery<>() {
            @Override
            URL getValue(final ResultSet resultSet) throws SQLException {
                return resultSet.getURL(1);
            }
        };
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to a Double.
     *
     * @return a new Query instance for execution
     */
    public SingleValueQuery<Double> queryForDouble() {
        return new SingleValueQuery<>() {
            @Override
            Double getValue(final ResultSet resultSet) throws SQLException {
                return resultSet.getDouble(1);
            }
        };
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to a Float.
     *
     * @return a new Query instance for execution
     */
    public SingleValueQuery<Float> queryForFloat() {
        return new SingleValueQuery<>() {
            @Override
            Float getValue(final ResultSet resultSet) throws SQLException {
                return resultSet.getFloat(1);
            }
        };
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to a BigDecimal.
     *
     * @return a new Query instance for execution
     */
    public SingleValueQuery<BigDecimal> queryForBigDecimal() {
        return new SingleValueQuery<>() {
            @Override
            BigDecimal getValue(final ResultSet resultSet) throws SQLException {
                return resultSet.getBigDecimal(1);
            }
        };
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to a Boolean.
     *
     * @return a new Query instance for execution
     */
    public SingleValueQuery<Boolean> queryForBoolean() {
        return new SingleValueQuery<>() {
            @Override
            Boolean getValue(final ResultSet resultSet) throws SQLException {
                return resultSet.getBoolean(1);
            }
        };
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to a Long.
     *
     * @return a new Query instance for execution
     */
    public SingleValueQuery<Long> queryForLong() {
        return new SingleValueQuery<>() {
            @Override
            Long getValue(final ResultSet resultSet) throws SQLException {
                return resultSet.getLong(1);
            }
        };
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to a Date.
     *
     * @return a new Query instance for execution
     */
    public SingleValueQuery<java.sql.Date> queryForDate() {
        return new SingleValueQuery<>() {
            @Override
            java.sql.Date getValue(final ResultSet resultSet)
                    throws SQLException {
                return resultSet.getDate(1);
            }
        };
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to a Time.
     *
     * @return a new Query instance for execution
     */
    public SingleValueQuery<java.sql.Time> queryForTime() {
        return new SingleValueQuery<>() {
            @Override
            java.sql.Time getValue(final ResultSet resultSet)
                    throws SQLException {
                return resultSet.getTime(1);
            }
        };
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to a Timestamp.
     *
     * @return a new Query instance for execution
     */
    public SingleValueQuery<java.sql.Timestamp> queryForTimestamp() {
        return new SingleValueQuery<>() {
            @Override
            java.sql.Timestamp getValue(final ResultSet resultSet)
                    throws SQLException {
                return resultSet.getTimestamp(1);
            }
        };
    }

    /**
     * Creates a new Query object that can be used to execute
     * a SELECT query and map the result set to an Object.
     *
     * @return a new Query instance for execution
     */
    public SingleValueQuery<Object> queryForObject() {
        return new SingleValueQuery<>() {
            @Override
            Object getValue(final ResultSet resultSet) throws SQLException {
                return resultSet.getObject(1);
            }
        };
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
     * RowMapper is an interface that defines how to map each row of a ResultSet
     * to a Java object.
     *
     * @param <T> the type of object to map the result set to
     */
    public interface RowMapper<T> {
        /**
         * Maps a single row of the result set to an object.
         *
         * @param resultSet the result set obtained from executing the SQL query
         * @return the mapped object
         * @throws SQLException if an SQL error occurs during mapping
         */
        T mapRow(ResultSet resultSet) throws SQLException;
    }

    /**
     * Functional interface representing a parameter mapper
     * that binds parameters
     * to a {@link PreparedStatement}. Implementations of this interface are
     * responsible for mapping a specific parameter
     * to the appropriate placeholder
     * in the SQL query.
     *
     * @param <T> the type of the parameter to be mapped
     */
    public interface ParamMapper<T> {

        /**
         * Binds the provided parameters to the placeholders in the given
         * {@link PreparedStatement}. This method is called to map and set
         * parameter values for the SQL query.
         *
         * @param preparedStatement the {@link PreparedStatement}
         *                          to bind parameters to
         * @throws SQLException if a database access error occurs or if
         *                      parameter binding fails
         */
        void mapParam(PreparedStatement preparedStatement) throws SQLException;
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

        T mapRow(final ResultSet resultSet) throws SQLException {
            return rowMapper.mapRow(resultSet);
        }
    }

    /**
     * Query to get Single Value.
     * @param <T> type of value
     */
    public abstract class SingleValueQuery<T> extends SingleRecordQuery<T> {

        /**
         * Private constructor for creating a Query instance.
         */
        private SingleValueQuery() {
            super(null);
        }

        @Override
        T mapRow(final ResultSet resultSet) throws SQLException {
            return getValue(resultSet);
        }

        /**
         * Gets Value from Result set.
         * @param resultSet
         * @return value
         * @throws SQLException
         */
        abstract T getValue(ResultSet resultSet) throws SQLException;
    }

    public class SingleRecordQuery<T> extends Query<T> implements Sql<T> {

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
            T result = null;
            try (PreparedStatement preparedStatement
                         = connection.prepareStatement(sql)) {
                prepare(preparedStatement);
                ResultSet resultSet = preparedStatement.executeQuery();

                if (resultSet.next()) {
                    result = mapRow(resultSet);
                }
            }
            return result;
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
            List<T> result = new ArrayList<>();
            try (PreparedStatement preparedStatement
                         = connection.prepareStatement(sql)) {
                prepare(preparedStatement);
                ResultSet resultSet = preparedStatement.executeQuery();

                while (resultSet.next()) {
                    result.add(mapRow(resultSet));
                }
            }
            return result;
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
            T result = null;
            try (PreparedStatement preparedStatement
                         = connection.prepareStatement(sql,
                    java.sql.Statement.RETURN_GENERATED_KEYS)) {
                prepare(preparedStatement);
                preparedStatement.executeUpdate();
                ResultSet resultSet = preparedStatement.getGeneratedKeys();

                if (resultSet.next()) {
                    result = mapRow(resultSet);
                }
            }
            return result;
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
            List<T> result = new ArrayList<>();
            try (PreparedStatement preparedStatement
                         = connection.prepareStatement(sql,
                    java.sql.Statement.RETURN_GENERATED_KEYS)) {
                prepare(preparedStatement);
                preparedStatement.executeUpdate();
                ResultSet resultSet = preparedStatement.getGeneratedKeys();

                while (resultSet.next()) {
                    result.add(mapRow(resultSet));
                }
            }
            return result;
        }
    }
    /**
     * Prepares the PreparedStatement by binding all the parameters
     * to their respective
     * positions in the SQL query.
     *
     * @param preparedStatement the PreparedStatement to bind parameters to
     * @throws SQLException if a database access error occurs during parameter
     * binding
     */
    private void prepare(final PreparedStatement preparedStatement)
            throws SQLException {
        for (ParamMapper<?> paramMapper: paramMappers) {
            paramMapper.mapParam(preparedStatement);
        }
    }
}
