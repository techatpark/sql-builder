package com.techatpark;

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
    private final List<ParamMapper<?>> parameters;

    /**
     * Constructor that initializes the SqlBuilder with a given SQL query.
     *
     * @param theSql the SQL query to be prepared and executed
     */
    public SqlBuilder(final String theSql) {
        this.sql = theSql;
        this.parameters = new ArrayList<>();
    }

    /**
     * Adds a parameter to the SQL query. The method allows chaining and is used
     * to bind values to placeholders in the SQL query.
     *
     * @param value the value of the parameter to be added
     * @return the current SqlBuilder instance, for method chaining
     */
    public SqlBuilder param(final Integer value) {
        final int index = this.parameters.size() + 1;
        this.parameters
                .add((preparedStatement)
                        -> preparedStatement.setInt(index, value));
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
        final int index = this.parameters.size() + 1;
        this.parameters
                .add((preparedStatement)
                        -> preparedStatement.setString(index, value));
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
    public <T> SqlBuilder.MultipleRecordQuery<T> query(
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
        for (ParamMapper<?> paramMapper: parameters) {
            paramMapper.mapParam(preparedStatement);
        }
    }
}
