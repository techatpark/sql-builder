package com.techatpark;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

class TransactionTest extends BaseTest {



    @Test
    void testTransaction() throws SQLException {

        Transaction
            .begin(SqlBuilder.sql("INSERT INTO movie ( title ,directed_by ) VALUES ( ? ,? )")
                    .param("Inception")
                    .param("Christopher Nolan"))
            // Invalid Insert (title can not be null). Should Fail.
            .thenApply(value -> SqlBuilder.sql("INSERT INTO movie ( title ,directed_by ) VALUES ( ? ,? )")
                    .param("Dunkrik")
                    .param("Christopher Nolan"))
            .commit(dataSource);


        Assertions.assertEquals(2,
                SqlBuilder.sql("SELECT id, title, directed_by from movie")
                        .queryForList(BaseTest::mapMovie)
                        .execute(dataSource).size());

    }

    @Test
    void testInvalidTransaction() throws SQLException {

        Assertions.assertThrows(SQLException.class, () -> {
            Transaction
                    .begin(SqlBuilder.sql("INSERT INTO movie ( title ,directed_by ) VALUES ( ? ,? )")
                            .param("Inception")
                            .param("Christopher Nolan"))
                    // Invalid Insert (title can not be null). Should Fail.
                    .thenApply(value -> SqlBuilder.sql("INSERT INTO movie ( title ,directed_by ) VALUES ( NULL ,? )")
                            .param("Christopher Nolan"))
                    .commit(dataSource);
        });

        Assertions.assertEquals(0,
                SqlBuilder.sql("SELECT id, title, directed_by from movie")
                .queryForList(BaseTest::mapMovie)
                .execute(dataSource).size());

    }

    @Test
    void testInvalidConnection() throws SQLException {
        DataSource dataSource1 = new DataSource() {
            @Override
            public Connection getConnection() throws SQLException {
                throw new SQLException("Invalid");
            }

            @Override
            public Connection getConnection(final String username, final String password) throws SQLException {
                return null;
            }

            @Override
            public PrintWriter getLogWriter() throws SQLException {
                return null;
            }

            @Override
            public void setLogWriter(final PrintWriter out) throws SQLException {

            }

            @Override
            public void setLoginTimeout(final int seconds) throws SQLException {

            }

            @Override
            public int getLoginTimeout() throws SQLException {
                return 0;
            }

            @Override
            public <T> T unwrap(final Class<T> iface) throws SQLException {
                return null;
            }

            @Override
            public boolean isWrapperFor(final Class<?> iface) throws SQLException {
                return false;
            }

            @Override
            public Logger getParentLogger() throws SQLFeatureNotSupportedException {
                return null;
            }
        };

        Assertions.assertThrows(SQLException.class, () -> {
            Transaction
                    .begin(SqlBuilder.sql("INSERT INTO movie ( title ,directed_by ) VALUES ( ? ,? )")
                            .param("Inception")
                            .param("Christopher Nolan"))
                    .commit(dataSource1);
        });

    }
}