package com.techatpark;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

class SqlBuilderTest {

    private final JdbcDataSource dataSource;

    SqlBuilderTest() {
        try {
            // Setup
            dataSource = new JdbcDataSource();
            dataSource.setURL("jdbc:h2:~/test");
            dataSource.setUser("sa");
            dataSource.setPassword("sa");

            new SqlBuilder("DROP TABLE movie IF EXISTS")
                    .execute(dataSource);

            new SqlBuilder("""
                    CREATE TABLE movie (
                        id bigint auto_increment PRIMARY KEY,
                        title VARCHAR(80) NOT NULL,
                        directed_by VARCHAR(80)
                    )
                    """)
                    .execute(dataSource);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeEach
    void beforeEach() throws SQLException {
        new SqlBuilder("""
                TRUNCATE TABLE movie
                """)
                .execute(dataSource);
    }

    @Test
    void testSQL() throws SQLException {

        int updateRows =
                new SqlBuilder("INSERT INTO movie(title, directed_by) VALUES (?, ?), (?, ?)")
                        .param("Dunkirk")
                        .param("Nolan")
                        .param("Inception")
                        .param("Nolan")
                    .execute(dataSource);

        Assertions.assertEquals(2, updateRows);

        final String sql = "SELECT id, title, directed_by from movie where id = ?";

        Movie movie = new SqlBuilder(sql)
                                .param(1)
                            .queryForOne(SqlBuilderTest::mapRow)
                            .execute(dataSource);

        Assertions.assertEquals("Dunkirk", movie.title());

        Assertions.assertEquals(2,
                new SqlBuilder("SELECT id, title, directed_by from movie")
                    .queryForList(SqlBuilderTest::mapRow)
                    .execute(dataSource)
                    .size());

    }

    @Test
    void testTransaction() throws SQLException {

        Assertions.assertThrows(SQLException.class, () -> {
            Transaction
                    .begin(new SqlBuilder("INSERT INTO movie ( title ,directed_by ) VALUES ( ? ,? )")
                                        .param("Inception")
                                        .param("Christopher Nolan"))
                        // Invalid Insert (title can not be null). Should Fail.
                        .thenApply(value -> new SqlBuilder("INSERT INTO movie ( title ,directed_by ) VALUES ( NULL ,? )")
                                .param("Christopher Nolan"))
                    .commit(dataSource);
        });

        Assertions.assertEquals(0, new SqlBuilder("SELECT id, title, directed_by from movie").queryForList(SqlBuilderTest::mapRow).execute(dataSource).size());

    }

    private static Movie mapRow(ResultSet rs) throws SQLException {
        return new Movie(
                rs.getShort(1),
                rs.getString(2),
                rs.getString(3));
    }

}
