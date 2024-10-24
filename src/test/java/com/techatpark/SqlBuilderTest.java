package com.techatpark;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

class SqlBuilderTest extends BaseTest {

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
                            .queryForOne(this::mapRow)
                            .execute(dataSource);

        Assertions.assertEquals("Dunkirk", movie.title());

        Assertions.assertEquals(2,
                new SqlBuilder("SELECT id, title, directed_by from movie")
                    .queryForList(this::mapRow)
                    .execute(dataSource)
                    .size());

    }





}
