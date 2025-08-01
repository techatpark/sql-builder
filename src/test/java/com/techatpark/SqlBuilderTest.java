package com.techatpark;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.IntStream;

class SqlBuilderTest extends BaseTest {

    @BeforeEach
    void init() throws SQLException {
        SqlBuilder.sql("DELETE FROM movie")
                .execute(dataSource);
    }

    @Test
    void testSQL() throws SQLException {
        int updateRows =
                SqlBuilder.sql("INSERT INTO movie(title, directed_by) VALUES (?, ?), (?, ?)")
                        .param("Dunkirk")
                        .param("Nolan")
                        .param("Inception")
                        .param("Nolan")
                    .execute(dataSource);

        Assertions.assertEquals(2, updateRows);

        long generetedId = SqlBuilder.sql("INSERT INTO movie(title, directed_by) VALUES (?, ?)")
                .param("Interstellar")
                .param("Nolan")
                .queryGeneratedKeys(resultSet -> resultSet.getLong(1))
                .execute(dataSource);

        List<Movie> movies = SqlBuilder.sql("SELECT id, title, directed_by from movie")
                .queryForList(BaseTest::mapMovie)
                .execute(dataSource);

        Assertions.assertEquals(3, movies.size());

        final String sql = "SELECT id, title, directed_by from movie where id = ?";

        Assertions.assertTrue(SqlBuilder.sql(sql)
                .param(generetedId)
                .queryForExists()
                .execute(dataSource));

        List<Long> generetedIds = SqlBuilder.sql("INSERT INTO movie(title, directed_by) VALUES (?, ?), (?, ?)")
                .param("Catch Me If you Can")
                .param("Cameroon")
                .param("Jurrasic Park")
                .param("Cameroon")
                .queryGeneratedKeysAsList(resultSet -> resultSet.getLong(1))
                .execute(dataSource);

        for (Long aLong : generetedIds) {
            Assertions.assertTrue(SqlBuilder.sql(sql)
                    .param(aLong)
                    .queryForExists()
                    .execute(dataSource));
        }

        Assertions.assertEquals(5,
                SqlBuilder.sql("SELECT COUNT(id) from movie")
                        .queryForInt()
                        .execute(dataSource));

        Movie movie = SqlBuilder.sql(sql)
                                .param(1)
                            .queryForOne(BaseTest::mapMovie)
                            .execute(dataSource);

        Assertions.assertEquals("Dunkirk", movie.title());

    }

    @Test
    void testBatch() throws SQLException {
        int[] updatedRows = SqlBuilder
                .sql("INSERT INTO movie(title, directed_by) VALUES (?, ?)")
                    .param("Interstellar")
                    .param("Nolan")
                .addBatch()
                    .param("Dunkrik")
                    .param("Nolan")
                .executeBatch(dataSource);

        Assertions.assertEquals(2L,
                IntStream.of(updatedRows).count());

        updatedRows = SqlBuilder
                .sql("INSERT INTO movie(title, directed_by) VALUES (?, ?)")
                    .param("Jurasic Park")
                    .param("Cameroon")
                .addBatch()
                    .param("Terminator 2")
                    .param("Cameroon")
                .addBatch()
                    .param("Titanic")
                    .param("Cameroon")
                .addBatch()
                    .param("Avatar")
                    .param("Cameroon")
                .executeBatch(dataSource);

        Assertions.assertEquals(4,
                IntStream.of(updatedRows).count());

        List<Movie> movies = SqlBuilder.sql("SELECT id, title, directed_by from movie")
                .queryForList(BaseTest::mapMovie)
                .execute(dataSource);

        Assertions.assertEquals("Interstellar",
                movies.get(0).title());
        Assertions.assertEquals("Dunkrik",
                movies.get(1).title());
        Assertions.assertEquals("Jurasic Park",
                movies.get(2).title());
        Assertions.assertEquals("Terminator 2",
                movies.get(3).title());
        Assertions.assertEquals("Titanic",
                movies.get(4).title());
        Assertions.assertEquals("Avatar",
                movies.get(5).title());

    }


}
