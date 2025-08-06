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
        SqlBuilder.prepareSql("TRUNCATE TABLE movie")
                .execute(dataSource);
    }

    @Test
    void testSql() throws SQLException {
        int updateRows =
                SqlBuilder.sql("INSERT INTO movie(title, directed_by) VALUES ('Dunkirk', 'Nolan')")
                        .execute(dataSource);

        Assertions.assertEquals(1, updateRows);

        long generetedId = SqlBuilder.sql("INSERT INTO movie(title, directed_by) VALUES ('Interstellar', 'Nolan')")
                .queryGeneratedKeys(resultSet -> resultSet.getLong(1))
                .execute(dataSource);

        Assertions.assertEquals(2, generetedId);

        List<Long> generetedIds = SqlBuilder.sql("INSERT INTO movie(title, directed_by) VALUES ('Jurrasic Park', 'Nolan'), ('Batman', 'Nolan')")
                .queryGeneratedKeysAsList(resultSet -> resultSet.getLong(1))
                .execute(dataSource);

        Assertions.assertEquals(3, generetedIds.get(0));
        Assertions.assertEquals(4, generetedIds.get(1));
    }
    @Test
    void testPrepareSql() throws SQLException {
        int updateRows =
                SqlBuilder.prepareSql("INSERT INTO movie(title, directed_by) VALUES (?, ?), (?, ?)")
                        .param("Dunkirk")
                        .param("Nolan")
                        .param("Inception")
                        .param("Nolan")
                    .execute(dataSource);

        Assertions.assertEquals(2, updateRows);

        long generetedId = SqlBuilder.prepareSql("INSERT INTO movie(title, directed_by) VALUES (?, ?)")
                .param("Interstellar")
                .param("Nolan")
                .queryGeneratedKeys(resultSet -> resultSet.getLong(1))
                .execute(dataSource);

        List<Movie> movies = SqlBuilder.prepareSql("SELECT id, title, directed_by from movie")
                .queryForList(BaseTest::mapMovie)
                .execute(dataSource);

        Assertions.assertEquals(3, movies.size());

        final String sql = "SELECT id, title, directed_by from movie where id = ?";

        Assertions.assertTrue(SqlBuilder.prepareSql(sql)
                .param(generetedId)
                .queryForExists()
                .execute(dataSource));

        List<Long> generetedIds = SqlBuilder.prepareSql("INSERT INTO movie(title, directed_by) VALUES (?, ?), (?, ?)")
                .param("Catch Me If you Can")
                .param("Cameroon")
                .param("Jurrasic Park")
                .param("Cameroon")
                .queryGeneratedKeysAsList(resultSet -> resultSet.getLong(1))
                .execute(dataSource);

        for (Long aLong : generetedIds) {
            Assertions.assertTrue(SqlBuilder.prepareSql(sql)
                    .param(aLong)
                    .queryForExists()
                    .execute(dataSource));
        }

        Assertions.assertEquals(5,
                SqlBuilder.prepareSql("SELECT COUNT(id) from movie")
                        .queryForInt()
                        .execute(dataSource));

        Movie movie = SqlBuilder.prepareSql(sql)
                                .param(1)
                            .queryForOne(BaseTest::mapMovie)
                            .execute(dataSource);

        Assertions.assertEquals("Dunkirk", movie.title());

    }


    @Test
    void testBatch() throws SQLException {
        int[] updatedRows = SqlBuilder
                .sql("INSERT INTO movie(title, directed_by) VALUES ('Interstellar', 'Nolan')")
                .addBatch("INSERT INTO movie(title, directed_by) VALUES ('Dunkrik', 'Nolan'),('Inception', 'Nolan')")
                .addBatch("INSERT INTO movie(title, directed_by) VALUES ('Batman', 'Nolan')")
                .executeBatch(dataSource);

        Assertions.assertEquals(4,
                IntStream.of(updatedRows).sum());
    }


    @Test
    void testPreparedBatch() throws SQLException {
        int[] updatedRows = SqlBuilder
                .prepareSql("INSERT INTO movie(title, directed_by) VALUES (?, ?)")
                    .param("Interstellar")
                    .param("Nolan")
                .addBatch()
                    .param("Dunkrik")
                    .param("Nolan")
                .executeBatch(dataSource);

        Assertions.assertEquals(2L,
                IntStream.of(updatedRows).sum());

        updatedRows = SqlBuilder
                .prepareSql("INSERT INTO movie(title, directed_by) VALUES (?, ?)")
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
                IntStream.of(updatedRows).sum());

        List<Movie> movies = SqlBuilder.prepareSql("SELECT id, title, directed_by from movie")
                .queryForList(BaseTest::mapMovie)
                .execute(dataSource);

        Assertions.assertTrue(
                SqlBuilder.sql("SELECT id, title, directed_by from movie WHERE title = 'Interstellar'")
                        .queryForExists().execute(dataSource));
        Assertions.assertEquals("Nolan",
                SqlBuilder.sql("SELECT directed_by from movie WHERE title = 'Interstellar'")
                        .queryForString().execute(dataSource));
        Assertions.assertTrue(
                SqlBuilder.sql("SELECT title from movie WHERE directed_by = 'Cameroon'")
                        .queryForListOfString()
                        .execute(dataSource)
                        .containsAll(List.of("Avatar", "Titanic", "Terminator 2", "Jurasic Park")));
        Assertions.assertTrue(
                SqlBuilder.sql("SELECT id from movie WHERE directed_by = 'Cameroon'")
                        .queryForListOfInt()
                        .execute(dataSource)
                        .containsAll(List.of(6,5,4,3)));


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
