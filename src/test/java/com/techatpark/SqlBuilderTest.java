package com.techatpark;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.List;

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

        long generetedId = new SqlBuilder("INSERT INTO movie(title, directed_by) VALUES (?, ?)")
                .param("Interstellar")
                .param("Nolan")
                .queryGeneratedKeys(resultSet -> resultSet.getLong(1))
                .execute(dataSource);

        final String sql = "SELECT id, title, directed_by from movie where id = ?";

        Assertions.assertTrue(new SqlBuilder(sql)
                .param(generetedId)
                .queryForExists()
                .execute(dataSource));

        List<Long> generetedIds = new SqlBuilder("INSERT INTO movie(title, directed_by) VALUES (?, ?), (?, ?)")
                .param("Catch Me If you Can")
                .param("Cameroon")
                .param("Jurrasic Park")
                .param("Cameroon")
                .queryGeneratedKeysAsList(resultSet -> resultSet.getLong(1))
                .execute(dataSource);

        for (Long aLong : generetedIds) {
            Assertions.assertTrue(new SqlBuilder(sql)
                    .param(aLong)
                    .queryForExists()
                    .execute(dataSource));
        }

        Assertions.assertEquals(5,
                new SqlBuilder("SELECT COUNT(id) from movie")
                        .queryForInt()
                        .execute(dataSource));

        Movie movie = new SqlBuilder(sql)
                                .param(1)
                            .queryForOne(this::mapRow)
                            .execute(dataSource);

        Assertions.assertEquals("Dunkirk", movie.title());

    }

}
