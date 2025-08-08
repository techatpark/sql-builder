package com.techatpark;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StoredProcedureTest extends BaseTest {

    @BeforeEach
    void init() throws SQLException {
        SqlBuilder.prepareSql("TRUNCATE TABLE movie")
                .execute(dataSource);
    }

    @Test
    void testAddMovie_IN() throws Exception {
        SqlBuilder.prepareCall("CALL insert_movie_in(?, ?)")
                .param("Inception", Types.VARCHAR)
                .paramNull(Types.VARCHAR, "VARCHAR")
                .execute(dataSource);
        Assertions.assertNull(
                SqlBuilder.sql("SELECT directed_by from movie WHERE title = 'Inception'")
                        .queryForString().execute(dataSource));
    }

    @Test
    void testAddMovie_OUT() throws Exception {
        long id = SqlBuilder.prepareCall("CALL insert_movie_out(?, ?, ?)")
                .param("Inception")
                .param("Christopher Nolan")
                .outParam(Types.BIGINT)
                .queryOutParams(statement -> statement.getLong(3))
                .execute(dataSource);
            Assertions.assertEquals("Christopher Nolan",
                    SqlBuilder.sql("SELECT directed_by from movie WHERE id = " + id)
                            .queryForString().execute(dataSource));
    }

    @Test
    void testAddMovie_Function() throws Exception {
        long id = SqlBuilder.prepareCall("{? = call insert_movie_fn(?, ?)}")
                .outParam(Types.BIGINT)
                .param("Inception")
                .param("Christopher Nolan")
                .queryOutParams(statement -> statement.getLong(1))
                .execute(dataSource);
        Assertions.assertEquals("Christopher Nolan",
                SqlBuilder.sql("SELECT directed_by from movie WHERE id = " + id)
                        .queryForString().execute(dataSource));
    }

    @Test
    void testUpdateTitle_INOUT() throws Exception {
        long id = SqlBuilder.sql("INSERT INTO movie(title, directed_by) VALUES ('Interstellar', 'Nolan')")
                .queryGeneratedKeys(resultSet -> resultSet.getLong(1))
                .execute(dataSource);

        String newTitle = SqlBuilder.prepareCall("CALL update_title_inout(?, ?)")
                .outParam(Types.BIGINT, id)
                .outParam(Types.VARCHAR, "Updated Title")
                .queryOutParams(statement -> statement.getString(2))
                .execute(dataSource);

        assertEquals("Updated Title", newTitle);

    }

    @Test
    void testAddMovie_Result() throws Exception {
        long generetedId = SqlBuilder.sql("INSERT INTO movie(title, directed_by) VALUES ('Interstellar', 'Nolan')")
                .queryGeneratedKeys(resultSet -> resultSet.getLong(1))
                .execute(dataSource);
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false); // 🔴 Important: Keeps the transaction open
            CallableStatement stmt = conn.prepareCall("{? = call get_movie_by_id(?)}");
            stmt.registerOutParameter(1, Types.OTHER); // For REFCURSOR
            stmt.setLong(2, generetedId); // p_id = 1

            stmt.execute();

            ResultSet rs = (ResultSet) stmt.getObject(1);
            while (rs.next()) {
                int id = rs.getInt("id");
                System.out.printf("Movie: %d, %s, %s%n", id, rs.getString("title"), rs.getString("directed_by"));
            }

            conn.commit();
            conn.setAutoCommit(true);

        }
    }
}
