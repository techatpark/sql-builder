package com.techatpark;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.CallableStatement;
import java.sql.Connection;
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
        try (Connection conn = dataSource.getConnection();
             CallableStatement stmt = conn.prepareCall("CALL insert_movie_in(?, ?)")) {
            stmt.setString(1, "Inception");
            stmt.setString(2, "Christopher Nolan");
            stmt.execute();
        }
        Assertions.assertEquals("Christopher Nolan",
                SqlBuilder.sql("SELECT directed_by from movie WHERE title = 'Inception'")
                        .queryForString().execute(dataSource));
    }

    @Test
    void testAddMovie_OUT() throws Exception {
        try (Connection conn = dataSource.getConnection();
             CallableStatement stmt = conn.prepareCall("CALL insert_movie_out(?, ?, ?)")) {
            stmt.setString(1, "Inception");
            stmt.setString(2, "Christopher Nolan");
            stmt.registerOutParameter(3, Types.BIGINT);
            stmt.execute();

            long id = stmt.getLong(3);
            Assertions.assertEquals("Christopher Nolan",
                    SqlBuilder.sql("SELECT directed_by from movie WHERE id = " + id)
                            .queryForString().execute(dataSource));
        }
    }

    @Test
    void testAddMovie_Function() throws Exception {
        try (Connection conn = dataSource.getConnection();
             CallableStatement stmt = conn.prepareCall("{? = call insert_movie_fn(?, ?)}")) {
            stmt.registerOutParameter(1, Types.BIGINT);
            stmt.setString(2, "Inception");
            stmt.setString(3, "Christopher Nolan");

            stmt.execute();

            long id = stmt.getLong(1);
            Assertions.assertEquals("Christopher Nolan",
                    SqlBuilder.sql("SELECT directed_by from movie WHERE id = " + id)
                            .queryForString().execute(dataSource));
        }
    }

    @Test
    void testUpdateTitle_INOUT() throws Exception {
        long generetedId = SqlBuilder.sql("INSERT INTO movie(title, directed_by) VALUES ('Interstellar', 'Nolan')")
                .queryGeneratedKeys(resultSet -> resultSet.getLong(1))
                .execute(dataSource);
        try (Connection conn = dataSource.getConnection();
             CallableStatement stmt = conn.prepareCall("CALL update_title_inout(?, ?)")) {
            stmt.setLong(1, generetedId);
            stmt.registerOutParameter(1, Types.BIGINT);
            stmt.setString(2, "Updated Title");
            stmt.registerOutParameter(2, Types.VARCHAR);
            stmt.execute();

            String newTitle = stmt.getString(2);
            assertEquals("Updated Title", newTitle);
        }
    }
}
