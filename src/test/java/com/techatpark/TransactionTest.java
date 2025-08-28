package com.techatpark;

import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

class TransactionTest extends BaseTest {

    @BeforeEach
    void clearData() throws SQLException {
        SqlBuilder.prepareSql("TRUNCATE TABLE movie")
                .execute(dataSource);
        SqlBuilder.prepareSql("TRUNCATE TABLE director")
                .execute(dataSource);

    }

    @Test
    void testBasicCommit() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO director(name) VALUES (?)")) {
                ps.setString(1, "Christopher Nolan");
                ps.executeUpdate();
            }

            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO movie(title, directed_by) VALUES (?, ?)")) {
                ps.setString(1, "Inception");
                ps.setString(2, "Christopher Nolan");
                ps.executeUpdate();

                ps.setString(1, "Interstellar");
                ps.setString(2, "Christopher Nolan");
                ps.executeUpdate();
            }

            conn.commit(); // ✅ Commit transaction
        }

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM movie");
            rs.next();
            assertEquals(2, rs.getInt(1));

            rs = stmt.executeQuery("SELECT COUNT(*) FROM director");
            rs.next();
            assertEquals(1, rs.getInt(1));
        }
    }

    @Test
    void testRollback() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO director(name) VALUES (?)")) {
                ps.setString(1, "Steven Spielberg");
                ps.executeUpdate();
            }

            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO movie(title, directed_by) VALUES (?, ?)")) {
                ps.setString(1, "Jurassic Park");
                ps.setString(2, "Steven Spielberg");
                ps.executeUpdate();
            }

            conn.rollback(); // ❌ Rollback entire transaction
        }

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM director");
            rs.next();
            assertEquals(0, rs.getInt(1));

            rs = stmt.executeQuery("SELECT COUNT(*) FROM movie");
            rs.next();
            assertEquals(0, rs.getInt(1));
        }
    }

    @Test
    void testSavepoint() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO director(name) VALUES (?)")) {
                ps.setString(1, "Steven Spielberg");
                ps.executeUpdate();
            }

            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO movie(title, directed_by) VALUES (?, ?)")) {
                ps.setString(1, "E.T.");
                ps.setString(2, "Steven Spielberg");
                ps.executeUpdate();

                Savepoint savepoint1 = conn.setSavepoint("AfterET");

                ps.setString(1, "Ready Player One");
                ps.setString(2, "Steven Spielberg");
                ps.executeUpdate();

                // Rollback to savepoint, remove "Ready Player One"
                conn.rollback(savepoint1);

                conn.commit();
            }
        }

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT title FROM movie");
            assertTrue(rs.next());
            assertEquals("E.T.", rs.getString(1));
            assertFalse(rs.next()); // Only one movie
        }
    }

    @Test
    void testChainedQueries() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            long directorId;
            // Step 1: Insert director and get generated id
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO director(name) VALUES (?) RETURNING id")) {
                ps.setString(1, "Christopher Nolan");
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    directorId = rs.getLong(1);
                }
            }

            // Step 2: Use directorId in a SELECT to fetch the name
            String directorName = null;
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT name FROM director WHERE id = ?")) {
                ps.setLong(1, directorId);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    directorName = rs.getString(1);
                }
            }

            // Step 3: Use directorName in inserting movies
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO movie(title, directed_by) VALUES (?, ?)")) {
                ps.setString(1, "Tenet");
                ps.setString(2, directorName);
                ps.executeUpdate();

                ps.setString(1, "Oppenheimer");
                ps.setString(2, directorName);
                ps.executeUpdate();
            }

            conn.commit(); // ✅ Commit all chained operations
        }

        // Verify both tables
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {

            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM director");
            rs.next();
            assertEquals(1, rs.getInt(1));

            rs = stmt.executeQuery("SELECT COUNT(*) FROM movie");
            rs.next();
            assertEquals(2, rs.getInt(1));
        }
    }

}
