package com.techatpark;

import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

class TransactionTest extends BaseTest {

    @BeforeEach
    void init() throws SQLException {
        Transaction
                .begin(SqlBuilder.prepareSql("TRUNCATE TABLE movie"))
                .thenApply(updatedRows -> SqlBuilder.prepareSql("TRUNCATE TABLE director"))
                .execute(dataSource);
    }

    @Test
    void testBasicCommit() throws SQLException {
        Transaction
                .begin(SqlBuilder.prepareSql("INSERT INTO director(name) VALUES (?)")
                        .param("Nolan")
                        .queryGeneratedKeys(resultSet -> resultSet.getLong(1)))
                .thenApply(generetedId -> SqlBuilder.prepareSql("INSERT INTO movie(title, directed_by) VALUES (?, ?), (?, ?)")
                        .param("Dunkirk")
                        .param(generetedId.toString())
                        .param("Inception")
                        .param(generetedId.toString()))
                .execute(dataSource);

        Assertions.assertEquals(2,
                SqlBuilder.prepareSql("SELECT COUNT(id) from movie")
                        .queryForInt()
                        .execute(dataSource));

        Assertions.assertEquals(1,
                SqlBuilder.prepareSql("SELECT COUNT(id) from director")
                        .queryForInt()
                        .execute(dataSource));

    }

    @Test
    void testInvalidCommit() throws SQLException {
        SQLException exception = assertThrows(SQLException.class, () -> {
            Transaction
                    .begin(SqlBuilder.prepareSql("INSERT INTO director(name) VALUES (?)")
                            .param("Nolan")
                            .queryGeneratedKeys(resultSet -> resultSet.getLong(1)))
                    .thenApply(generetedId -> SqlBuilder.prepareSql("INSERT INTO movie(title, directed_by) VALUES (?, ?), (?, ?)")
                            .param("Inception")
                            .param(generetedId.toString())
                            .paramNull()
                            .param(generetedId.toString()))
                    .execute(dataSource);
        });

        Assertions.assertEquals(0,
                SqlBuilder.prepareSql("SELECT COUNT(id) from director")
                        .queryForInt()
                        .execute(dataSource));

        Assertions.assertEquals(0,
                SqlBuilder.prepareSql("SELECT COUNT(id) from movie")
                        .queryForInt()
                        .execute(dataSource));

    }

    @Test
    void testSavepoint() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            // Insert director
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO director(name) VALUES (?)")) {
                ps.setString(1, "Steven Spielberg");
                ps.executeUpdate();
            }

            // Insert first movie
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO movie(title, directed_by) VALUES (?, ?)")) {
                ps.setString(1, "E.T.");
                ps.setString(2, "Steven Spielberg");
                ps.executeUpdate();
            }

            // Create savepoint after successful insert
            Savepoint savepoint1 = conn.setSavepoint("AfterET");

            try {
                // Insert invalid movie (null title → should fail)
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO movie(title, directed_by) VALUES (?, ?)")) {
                    ps.setNull(1, Types.VARCHAR);
                    ps.setString(2, "Steven Spielberg");
                    ps.executeUpdate();
                }
            } catch (SQLException e) {
                // Rollback only to savepoint, keep "E.T."
                conn.rollback(savepoint1);
            }

            conn.commit();
        }

        // Verify only "E.T." exists
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
        Transaction
                // Step 1: Insert director and return generated ID
                .begin(SqlBuilder.prepareSql("INSERT INTO director(name) VALUES (?)")
                        .param("Christopher Nolan")
                        .queryGeneratedKeys(rs -> rs.getLong(1)))

                // Step 2: Use directorId to fetch directorName
                .thenApply(directorId -> SqlBuilder
                        .prepareSql("SELECT name FROM director WHERE id = ?")
                        .param(directorId)
                        .queryForString())

                // Step 3: Use directorName to insert movies
                .thenApply(directorName -> SqlBuilder
                        .prepareSql("INSERT INTO movie(title, directed_by) VALUES (?, ?), (?, ?)")
                        .param("Tenet").param(directorName)
                        .param("Oppenheimer").param(directorName))

                // Execute as one transaction
                .execute(dataSource);

        // ✅ Verification
        Assertions.assertEquals(1,
                SqlBuilder.prepareSql("SELECT COUNT(id) FROM director")
                        .queryForInt()
                        .execute(dataSource));

        Assertions.assertEquals(2,
                SqlBuilder.prepareSql("SELECT COUNT(id) FROM movie")
                        .queryForInt()
                        .execute(dataSource));


    }

    @Test
    //  Savepoints allow you to selectively discard parts of the transaction, while committing the rest.
    //  After defining a savepoint with SAVEPOINT, you can if needed roll back to the savepoint with ROLLBACK
    //  TO. All the transaction's database changes between defining
    //  the savepoint and rolling back to it are discarded, but changes earlier than the savepoint are kept.
    void testNolanMoviesWithSavepointRollback() throws SQLException {

            Transaction
                // Step 1: Insert director and return generated ID
                .begin(SqlBuilder.prepareSql("INSERT INTO director(name) VALUES (?)")
                        .param("Christopher Nolan")
                        .queryGeneratedKeys(rs -> rs.getLong(1)))

                // Step 2: Use directorId to fetch directorName
                .thenApply(directorId -> SqlBuilder
                        .prepareSql("SELECT name FROM director WHERE id = ?")
                        .param(directorId)
                        .queryForString())

                .savePoint("savepoint_nolan_additional_works", directorName -> SqlBuilder
                        .prepareSql("INSERT INTO movie(title, directed_by) VALUES (?, ?), (?, ?)")
                        .param("Tenet")
                        .param(directorName)
                        .paramNull() // NOTNULL Error
                        .param(directorName))

                // Execute as one transaction
                .execute(dataSource);



        // ✅ Assertions
        Assertions.assertEquals(1,
                SqlBuilder.prepareSql("SELECT COUNT(id) FROM director")
                        .queryForInt()
                        .execute(dataSource));

        Assertions.assertEquals(0,
                SqlBuilder.prepareSql("SELECT COUNT(id) FROM movie")
                        .queryForInt()
                        .execute(dataSource));
    }



}
