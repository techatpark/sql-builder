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
    void testSavepoint() throws SQLException {

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
                        .param("Inception")
                        .param(directorName))

                // Execute as one transaction
                .execute(dataSource);



        // ✅ Assertions
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
    void testSavepointRollback() throws SQLException {

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
