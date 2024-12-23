package com.techatpark;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

class TransactionTest extends BaseTest {

    @Test
    void testTransaction() throws SQLException {

        Transaction
            .begin(new SqlBuilder("INSERT INTO movie ( title ,directed_by ) VALUES ( ? ,? )")
                    .param("Inception")
                    .param("Christopher Nolan"))
            // Invalid Insert (title can not be null). Should Fail.
            .thenApply(value -> new SqlBuilder("INSERT INTO movie ( title ,directed_by ) VALUES ( ? ,? )")
                    .param("Dunkrik")
                    .param("Christopher Nolan"))
            .commit(dataSource);


        Assertions.assertEquals(2,
                new SqlBuilder("SELECT id, title, directed_by from movie")
                        .queryForList(BaseTest::mapMovie)
                        .execute(dataSource).size());

    }

    @Test
    void testInvalidTransaction() throws SQLException {

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

        Assertions.assertEquals(0,
                new SqlBuilder("SELECT id, title, directed_by from movie")
                .queryForList(BaseTest::mapMovie)
                .execute(dataSource).size());

    }
}