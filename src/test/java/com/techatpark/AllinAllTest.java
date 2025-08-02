package com.techatpark;

import org.h2.jdbc.JdbcSQLFeatureNotSupportedException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import static org.junit.jupiter.api.Assertions.*;

class AllinAllTest extends BaseTest {

    // Constants for Test Data
    static final String STR_VAL = "Hello World";
    static final int INT_VAL = 42;
    static final long LONG_VAL = 123456789L;
    static final double DOUBLE_VAL = 99.99;
    static final float FLOAT_VAL = 45.5f;
    static final boolean BOOL_VAL = true;
    static final short SHORT_VAL = 100;
    static final byte BYTE_VAL = 1;
    static final Date DATE_VAL = Date.valueOf(LocalDate.of(2024, 12, 12));
    static final Time TIME_VAL = Time.valueOf(LocalTime.of(14, 30, 0));
    static final Timestamp TIMESTAMP_VAL = Timestamp.valueOf(LocalDateTime.of(2024, 12, 12, 14, 30, 0));
    static final BigDecimal BIG_DECIMAL_VAL = new BigDecimal("12345.67");
    static final byte[] BYTES_VAL = new byte[]{1, 2, 3, 4, 5};
    static final String URL_STR = "http://example.com";
    private static final SqlBuilder ALL_RESULS_QUERY = SqlBuilder.sql("SELECT * FROM AllTypes");

    private final SqlBuilder sqlBuilder = SqlBuilder.sql("""
                INSERT INTO AllTypes
                (str, intVal, longVal, doubleVal, floatVal, boolVal, shortVal, byteVal, 
                dateVal, timeVal, timestampVal, bigDecimalVal, bytesVal, urlVal, nullVal)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """)
            .param(STR_VAL)
            .param(INT_VAL)
            .param(LONG_VAL)
            .param(DOUBLE_VAL)
            .param(FLOAT_VAL)
            .param(BOOL_VAL)
            .param(SHORT_VAL)
            .param(BYTE_VAL)
            .param(DATE_VAL)
            .param(TIME_VAL)
            .param(TIMESTAMP_VAL)
            .param(BIG_DECIMAL_VAL)
            .param(BYTES_VAL)
            .param(URL_STR)
            .paramNull();

    private static AllTypesRecord mapRow(ResultSet rs) {
        try {
            return new AllTypesRecord(
                    rs.getString("str"),
                    rs.getInt("intVal"),
                    rs.getLong("longVal"),
                    rs.getDouble("doubleVal"),
                    rs.getFloat("floatVal"),
                    rs.getBoolean("boolVal"),
                    rs.getShort("shortVal"),
                    rs.getByte("byteVal"),
                    rs.getDate("dateVal"),
                    rs.getTime("timeVal"),
                    rs.getTimestamp("timestampVal"),
                    rs.getBigDecimal("bigDecimalVal"),
                    rs.getBytes("bytesVal"),
                    new URL(rs.getString("urlVal")),
                    rs.getString("nullVal")
            );
        } catch (MalformedURLException | SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeEach
    void init() throws SQLException {
        SqlBuilder.sql("DELETE FROM AllTypes")
                .execute(dataSource);
    }

    @Test
    void testBatch() throws Exception {

        sqlBuilder
                .addBatch()
                    .param(STR_VAL)
                    .param(INT_VAL)
                    .param(LONG_VAL)
                    .param(DOUBLE_VAL)
                    .param(FLOAT_VAL)
                    .param(BOOL_VAL)
                    .param(SHORT_VAL)
                    .param(BYTE_VAL)
                    .param(DATE_VAL)
                    .param(TIME_VAL)
                    .param(TIMESTAMP_VAL)
                    .param(BIG_DECIMAL_VAL)
                    .param(BYTES_VAL)
                    .param(URL_STR)
                    .paramNull()
                .executeBatch(dataSource);

        assertEquals(2, ALL_RESULS_QUERY
                .queryForList(AllinAllTest::mapRow)
                .execute(dataSource)
                .size());

    }
    @Test
    void testInvalidBatch() throws Exception {
        assertThrows(SQLException.class, () -> {
            // If we give less parameter
            sqlBuilder
                    .addBatch()
                    .param(STR_VAL)
                    .executeBatch(dataSource);
            // if we give more parameters
            sqlBuilder
                    .addBatch()
                    .param(STR_VAL)
                    .param(INT_VAL)
                    .param(LONG_VAL)
                    .param(DOUBLE_VAL)
                    .param(FLOAT_VAL)
                    .param(BOOL_VAL)
                    .param(SHORT_VAL)
                    .param(BYTE_VAL)
                    .param(DATE_VAL)
                    .param(TIME_VAL)
                    .param(TIMESTAMP_VAL)
                    .param(BIG_DECIMAL_VAL)
                    .param(BYTES_VAL)
                    .param(URL_STR)
                    .paramNull()
                    .param(STR_VAL) // 1 More
                    .executeBatch(dataSource);
        });
    }

    @Test
    void testEmptyResult() throws Exception {

        assertEquals(0, ALL_RESULS_QUERY
                .queryForList(AllinAllTest::mapRow)
                .execute(dataSource)
                .size());

        assertNull(ALL_RESULS_QUERY
                .queryForOne(AllinAllTest::mapRow)
                .execute(dataSource));
    }


    @Test
    void testNullData() throws Exception {

        SqlBuilder.sql("""
                INSERT INTO AllTypes
                (timeVal)
                VALUES (?)
                """)
                .paramNull(Types.TIME, "TIME")
                .execute(dataSource);

        SqlBuilder.sql("""
                INSERT INTO AllTypes
                (timeVal)
                VALUES (?)
                """)
                .param(TIME_VAL, Types.TIME)
                .execute(dataSource);

        assertEquals(2L, SqlBuilder.sql("SELECT COUNT(*) FROM AllTypes")
                .queryForLong()
                .execute(dataSource));

    }

    @Test
    void testInsertAndRetrieveData() throws Exception {

        final Connection connection = dataSource.getConnection();




        sqlBuilder.execute(connection);

        AllTypesRecord record = ALL_RESULS_QUERY
                .queryForOne(AllinAllTest::mapRow)
                .execute(connection);

        assertAll("Verify Data",
                () -> assertEquals(STR_VAL, record.str()),
                () -> assertEquals(INT_VAL, record.intVal()),
                () -> assertEquals(LONG_VAL, record.longVal()),
                () -> assertEquals(DOUBLE_VAL, record.doubleVal()),
                () -> assertEquals(FLOAT_VAL, record.floatVal()),
                () -> assertEquals(BOOL_VAL, record.boolVal()),
                () -> assertEquals(SHORT_VAL, record.shortVal()),
                () -> assertEquals(BYTE_VAL, record.byteVal()),
                () -> assertEquals(DATE_VAL, record.dateVal()),
                () -> assertEquals(TIME_VAL, record.timeVal()),
                () -> assertEquals(TIMESTAMP_VAL, record.timestampVal()),
                () -> assertEquals(BIG_DECIMAL_VAL, record.bigDecimalVal()),
                () -> assertArrayEquals(BYTES_VAL, record.bytesVal()),
                () -> assertEquals(new URL(URL_STR), record.urlVal()),
                () -> assertNull(record.nullVal())
        );

        assertEquals(STR_VAL,
                SqlBuilder.sql("select str from AllTypes")
                        .queryForString()
                        .execute(connection));

        assertEquals(STR_VAL,
                SqlBuilder.sql("select str from AllTypes")
                        .queryForObject()
                        .execute(connection));

        assertEquals(INT_VAL,
                        SqlBuilder.sql("select intVal from AllTypes")
                                .queryForInt()
                                .execute(connection));

         assertEquals(LONG_VAL,
                 SqlBuilder.sql("select longVal from AllTypes")
                         .queryForLong()
                         .execute(connection));

         assertEquals(DOUBLE_VAL,
                 SqlBuilder.sql("select doubleVal from AllTypes")
                         .queryForDouble()
                         .execute(connection));
         assertEquals(FLOAT_VAL,
                 SqlBuilder.sql("select floatVal from AllTypes")
                         .queryForFloat()
                         .execute(connection));
         assertEquals(BOOL_VAL,
                 SqlBuilder.sql("select boolVal from AllTypes")
                         .queryForBoolean()
                         .execute(connection));
         assertEquals(SHORT_VAL,
                 SqlBuilder.sql("select shortVal from AllTypes")
                         .queryForShort()
                         .execute(connection));
         assertEquals(BYTE_VAL,
                 SqlBuilder.sql("select byteVal from AllTypes")
                         .queryForByte()
                         .execute(connection));
        assertArrayEquals(BYTES_VAL,
                SqlBuilder.sql("select bytesVal from AllTypes")
                        .queryForBytes()
                        .execute(connection));
         assertEquals(DATE_VAL,
                 SqlBuilder.sql("select dateVal from AllTypes")
                         .queryForDate()
                         .execute(connection));
         assertEquals(TIME_VAL,
                 SqlBuilder.sql("select timeVal from AllTypes")
                         .queryForTime()
                         .execute(connection));
         assertEquals(TIMESTAMP_VAL,
                 SqlBuilder.sql("select timestampVal from AllTypes")
                         .queryForTimestamp()
                         .execute(connection));
         assertEquals(BIG_DECIMAL_VAL,
                 SqlBuilder.sql("select bigDecimalVal from AllTypes")
                         .queryForBigDecimal()
                         .execute(connection));

        assertThrows(JdbcSQLFeatureNotSupportedException.class, () -> {
            assertEquals(new URL(URL_STR),
                    SqlBuilder.sql("select urlVal from AllTypes")
                            .queryForURL()
                            .execute(connection));
        });
    }

    // Define Java record for table mapping
    record AllTypesRecord(String str, int intVal, long longVal, double doubleVal, float floatVal, boolean boolVal,
                          short shortVal, byte byteVal, Date dateVal, Time timeVal, Timestamp timestampVal,
                          BigDecimal bigDecimalVal, byte[] bytesVal, URL urlVal, String nullVal) {
    }


}
