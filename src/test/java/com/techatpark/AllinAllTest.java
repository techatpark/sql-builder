package com.techatpark;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
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
        new SqlBuilder("TRUNCATE TABLE AllTypes")
                .execute(dataSource);
    }

    @Test
    void testEmptyResult() throws Exception {

        final SqlBuilder emptyResulsQuery = new SqlBuilder("SELECT * FROM AllTypes");

        assertEquals(0, emptyResulsQuery
                .queryForList(AllinAllTest::mapRow)
                .execute(dataSource)
                .size());

        assertNull(emptyResulsQuery
                .queryForOne(AllinAllTest::mapRow)
                .execute(dataSource));
    }


    @Test
    void testNullData() throws Exception {

        new SqlBuilder("""
                INSERT INTO AllTypes
                (timeVal)
                VALUES (?)
                """)
                .paramNull(Types.TIME, "TIME")
                .execute(dataSource);

        new SqlBuilder("""
                INSERT INTO AllTypes
                (timeVal)
                VALUES (?)
                """)
                .param(TIME_VAL, Types.TIME)
                .execute(dataSource);

        assertEquals(2L, new SqlBuilder("SELECT COUNT(*) FROM AllTypes")
                .queryForLong()
                .execute(dataSource));

    }

    @Test
    void testInsertAndRetrieveData() throws Exception {
        new SqlBuilder("""
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
                .paramNull()
                .execute(dataSource);

        AllTypesRecord record = new SqlBuilder("SELECT * FROM AllTypes")
                .queryForOne(AllinAllTest::mapRow)
                .execute(dataSource);
        // Assertions
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
    }

    // Define Java record for table mapping
    record AllTypesRecord(String str, int intVal, long longVal, double doubleVal, float floatVal, boolean boolVal,
                          short shortVal, byte byteVal, Date dateVal, Time timeVal, Timestamp timestampVal,
                          BigDecimal bigDecimalVal, byte[] bytesVal, URL urlVal, String nullVal) {
    }


}
