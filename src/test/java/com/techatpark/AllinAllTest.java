package com.techatpark;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
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
    private static final SqlBuilder ALL_RESULS_QUERY = SqlBuilder.prepareSql("SELECT * FROM AllTypes");

    private final SqlBuilder.PreparedSqlBuilder sqlBuilder = SqlBuilder.prepareSql("""
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

    private final SqlBuilder.PreparedSqlBuilder.PreparedBatch preparedBatch = sqlBuilder
            .addBatch()
            .param(STR_VAL, Types.VARCHAR)
            .param(INT_VAL)
            .param(LONG_VAL)
            .param(DOUBLE_VAL)
            .param(FLOAT_VAL)
            .paramNull(Types.BOOLEAN, "BOOLEAN")
            .param(SHORT_VAL)
            .param(BYTE_VAL)
            .param(DATE_VAL)
            .param(TIME_VAL)
            .param(TIMESTAMP_VAL)
            .param(BIG_DECIMAL_VAL)
            .param(BYTES_VAL)
            .param(URL_STR)
            .paramNull();

    private final SqlBuilder.CallableSqlBuilder callSqlBuilder = SqlBuilder.prepareCall("""
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

    private final SqlBuilder.CallableSqlBuilder.CallableBatch callableBatch = callSqlBuilder
            .addBatch()
            .param(STR_VAL, Types.VARCHAR)
            .param(INT_VAL)
            .param(LONG_VAL)
            .param(DOUBLE_VAL)
            .param(FLOAT_VAL)
            .paramNull(Types.BOOLEAN, "BOOLEAN")
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
        SqlBuilder.prepareSql("TRUNCATE TABLE AllTypes")
                .execute(dataSource);
    }

    @Test
    void testBatch() throws Exception {

        preparedBatch.executeBatch(dataSource);

        assertEquals(2, ALL_RESULS_QUERY
                .queryForList(AllinAllTest::mapRow)
                .execute(dataSource)
                .size());

    }
    @Test
    void testInvalidBatchFromSqlBuilder() {
        // If we give more parameter to SQL Builder initiated batch
        SQLException exception = assertThrows(SQLException.class, () -> {
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

        Assertions.assertTrue(exception.getMessage().startsWith("Parameters "));
    }

    @Test
    void testInvalidExecuteBatchFromBatch() throws Exception {
        SqlBuilder.PreparedSqlBuilder.PreparedBatch batchFromPreparedBatch = this.preparedBatch
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
                .addBatch();
        // If we give less parameter to SQL Builder initiated batch
        SQLException exception = assertThrows(SQLException.class, () -> {
            batchFromPreparedBatch
                    .param(STR_VAL)
                    .addBatch()
                    .executeBatch(dataSource);
        });

        Assertions.assertTrue(exception.getMessage().startsWith("Parameters "));

    }

    @Test
    void testInvalidAddBatchFromBatch() {
        // If we give more parameter to SQL Builder initiated batch
        SQLException exception = assertThrows(SQLException.class, () -> {
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
                    .addBatch();
        });

        Assertions.assertTrue(exception.getMessage().startsWith("Parameters "));
    }

    @Test
    void testInvalidExecuteBatchFromCallableBatch() throws Exception {
        SqlBuilder.CallableSqlBuilder.CallableBatch batchFromCallableBatch = this.callableBatch
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
                .addBatch();
        // If we give less parameter to SQL Builder initiated batch
        SQLException exception = assertThrows(SQLException.class, () -> {
            batchFromCallableBatch
                    .param(STR_VAL)
                    .addBatch()
                    .executeBatch(dataSource);
        });

        Assertions.assertTrue(exception.getMessage().startsWith("Parameters "));
    }

    @Test
    void testInvalidAddBatchFromCallableBatch() {
        // If we give more parameter to SQL Builder initiated batch
        SQLException exception = assertThrows(SQLException.class, () -> {
            callSqlBuilder
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
                    .addBatch();
        });

        Assertions.assertTrue(exception.getMessage().startsWith("Parameters "));
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

        SqlBuilder.prepareSql("""
                INSERT INTO AllTypes
                (timeVal)
                VALUES (?)
                """)
                .paramNull(Types.TIME, "TIME")
                .execute(dataSource);

        SqlBuilder.prepareSql("""
                INSERT INTO AllTypes
                (timeVal)
                VALUES (?)
                """)
                .param(TIME_VAL, Types.TIME)
                .execute(dataSource);

        assertEquals(2L, SqlBuilder.prepareSql("SELECT COUNT(*) FROM AllTypes")
                .queryForLong()
                .execute(dataSource));

    }

    @Test
    void testInsertAndRetrieveData() throws Exception {

        sqlBuilder.execute(dataSource);

        verifyData();

        assertEquals(STR_VAL,
                SqlBuilder.prepareSql("select str from AllTypes")
                        .queryForString()
                        .execute(dataSource));

        assertEquals(STR_VAL,
                SqlBuilder.prepareSql("select str from AllTypes")
                        .queryForListOfString()
                        .execute(dataSource).get(0));

        assertEquals(STR_VAL,
                SqlBuilder.prepareSql("select str from AllTypes")
                        .queryForObject()
                        .execute(dataSource));

        assertEquals(STR_VAL,
                SqlBuilder.prepareSql("select str from AllTypes")
                        .queryForListOfObject()
                        .execute(dataSource).get(0));

        assertEquals(INT_VAL,
                        SqlBuilder.prepareSql("select intVal from AllTypes")
                                .queryForInt()
                                .execute(dataSource));

        assertEquals(INT_VAL,
                SqlBuilder.prepareSql("select intVal from AllTypes")
                        .queryForListOfInt()
                        .execute(dataSource).get(0));

         assertEquals(LONG_VAL,
                 SqlBuilder.prepareSql("select longVal from AllTypes")
                         .queryForLong()
                         .execute(dataSource));

        assertEquals(LONG_VAL,
                SqlBuilder.prepareSql("select longVal from AllTypes")
                        .queryForListOfLong()
                        .execute(dataSource).get(0));

         assertEquals(DOUBLE_VAL,
                 SqlBuilder.prepareSql("select doubleVal from AllTypes")
                         .queryForDouble()
                         .execute(dataSource));

        assertEquals(DOUBLE_VAL,
                SqlBuilder.prepareSql("select doubleVal from AllTypes")
                        .queryForListOfDouble()
                        .execute(dataSource).get(0));

         assertEquals(FLOAT_VAL,
                 SqlBuilder.prepareSql("select floatVal from AllTypes")
                         .queryForFloat()
                         .execute(dataSource));

        assertEquals(FLOAT_VAL,
                SqlBuilder.prepareSql("select floatVal from AllTypes")
                        .queryForListOfFloat()
                        .execute(dataSource).get(0));

         assertEquals(BOOL_VAL,
                 SqlBuilder.prepareSql("select boolVal from AllTypes")
                         .queryForBoolean()
                         .execute(dataSource));

        assertEquals(BOOL_VAL,
                SqlBuilder.prepareSql("select boolVal from AllTypes")
                        .queryForListOfBoolean()
                        .execute(dataSource).get(0));

         assertEquals(SHORT_VAL,
                 SqlBuilder.prepareSql("select shortVal from AllTypes")
                         .queryForShort()
                         .execute(dataSource));

        assertEquals(SHORT_VAL,
                SqlBuilder.prepareSql("select shortVal from AllTypes")
                        .queryForListOfShort()
                        .execute(dataSource).get(0));

         assertEquals(BYTE_VAL,
                 SqlBuilder.prepareSql("select byteVal from AllTypes")
                         .queryForByte()
                         .execute(dataSource));

        assertEquals(BYTE_VAL,
                SqlBuilder.prepareSql("select byteVal from AllTypes")
                        .queryForListOfByte()
                        .execute(dataSource).get(0));

        assertArrayEquals(BYTES_VAL,
                SqlBuilder.prepareSql("select bytesVal from AllTypes")
                        .queryForBytes()
                        .execute(dataSource));

        assertArrayEquals(BYTES_VAL,
                SqlBuilder.prepareSql("select bytesVal from AllTypes")
                        .queryForListOfBytes()
                        .execute(dataSource).get(0));

         assertEquals(DATE_VAL,
                 SqlBuilder.prepareSql("select dateVal from AllTypes")
                         .queryForDate()
                         .execute(dataSource));

        assertEquals(DATE_VAL,
                SqlBuilder.prepareSql("select dateVal from AllTypes")
                        .queryForListOfDate()
                        .execute(dataSource).get(0));

         assertEquals(TIME_VAL,
                 SqlBuilder.prepareSql("select timeVal from AllTypes")
                         .queryForTime()
                         .execute(dataSource));

        assertEquals(TIME_VAL,
                SqlBuilder.prepareSql("select timeVal from AllTypes")
                        .queryForListOfTime()
                        .execute(dataSource).get(0));

         assertEquals(TIMESTAMP_VAL,
                 SqlBuilder.prepareSql("select timestampVal from AllTypes")
                         .queryForTimestamp()
                         .execute(dataSource));

        assertEquals(TIMESTAMP_VAL,
                SqlBuilder.prepareSql("select timestampVal from AllTypes")
                        .queryForListOfTimestamp()
                        .execute(dataSource).get(0));

         assertEquals(BIG_DECIMAL_VAL,
                 SqlBuilder.prepareSql("select bigDecimalVal from AllTypes")
                         .queryForBigDecimal()
                         .execute(dataSource));

        assertEquals(BIG_DECIMAL_VAL,
                SqlBuilder.prepareSql("select bigDecimalVal from AllTypes")
                        .queryForListOfBigDecimal()
                        .execute(dataSource).get(0));

        assertThrows(SQLFeatureNotSupportedException.class, () -> {
            assertEquals(new URL(URL_STR),
                    SqlBuilder.prepareSql("select urlVal from AllTypes")
                            .queryForURL()
                            .execute(dataSource));
        });

        assertThrows(SQLFeatureNotSupportedException.class, () -> {
            assertEquals(new URL(URL_STR),
                    SqlBuilder.prepareSql("select urlVal from AllTypes")
                            .queryForListOfURL()
                            .execute(dataSource).get(0));
        });
    }

    private void verifyData() throws SQLException {
        AllTypesRecord record = ALL_RESULS_QUERY
                .queryForOne(AllinAllTest::mapRow)
                .execute(dataSource);

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

    @Test
    void testStoredProcedure_IN() throws Exception {

        SqlBuilder.CallableSqlBuilder callableSqlBuilder = SqlBuilder.prepareCall("CALL insert_alltypes_in(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
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
        callableSqlBuilder.execute(dataSource);
        verifyData();

        callableSqlBuilder.addBatch()
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

        assertEquals(3L, SqlBuilder.prepareSql("SELECT COUNT(*) FROM AllTypes")
                .queryForLong()
                .execute(dataSource));

    }

    @Test
    void testStoredProcedure_INOUT() throws Exception {
        SqlBuilder.prepareCall("CALL insert_alltypes_out(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
                .outParam(Types.VARCHAR, STR_VAL)
                .outParam(Types.INTEGER, INT_VAL)
                .outParam(Types.BIGINT, LONG_VAL)
                .outParam(Types.DOUBLE, DOUBLE_VAL)
                .outParam(Types.REAL, FLOAT_VAL)
                .outParam(Types.BOOLEAN, BOOL_VAL)
                .outParam(Types.SMALLINT, SHORT_VAL)
                .outParam(Types.SMALLINT, BYTE_VAL)
                .outParam(Types.DATE, DATE_VAL)
                .outParam(Types.TIME, TIME_VAL)
                .outParam(Types.TIMESTAMP, TIMESTAMP_VAL)
                .outParam(Types.DECIMAL, BIG_DECIMAL_VAL)
                .outParam(Types.BINARY, BYTES_VAL) // or Types.VARBINARY depending on your JDBC driver
                .outParam(Types.VARCHAR, URL_STR)
                .outParam(Types.VARCHAR, (String) null) // NULL input// OUT: p_id
                .execute(dataSource);

        verifyData();
    }
    @Test
    void testStoredProcedure_IN_OUT() throws Exception {

        SqlBuilder.prepareCall("CALL insert_alltypes_in_and_out(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
                .outParam(Types.VARCHAR, STR_VAL)
                .param(STR_VAL)
                .param(INT_VAL)
                .param(LONG_VAL)
                .param(DOUBLE_VAL)
                .param(FLOAT_VAL)
                .param(BOOL_VAL)
                .param(SHORT_VAL, Types.SMALLINT)
                .param(BYTE_VAL)
                .param(DATE_VAL)
                .param(TIME_VAL)
                .param(TIMESTAMP_VAL)
                .param(BIG_DECIMAL_VAL)
                .param(BYTES_VAL)
                .param(URL_STR)
                .paramNull()
                .outParam(Types.SMALLINT)
                .execute(dataSource);
        verifyData();

        SqlBuilder.prepareCall("CALL insert_alltypes_in_and_out(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
                .outParam(Types.VARCHAR, STR_VAL)
                .paramNull(Types.VARCHAR, "VARCHAR")
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
                .outParam(Types.SMALLINT)
                .execute(dataSource);

        assertEquals(2L, SqlBuilder.prepareSql("SELECT COUNT(*) FROM AllTypes")
                .queryForLong()
                .execute(dataSource));
    }

    // Define Java record for table mapping
    record AllTypesRecord(String str, int intVal, long longVal, double doubleVal, float floatVal, boolean boolVal,
                          short shortVal, byte byteVal, Date dateVal, Time timeVal, Timestamp timestampVal,
                          BigDecimal bigDecimalVal, byte[] bytesVal, URL urlVal, String nullVal) {
    }


}
