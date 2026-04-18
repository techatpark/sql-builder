# SQL Builder

> **SQL Builder** is a framework-independent, lightweight alternative to **[Spring JDBC Client](https://www.baeldung.com/spring-6-jdbcclient-api)** and **[MyBatis](https://mybatis.org/mybatis-3/)**. It is designed to provide the same essential functionality as these tools, but with a strong focus on simplicity and readability. With SQL Builder, developers can streamline database operations without being tied down by unnecessary complexity.

## Why Use SQL Builder?

- **Simpler Fluent & Smart API:** Focused on readability, typesafe and minimal configuration.
- **Framework Independent:** It can be used in any Java framework like Spring Boot , Quarkus  etc ([examples](examples/README.md))
- **Lightweight:** with no third party dependencies

---

## Table of Contents

- [Installation](#installation)
- [How It Works](#how-it-works)
- [Queries](#queries)
    - [INSERT / UPDATE / DELETE](#insert--update--delete)
    - [SELECT](#select)
    - [Type-Safe Query Results](#type-safe-query-results)
    - [Generated Keys](#generated-keys)
- [Batch Operations](#batch-operations)
- [Stored Procedures](#stored-procedures)
- [Transactions](#transactions)
- [Custom RowMapper](#custom-rowmapper)
- [Null Parameters](#null-parameters)
- [Supported Parameter Types](#supported-parameter-types)
- [Framework Integration](#framework-integration)

---

## Installation

Add dependency to your project (With **JDK 17+**)

### Maven
```xml
<dependency>
    <groupId>org.tamilnadujug</groupId>
    <artifactId>sql-builder</artifactId>
    <version>1.0</version>
</dependency>
```
### Gradle
```groovy
implementation 'org.tamilnadujug:sql-builder:1.0-SNAPSHOT'
```

That's all! You can now build and execute [Queries](#queries), [Batch operations](#batch-operations), [Stored Procedures](#stored-procedures), and [Transactions](#transactions).

---

## How It Works

SQL Builder provides three primary entry points:

| Method | Use Case |
|---|---|
| `SqlBuilder.sql(String)` | Plain SQL — no parameters (static SQL) |
| `SqlBuilder.prepareSql(String)` | Parameterized SQL using `?` placeholders |
| `SqlBuilder.prepareCall(String)` | Stored procedure / callable statement |

Every builder follows the same fluent pattern:

```
SqlBuilder.[sql|prepareSql|prepareCall](...)
    .[optional: params / query config]
    .execute(dataSource)   // ← always the last step
```

The `execute()` method accepts either a `javax.sql.DataSource` or a raw `java.sql.Connection`. This makes SQL Builder completely framework-independent — just provide a standard JDBC DataSource.

### Execution Flow

```
DataSource / Connection
       │
       ▼
SqlBuilder.sql(...)          ← plain SQL
SqlBuilder.prepareSql(...)   ← parameterized SQL
SqlBuilder.prepareCall(...)  ← stored procedures
       │
       │  .param(...)        ← bind parameters (type-safe)
       │  .queryFor*(...)    ← configure result mapping
       │
       ▼
   .execute(dataSource)      ← runs the SQL, returns result
```

For **transactions**, the flow chains multiple SQL steps together:

```
Transaction
    .begin(firstSql)
    .thenApply(result -> nextSql(result))
    .thenApply(result -> anotherSql(result))
    .savePoint("id", result -> Transaction.begin(...))
    .execute(dataSource)      ← commits atomically
```

---

## Queries

### INSERT / UPDATE / DELETE

Run a plain SQL statement (no parameters):

```java
int updatedRows = SqlBuilder
    .sql("INSERT INTO movie(title, directed_by) VALUES ('Dunkirk', 'Nolan')")
    .execute(dataSource);
```

Run a parameterized SQL statement — chain `.param(...)` calls in order of `?` placeholders:

```java
int updatedRows = SqlBuilder
    .prepareSql("INSERT INTO movie(title, directed_by) VALUES (?, ?)")
        .param("Dunkirk")
        .param("Nolan")
    .execute(dataSource);
```

Multiple rows in one statement:

```java
int updatedRows = SqlBuilder
    .prepareSql("INSERT INTO movie(title, directed_by) VALUES (?, ?), (?, ?)")
        .param("Dunkirk").param("Nolan")
        .param("Inception").param("Nolan")
    .execute(dataSource);
```

UPDATE with parameters:

```java
int updatedRows = SqlBuilder
    .prepareSql("UPDATE movie SET directed_by = ? WHERE title = ?")
        .param("Christopher Nolan")
        .param("Dunkirk")
    .execute(dataSource);
```

DELETE with parameters:

```java
int deletedRows = SqlBuilder
    .prepareSql("DELETE FROM movie WHERE id = ?")
        .param(42)
    .execute(dataSource);
```

TRUNCATE table:

```java
SqlBuilder
    .prepareSql("TRUNCATE TABLE movie RESTART IDENTITY CASCADE")
    .execute(dataSource);
```

---

### SELECT

**Fetch a single record** with a custom row mapper:

```java
Movie movie = SqlBuilder
    .prepareSql("SELECT id, title, directed_by FROM movie WHERE id = ?")
        .param(1)
    .queryForOne(rs -> new Movie(rs.getShort(1), rs.getString(2), rs.getString(3)))
    .execute(dataSource);
```

**Fetch a list of records:**

```java
List<Movie> movies = SqlBuilder
    .prepareSql("SELECT id, title, directed_by FROM movie")
    .queryForList(rs -> new Movie(rs.getShort(1), rs.getString(2), rs.getString(3)))
    .execute(dataSource);
```

**Check if a record exists:**

```java
boolean exists = SqlBuilder
    .prepareSql("SELECT 1 FROM movie WHERE id = ?")
        .param(generatedId)
    .queryForExists()
    .execute(dataSource);
```

**Count query:**

```java
int count = SqlBuilder
    .prepareSql("SELECT COUNT(id) FROM movie")
    .queryForInt()
    .execute(dataSource);
```

**Fetch a single String column:**

```java
String directedBy = SqlBuilder
    .prepareSql("SELECT directed_by FROM movie WHERE title = ?")
        .param("Interstellar")
    .queryForString()
    .execute(dataSource);
```

**Fetch a list of Strings:**

```java
List<String> titles = SqlBuilder
    .sql("SELECT title FROM movie WHERE directed_by = 'Nolan'")
    .queryForListOfString()
    .execute(dataSource);
```

**Fetch a list of IDs (Long):**

```java
List<Long> ids = SqlBuilder
    .sql("SELECT id FROM movie")
    .queryForListOfLong()
    .execute(dataSource);
```

---

### Type-Safe Query Results

SQL Builder provides built-in `queryFor*()` shortcuts for every common Java type — no manual casting required:

| Method | Return Type |
|---|---|
| `queryForString()` | `String` |
| `queryForInt()` | `Integer` |
| `queryForLong()` | `Long` |
| `queryForShort()` | `Short` |
| `queryForByte()` | `Byte` |
| `queryForBytes()` | `byte[]` |
| `queryForDouble()` | `Double` |
| `queryForFloat()` | `Float` |
| `queryForBigDecimal()` | `BigDecimal` |
| `queryForBoolean()` | `Boolean` |
| `queryForDate()` | `java.sql.Date` |
| `queryForTime()` | `java.sql.Time` |
| `queryForTimestamp()` | `java.sql.Timestamp` |
| `queryForObject()` | `Object` |
| `queryForExists()` | `Boolean` |

Each one has a matching `queryForListOf*()` variant:

```java
List<Integer> ids     = SqlBuilder.sql("SELECT id FROM movie").queryForListOfInt().execute(dataSource);
List<String> titles   = SqlBuilder.sql("SELECT title FROM movie").queryForListOfString().execute(dataSource);
List<Double> ratings  = SqlBuilder.sql("SELECT rating FROM movie").queryForListOfDouble().execute(dataSource);
List<Boolean> flags   = SqlBuilder.sql("SELECT active FROM account").queryForListOfBoolean().execute(dataSource);
List<Date> dates      = SqlBuilder.sql("SELECT release_date FROM movie").queryForListOfDate().execute(dataSource);
List<Timestamp> times = SqlBuilder.sql("SELECT created_at FROM movie").queryForListOfTimestamp().execute(dataSource);
```

---

### Generated Keys

**Fetch the single generated key as `Long`:**

```java
long generatedId = SqlBuilder
    .prepareSql("INSERT INTO movie(title, directed_by) VALUES (?, ?)")
        .param("Interstellar")
        .param("Nolan")
    .queryGeneratedKeyForLong()
    .execute(dataSource);
```

**Fetch the generated key using a custom mapper:**

```java
long generatedId = SqlBuilder
    .prepareSql("INSERT INTO movie(title, directed_by) VALUES (?, ?)")
        .param("Interstellar")
        .param("Nolan")
    .queryGeneratedKeys(rs -> rs.getLong(1))
    .execute(dataSource);
```

**Fetch multiple generated keys (multi-row insert):**

```java
List<Long> generatedIds = SqlBuilder
    .prepareSql("INSERT INTO movie(title, directed_by) VALUES (?, ?), (?, ?)")
        .param("Catch Me If You Can").param("Spielberg")
        .param("Jurassic Park").param("Spielberg")
    .queryGeneratedKeysAsList(rs -> rs.getLong(1))
    .execute(dataSource);
```

**With `RETURNING` clause (PostgreSQL-style):**

```java
Movie movie = SqlBuilder
    .sql("INSERT INTO movie(title, directed_by) VALUES ('Interstellar', 'Nolan') RETURNING id, title, directed_by")
    .queryForOne(rs -> new Movie(rs.getShort(1), rs.getString(2), rs.getString(3)))
    .execute(dataSource);
```

**All typed `queryGeneratedKeyFor*()` variants:**

```java
Integer id        = SqlBuilder.sql("INSERT ...").queryGeneratedKeyForInt().execute(dataSource);
Long id           = SqlBuilder.sql("INSERT ...").queryGeneratedKeyForLong().execute(dataSource);
String val        = SqlBuilder.sql("INSERT ...").queryGeneratedKeyForString().execute(dataSource);
BigDecimal val    = SqlBuilder.sql("INSERT ...").queryGeneratedKeyForBigDecimal().execute(dataSource);
Boolean val       = SqlBuilder.sql("INSERT ...").queryGeneratedKeyForBoolean().execute(dataSource);
Date val          = SqlBuilder.sql("INSERT ...").queryGeneratedKeyForDate().execute(dataSource);
Timestamp val     = SqlBuilder.sql("INSERT ...").queryGeneratedKeyForTimestamp().execute(dataSource);
Object val        = SqlBuilder.sql("INSERT ...").queryGeneratedKeyForObject().execute(dataSource);
```

---

## Batch Operations

### Plain SQL Batch

Execute multiple SQL statements in a single batch:

```java
int[] updatedRows = SqlBuilder
    .sql("INSERT INTO movie(title, directed_by) VALUES ('Interstellar', 'Nolan')")
        .addBatch("INSERT INTO movie(title, directed_by) VALUES ('Dunkirk', 'Nolan'), ('Inception', 'Nolan')")
        .addBatch("INSERT INTO movie(title, directed_by) VALUES ('Batman', 'Nolan')")
    .executeBatch(dataSource);
```

### Prepared SQL Batch

Chain multiple rows for the same parameterized statement:

```java
int[] updatedRows = SqlBuilder
    .prepareSql("INSERT INTO movie(title, directed_by) VALUES (?, ?)")
        .param("Interstellar").param("Nolan")
    .addBatch()
        .param("Dunkirk").param("Nolan")
    .executeBatch(dataSource);
```

**Batch with more entries:**

```java
int[] updatedRows = SqlBuilder
    .prepareSql("INSERT INTO movie(title, directed_by) VALUES (?, ?)")
        .param("Jurassic Park").param("Spielberg")
    .addBatch()
        .param("Terminator 2").param("Cameron")
    .addBatch()
        .param("Titanic").param("Cameron")
    .addBatch()
        .param("Avatar").param("Cameron")
    .executeBatch(dataSource);
```

> **Note:** Each `.addBatch()` must supply the **same number of parameters** as the first set. A `SQLException` is thrown if the parameter count doesn't match.

---

## Stored Procedures

### `IN` Parameters Only

```java
SqlBuilder
    .prepareCall("CALL insert_movie_in(?, ?)")
        .param("Inception", Types.VARCHAR)
        .paramNull(Types.VARCHAR, "VARCHAR")
    .execute(dataSource);
```

### `OUT` Parameters (Function return value)

Use `{? = call fn(...)}` JDBC escape syntax. Register the return position with `outParam(sqlType)`:

```java
long id = SqlBuilder
    .prepareCall("{? = call insert_movie_fn(?, ?)}")
        .outParam(Types.BIGINT)
        .param("Inception")
        .param("Christopher Nolan")
    .queryOutParams(statement -> statement.getLong(1))
    .execute(dataSource);
```

### `INOUT` Parameters

Pass a value and also register it as out:

```java
String newTitle = SqlBuilder
    .prepareCall("CALL update_title_inout(?, ?)")
        .outParam(Types.BIGINT, id)
        .outParam(Types.VARCHAR, "Updated Title")
    .queryOutParams(statement -> statement.getString(2))
    .execute(dataSource);
```

### Mixed `IN` and `OUT` Parameters

```java
long generatedId = SqlBuilder
    .prepareCall("{? = call create_account(?, ?, ?)}")
        .outParam(Types.BIGINT)           // OUT: return value
        .param("john_doe")                // IN: username
        .param("john@example.com")        // IN: email
        .param(true)                      // IN: active flag
    .queryOutParams(stmt -> stmt.getLong(1))
    .execute(dataSource);
```

### Batch Stored Procedures

Only works with `IN` parameters:

```java
SqlBuilder
    .prepareCall("CALL insert_movie_in(?, ?)")
        .param("Inception", Types.VARCHAR)
        .paramNull(Types.VARCHAR, "VARCHAR")
    .addBatch()
        .param("Dunkirk")
        .param("Nolan")
    .addBatch()
        .param("Avatar")
        .param("Cameron")
    .executeBatch(dataSource);
```

> **Note:** Batch for Stored procedures will only work with `IN` parameters — `OUT`/`INOUT` parameters are not batch-friendly.

---

## Transactions

SQL Builder's `Transaction` API allows you to chain multiple SQL operations atomically. Each step's result is automatically passed to the next step via `thenApply()`.

### Transaction Lifecycle

```
Transaction.begin(sql)
       │  commits on success
       │  rolls back automatically on SQLException
       ▼
   .execute(dataSource)
```

Internally:
1. Auto-commit is **disabled** before execution.
2. All steps execute in sequence on the **same connection**.
3. On success: **commit** is called.
4. On failure: the exception propagates to the caller (no full commit happens).
5. Auto-commit is **restored** afterward.

### Basic Transaction (Chained Steps)

```java
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
    // Execute as one atomic transaction
    .execute(dataSource);
```

### Transaction Rollback on Failure

If any step throws a `SQLException`, the whole transaction is not committed:

```java
Transaction
    .begin(SqlBuilder.prepareSql("INSERT INTO director(name) VALUES (?)")
            .param("Nolan")
            .queryGeneratedKeys(rs -> rs.getLong(1)))
    .thenApply(generatedId -> SqlBuilder
            .prepareSql("INSERT INTO movie(title, directed_by) VALUES (?, ?), (?, ?)")
            .param("Inception")
            .param(generatedId.toString())
            .paramNull()  // ← causes NOT NULL violation → whole tx rolls back
            .param(generatedId.toString()))
    .execute(dataSource);
// Both director AND movie inserts are rolled back
```

### Chaining a Select Inside a Transaction

You can mix reads and writes in the same transaction:

```java
Transaction
    .begin(SqlBuilder.prepareSql("INSERT INTO account(username) VALUES (?)")
            .param("john_doe")
            .queryGeneratedKeys(rs -> rs.getLong(1)))
    .thenApply(accountId -> SqlBuilder
            .prepareSql("SELECT username FROM account WHERE id = ?")
            .param(accountId)
            .queryForString())
    .thenApply(username -> SqlBuilder
            .prepareSql("INSERT INTO audit_log(event, username) VALUES (?, ?)")
            .param("ACCOUNT_CREATED")
            .param(username))
    .execute(dataSource);
```

### Transactions with Savepoints

Savepoints let you roll back a **portion** of a transaction without losing earlier committed work. Use `.savePoint(id, fn)` to wrap a sub-transaction:

```java
Transaction
    // Step 1: Insert director
    .begin(SqlBuilder.prepareSql("INSERT INTO director(name) VALUES (?)")
            .param("Christopher Nolan")
            .queryGeneratedKeys(rs -> rs.getLong(1)))

    // Step 2: Fetch director name
    .thenApply(directorId -> SqlBuilder
            .prepareSql("SELECT name FROM director WHERE id = ?")
            .param(directorId)
            .queryForString())

    // Step 3: Savepoint — if this fails, only this block is rolled back
    .savePoint("savepoint_nolan_films", directorName -> Transaction
            .begin(SqlBuilder
               .prepareSql("INSERT INTO movie(title, directed_by) VALUES (?, ?), (?, ?)")
                    .param("Tenet")
                    .param(directorName)
                    .param("Inception")
                    .param(directorName)))

    // Execute — director insert is committed even if savepoint block fails
    .execute(dataSource);
```

**Savepoint rollback example** — inner failure only rolls back to the savepoint:

```java
Transaction
    .begin(SqlBuilder.prepareSql("INSERT INTO director(name) VALUES (?)")
            .param("Christopher Nolan")
            .queryGeneratedKeys(rs -> rs.getLong(1)))

    .thenApply(directorId -> SqlBuilder
            .prepareSql("SELECT name FROM director WHERE id = ?")
            .param(directorId)
            .queryForString())

    .savePoint("savepoint_optional_movies", directorName -> Transaction
            .begin(SqlBuilder
               .prepareSql("INSERT INTO movie(title, directed_by) VALUES (?, ?), (?, ?)")
                    .param("Tenet")
                    .param(directorName)
                    .paramNull()  // ← NOT NULL violation triggers savepoint rollback
                    .param(directorName)))

    .execute(dataSource);
// Result: director is saved, movie inserts are rolled back to savepoint
```

---

## Custom RowMapper

`RowMapper<T>` is a functional interface — use a lambda or method reference:

```java
// Lambda
Movie movie = SqlBuilder
    .prepareSql("SELECT id, title, directed_by FROM movie WHERE id = ?")
        .param(1)
    .queryForOne(rs -> new Movie(rs.getShort(1), rs.getString(2), rs.getString(3)))
    .execute(dataSource);

// Method reference
List<Movie> movies = SqlBuilder
    .prepareSql("SELECT id, title, directed_by FROM movie")
    .queryForList(BaseTest::mapMovie)
    .execute(dataSource);
```

You can also define a reusable mapper:

```java
RowMapper<Movie> movieMapper = rs -> new Movie(
    rs.getShort("id"),
    rs.getString("title"),
    rs.getString("directed_by")
);

Movie movie = SqlBuilder
    .prepareSql("SELECT id, title, directed_by FROM movie WHERE id = ?")
        .param(1)
    .queryForOne(movieMapper)
    .execute(dataSource);

List<Movie> movies = SqlBuilder
    .prepareSql("SELECT id, title, directed_by FROM movie")
    .queryForList(movieMapper)
    .execute(dataSource);
```

---

## Null Parameters

Set a parameter value to SQL `NULL`:

```java
// Generic null
SqlBuilder
    .prepareSql("INSERT INTO movie(title, directed_by) VALUES (?, ?)")
        .param("Unknown Title")
        .paramNull()       // directed_by = NULL
    .execute(dataSource);
```

Set `NULL` for a specific SQL type with a type name (for `STRUCT`, `ARRAY`, etc.):

```java
SqlBuilder
    .prepareCall("CALL insert_movie_in(?, ?)")
        .param("Inception", Types.VARCHAR)
        .paramNull(Types.VARCHAR, "VARCHAR")
    .execute(dataSource);
```

---

## Supported Parameter Types

`.param(value)` is overloaded for all standard JDBC types:

| Java Type | Method Signature |
|---|---|
| `String` | `.param(String value)` |
| `Integer` | `.param(Integer value)` |
| `Long` | `.param(Long value)` |
| `Short` | `.param(Short value)` |
| `Byte` | (via `Object`) |
| `byte[]` | `.param(byte[] value)` |
| `Double` | `.param(Double value)` |
| `Float` | `.param(Float value)` |
| `BigDecimal` | `.param(BigDecimal value)` |
| `Boolean` | `.param(Boolean value)` |
| `java.sql.Date` | `.param(java.sql.Date value)` |
| `java.sql.Time` | `.param(java.sql.Time value)` |
| `java.sql.Timestamp` | `.param(java.sql.Timestamp value)` |
| `Object` | `.param(Object value)` |
| `Object + sqlType` | `.param(Object value, int targetSqlType)` |
| `NULL` | `.paramNull()` |
| `NULL (typed)` | `.paramNull(int sqlType, String typeName)` |

---

## Framework Integration

SQL Builder is framework-independent and works anywhere you have a `javax.sql.DataSource`:

- **[Spring Boot Integration](examples/sqlbuilder-springboot)**
- **[Quarkus Integration](examples/sqlbuilder-quarkus)**

Configure your DataSource as you normally would in the framework, then pass it directly to `.execute(dataSource)`.

**Spring Boot example:**

```java
@Repository
public class MovieRepository {

    private final DataSource dataSource;

    public MovieRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public Movie findById(long id) throws SQLException {
        return SqlBuilder
            .prepareSql("SELECT id, title, directed_by FROM movie WHERE id = ?")
                .param(id)
            .queryForOne(rs -> new Movie(rs.getShort(1), rs.getString(2), rs.getString(3)))
            .execute(dataSource);
    }

    public long save(Movie movie) throws SQLException {
        return SqlBuilder
            .prepareSql("INSERT INTO movie(title, directed_by) VALUES (?, ?)")
                .param(movie.title())
                .param(movie.directedBy())
            .queryGeneratedKeyForLong()
            .execute(dataSource);
    }
}
```

**Quarkus (with Agroal datasource) example:**

```java
@ApplicationScoped
public class MovieService {

    @Inject
    DataSource dataSource;

    public List<Movie> findAll() throws SQLException {
        return SqlBuilder
            .prepareSql("SELECT id, title, directed_by FROM movie")
            .queryForList(rs -> new Movie(rs.getShort(1), rs.getString(2), rs.getString(3)))
            .execute(dataSource);
    }
}
```
