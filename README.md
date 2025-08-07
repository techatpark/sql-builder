# SQL Builder

> **SQL Builder** is a lightweight alternative for **[Spring JDBC Client](https://www.baeldung.com/spring-6-jdbcclient-api)** and **[MyBatis](https://mybatis.org/mybatis-3/)** designed to provide the same essential functionality as these tools but with a focus on simplicity, readability, and native Java. With SQL Builder, you can streamline your database operations without being tied down by unnecessary complexity.

## Why Use SQL Builder?

- **Simpler Fluent API:** Focused on readability and minimal configuration.
- **Framework Independent:** It can be used in any Java framework ( Quarkus, Spring Boot etc )
- **Lightweight:** and **Simple:** to use with no third party dependencies

## Usage

Add dependency to your maven project

```xml
<dependency>
    <groupId>com.techatpark</groupId>
    <artifactId>sql-builder</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

Thats all, You can now build and execute [Queries](#queries) and [Batch](#batch)

### Queries

For SQL Queries, You can use `SqlBuilder.sql` and `SqlBuilder.prepareSql` (for queries with parameters)

#### INSERT

```java
// Insert multiple rows
int updateRows = SqlBuilder
                .sql("INSERT INTO movie(title, directed_by) VALUES ('Dunkirk', 'Nolan')")
                .execute(dataSource);
```

With Parameters

```java
// Insert multiple rows
int updateRows = SqlBuilder
                .prepareSql("INSERT INTO movie(title, directed_by) VALUES (?, ?)")
                    .param("Dunkirk")
                    .param("Nolan")
                .execute(dataSource);
```

#### GENERATED KEYS
```java
// Insert a row and fetch the generated key
long generatedId = SqlBuilder
                .prepareSql("INSERT INTO movie(title, directed_by) VALUES (?, ?)")
                    .param("Interstellar")
                    .param("Nolan")
                .queryGeneratedKeys(resultSet -> resultSet.getLong(1))
                .execute(dataSource);
```

#### SELECT

```java
// Fetch a single record by ID
Movie movie = SqlBuilder
                .prepareSql("SELECT id, title, directed_by FROM movie WHERE id = ?")
                    .param(generatedId)
                .queryForOne(this::mapRow)
                .execute(dataSource);

List<Movie> movies = SqlBuilder
                .prepareSql("SELECT id, title, directed_by from movie")
                    .queryForList(this::mapRow)
                .execute(dataSource);

// Check if the record exists
boolean exists = SqlBuilder
        .prepareSql("SELECT id FROM movie WHERE id = ?")
            .param(generatedId)
            .queryForExists()
        .execute(dataSource);
```

### Batch

From SQL,

```java
int[] updatedRows = SqlBuilder
                .sql("INSERT INTO movie(title, directed_by) VALUES ('Interstellar', 'Nolan')")
                    .addBatch("INSERT INTO movie(title, directed_by) VALUES ('Dunkrik', 'Nolan'),('Inception', 'Nolan')")
                .executeBatch(dataSource);
```

From Prepared SQL,

```java
int[] updatedRows = SqlBuilder
                .prepareSql("INSERT INTO movie(title, directed_by) VALUES (?, ?)")
                    .param("Interstellar")
                    .param("Nolan")
                .addBatch()
                    .param("Dunkrik")
                    .param("Nolan")
                .executeBatch(dataSource);
```