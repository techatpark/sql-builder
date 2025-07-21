# SQL Builder

> **SQL Builder** is a lightweight alternative for **[Spring JDBC Client](https://www.baeldung.com/spring-6-jdbcclient-api)** and **[MyBatis](https://mybatis.org/mybatis-3/)** designed to provide the same essential functionality as these tools but with a focus on simplicity, readability, and native Java. With SQL Builder, you can streamline your database operations without being tied down by unnecessary complexity.

## Why Use SQL Builder?
- **Framework Independent:** It can be used in any Java framework ( Quarkus, Spring Boot etc )
- **Native Java:** Built using Java’s standard libraries it is **Lightweight:** and **Simple:** to use

## Goals
1. **No Third-Party Dependencies:** The entire implementation relies on native Java libraries.
2. **Simpler Fluent API:** Focused on readability and minimal configuration.

## Usage
Here’s how you can use **SQL Builder** for common database operations.

### **INSERT Operation**
```java
// Insert multiple rows
int updateRows =
    new SqlBuilder("INSERT INTO movie(title, directed_by) VALUES (?, ?), (?, ?)")
        .param("Dunkirk")
        .param("Nolan")
        .param("Inception")
        .param("Nolan")
        .execute(dataSource);
```

### **GET GENERATED KEYS**
```java
// Insert a row and fetch the generated key
long generatedId = new SqlBuilder("INSERT INTO movie(title, directed_by) VALUES (?, ?)")
    .param("Interstellar")
    .param("Nolan")
    .queryGeneratedKeys(resultSet -> resultSet.getLong(1))
    .execute(dataSource);

```

### **SELECT Operation**
```java
// Fetch a single record by ID
Movie movie = new SqlBuilder("SELECT id, title, directed_by FROM movie WHERE id = ?")
    .param(generatedId)
    .queryForOne(this::mapRow)
    .execute(dataSource);

List<Movie> movies = new SqlBuilder("SELECT id, title, directed_by from movie")
        .queryForList(BaseTest::mapMovie)
        .execute(dataSource);

// Check if the record exists
boolean exists = new SqlBuilder("SELECT id FROM movie WHERE id = ?")
    .param(generatedId)
    .queryForExists()
    .execute(dataSource);
```