# SQL Builder

**Spring JDBC Template** and **MyBatis** have become essential tools for developers managing database interactions in Java applications. They simplify SQL execution, parameter binding, and result mapping. However, these libraries come with learning curves, heavy dependencies, and complex configurations.

**SQL Builder** aims to **provide the same functionality with small (~1000 lines), simple, readable, and native Java** code.

---

## Why Use SQL Builder?
- **Lightweight:** No extra dependencies or frameworks. (~1000 lines of code) 
- **Readable Code:** Fluent API for easy-to-follow queries.
- **Native Java:** Built using Java’s standard libraries.

This project is ideal for developers looking for a simple, dependency-free SQL execution library in Java. Let me know if you’d like additional sections or more examples! 🚀

---

## Goals
1. **No Third-Party Dependencies:** The entire implementation relies on native Java libraries.
2. **Simpler Fluent API:** Focused on readability and minimal configuration.

---
## Description / Usage
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

---

### **GET GENERATED KEYS**
```java
// Insert a row and fetch the generated key
long generatedId = new SqlBuilder("INSERT INTO movie(title, directed_by) VALUES (?, ?)")
    .param("Interstellar")
    .param("Nolan")
    .queryGeneratedKeys(resultSet -> resultSet.getLong(1))
    .execute(dataSource);

```

---

### **SELECT Operation**
```java
// Fetch a single record by ID
Movie movie = new SqlBuilder("SELECT id, title, directed_by FROM movie WHERE id = ?")
    .param(generatedId)
    .queryForOne(this::mapRow)
    .execute(dataSource);

// Check if the record exists
boolean exists = new SqlBuilder("SELECT id FROM movie WHERE id = ?")
    .param(generatedId)
    .queryForExists()
    .execute(dataSource);
```
