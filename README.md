# SQL Builder

**[Spring JDBC Template](https://spring.io/guides/gs/relational-data-access)** and **[MyBatis](https://mybatis.org/mybatis-3/)** have become essential tools for developers managing database interactions in Java applications. They simplify SQL execution, parameter binding, and result mapping. However, these libraries come with learning curves, heavy dependencies, and complex configurations.

**SQL Builder** is a lightweight library designed to provide the same essential functionality as these tools but with a focus on simplicity, readability, and native Java. With SQL Builder, you can streamline your database operations without being tied down by unnecessary complexity.

### What Makes SQL Builder Unique?

- **Inspired by JEP 269**: SQL Builder draws its [design philosophy](https://techatpark.com/posts/api-design/) from **[JEP 269: Convenience Factory Methods for Collections](https://openjdk.org/jeps/269)**, which introduced fluent and optimized APIs for Java collections. Similarly, SQL Builder adopts a fluent approach to make database interactions intuitive and efficient.
- **Minimalistic**: At around 1000 lines of code, SQL Builder is lightweight and avoids the heavy dependencies that often accompany larger frameworks.
- **Native Java**: Built with plain Java, SQL Builder ensures that you have full control over your codebase while benefiting from the convenience of a well-designed API.
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
