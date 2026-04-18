# SQL Builder — Framework Integration Examples

SQL Builder is framework-independent and works with any Java framework that provides a `javax.sql.DataSource`. This directory contains ready-to-run example projects demonstrating how to integrate SQL Builder in popular frameworks.

> For the full SQL Builder API reference, see the [complete documentation](../docs/COMPLETE_DOCS.md).

---

## Available Examples

| Framework | Directory | Description |
|---|---|---|
| Spring Boot | [sqlbuilder-springboot](sqlbuilder-springboot) | Spring Boot with `DataSource` auto-configuration |
| Quarkus | [sqlbuilder-quarkus](sqlbuilder-quarkus) | Quarkus with Agroal connection pool |

---

## Spring Boot

### Dependencies

Add the following to your `pom.xml` (**JDK 17+** required):

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-data-jdbc</artifactId>
</dependency>

<dependency>
    <groupId>org.tamilnadujug</groupId>
    <artifactId>sql-builder</artifactId>
    <version>${sql-builder.version}</version>
</dependency>

<!-- JDBC Driver -->
<dependency>
    <groupId>org.postgresql</groupId>
    <artifactId>postgresql</artifactId>
    <version>${postgresql.version}</version>
</dependency>
```

### Configuration

`src/main/resources/application.yml`:

```yml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/sampledb
    username: sampledb
    password: sampledb
```

### Usage

Inject `DataSource` via constructor injection and pass it directly to `.execute(dataSource)`:

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

    public List<Movie> findAll() throws SQLException {
        return SqlBuilder
            .prepareSql("SELECT id, title, directed_by FROM movie")
            .queryForList(rs -> new Movie(rs.getShort(1), rs.getString(2), rs.getString(3)))
            .execute(dataSource);
    }

    public int delete(long id) throws SQLException {
        return SqlBuilder
            .prepareSql("DELETE FROM movie WHERE id = ?")
                .param(id)
            .execute(dataSource);
    }
}
```

**Example Application entry point:**

```java
@SpringBootApplication
public class Application implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(Application.class);

    @Autowired
    private MovieRepository movieRepository;

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        long id = movieRepository.save(new Movie(null, "Titanic", "James Cameron"));
        Movie movie = movieRepository.findById(id);
        log.info("Movie saved and retrieved: {}", movie);
    }
}
```

→ See full example: [sqlbuilder-springboot](sqlbuilder-springboot)

---

## Quarkus

### Dependencies

Add the following to your `pom.xml` (**JDK 17+** required):

```xml
<dependency>
    <groupId>io.quarkus</groupId>
    <artifactId>quarkus-jdbc-postgresql</artifactId>
</dependency>

<dependency>
    <groupId>io.quarkus</groupId>
    <artifactId>quarkus-agroal</artifactId>
</dependency>

<dependency>
    <groupId>org.tamilnadujug</groupId>
    <artifactId>sql-builder</artifactId>
    <version>${sql-builder.version}</version>
</dependency>
```

### Configuration

`src/main/resources/application.properties`:

```properties
quarkus.datasource.db-kind=postgresql
quarkus.datasource.username=sampledb
quarkus.datasource.password=sampledb
quarkus.datasource.jdbc.url=jdbc:postgresql://localhost:5432/sampledb
```

### Usage

Use `@Inject` to obtain the `DataSource` provided by Agroal and pass it directly to `.execute(dataSource)`:

```java
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.List;

@ApplicationScoped
public class MovieService {

    @Inject
    DataSource dataSource;

    public Movie create(Movie movie) throws SQLException {
        return SqlBuilder
            .sql("INSERT INTO movie(title, directed_by) VALUES ('Interstellar', 'Nolan') RETURNING id, title, directed_by")
            .queryForOne(rs -> new Movie(
                    rs.getShort(1),
                    rs.getString(2),
                    rs.getString(3)))
            .execute(dataSource);
    }

    public List<Movie> findAll() throws SQLException {
        return SqlBuilder
            .prepareSql("SELECT id, title, directed_by FROM movie")
            .queryForList(rs -> new Movie(rs.getShort(1), rs.getString(2), rs.getString(3)))
            .execute(dataSource);
    }

    public Movie findById(long id) throws SQLException {
        return SqlBuilder
            .prepareSql("SELECT id, title, directed_by FROM movie WHERE id = ?")
                .param(id)
            .queryForOne(rs -> new Movie(rs.getShort(1), rs.getString(2), rs.getString(3)))
            .execute(dataSource);
    }

    public int delete(long id) throws SQLException {
        return SqlBuilder
            .prepareSql("DELETE FROM movie WHERE id = ?")
                .param(id)
            .execute(dataSource);
    }
}
```

**Example Application entry point:**

```java
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

@QuarkusMain
public class Application implements QuarkusApplication {

    private static final Logger log = Logger.getLogger(Application.class);

    @Inject
    MovieService movieService;

    public static void main(String... args) {
        Quarkus.run(Application.class, args);
    }

    @Override
    public int run(String... args) throws Exception {
        Movie movie = movieService.create(new Movie(null, "Titanic", "James Cameron"));
        log.info("Movie Created: " + movie);
        return 0;
    }
}
```

→ See full example: [sqlbuilder-quarkus](sqlbuilder-quarkus)

---

## Running the Examples

Both examples use Docker Compose to spin up a PostgreSQL instance. From the example project directory:

```bash
# Start PostgreSQL
docker-compose up -d

# Run the application
./mvnw spring-boot:run        # Spring Boot
./mvnw quarkus:dev            # Quarkus (dev mode with live reload)
```

---

## How It Works (Core Pattern)

Regardless of the framework, the integration pattern is always the same:

1. Let the framework manage the `DataSource` (connection pooling, config, lifecycle).
2. Inject or obtain the `DataSource` in your service/repository.
3. Pass it to `.execute(dataSource)` — SQL Builder handles the rest.

```
Framework DataSource
        │
        ▼
  SqlBuilder.prepareSql(...)
      .param(...)
      .queryForOne(...)
      .execute(dataSource)   ← framework-provided DataSource
```

No additional adapters, wrappers, or configuration needed.