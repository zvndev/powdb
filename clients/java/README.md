# PowDB — Java client

Java 21+ client for [PowDB](https://github.com/zvndev/powdb).

## Install (Maven)

```xml
<dependency>
  <groupId>dev.zvn</groupId>
  <artifactId>powdb-client</artifactId>
  <version>0.1.0</version>
</dependency>
```

## Usage

```java
import dev.zvn.powdb.Client;
import dev.zvn.powdb.QueryResult;

try (Client c = Client.connect("127.0.0.1", 5433)) {
    System.out.println("server " + c.serverVersion());

    c.query("create table User { name: string, age: int }");
    c.query("User insert { name = 'Alice', age = 30 }");

    QueryResult r = c.query("User filter .age > 27 { .name, .age }");
    switch (r) {
        case QueryResult.Rows rows -> {
            System.out.println(rows.columns());
            rows.rows().forEach(System.out::println);
        }
        case QueryResult.Scalar s -> System.out.println(s.value());
        case QueryResult.Ok ok -> System.out.println(ok.affected());
    }
}
```

`Client` is **not** thread-safe; serialise calls externally or use one client
per thread.

## Testing

```bash
cd clients/java && mvn test
```

## License

MIT
