# turso_dart

Dart/Flutter binding for turso database.

## Getting Started

### Add it to your `pubspec.yaml`.

```
turso_dart:
```

### Create the client

- In memory

```dart
final client = TursoClient.memory();
```

- Local

```dart
final dir = await getApplicationCacheDirectory();
final path = '${dir.path}/local.db';
final client = TursoClient.local(path);
```

### Connect

```dart
await client.connect();
```

### Run SQL statements

- Create table

```dart
await client.execute("create table if not exists customers (id integer primary key, name text);");
```

- Insert query

```dart
await client.query("insert into customers(name) values ('John Doe')");
```

- Select query

```dart
print(await client.query("select * from customers"));
```

- Prepared statement

```dart
final statement = await client
	.prepare("select * from customers where id = ?");
await statement.query(positional: [1])
```
