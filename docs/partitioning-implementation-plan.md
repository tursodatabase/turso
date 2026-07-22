# План реализации Time-Partitioning для Turso

> Это исторический проектный черновик. Актуальное реализованное поведение,
> ограничения и API описаны в [time-partitioning.md](time-partitioning.md).
> В частности, некоторые предложения ниже (например, логический
> `CREATE INDEX` и vector/FTS по архиву) пока не входят в поддерживаемый
> контракт.

## Обзор

Реализация автоматического партиционирования таблиц по времени для задач видеоаналитики. Данные автоматически распределяются по файлам (по умолчанию — суточным), при этом для пользователя БД это выглядит как обычная таблица.

### Цели

- Прозрачность для разработчика — работа с партиционированной таблицей как с обычной
- Автоматическая маршрутизация INSERT в нужный файл по timestamp
- Прозрачные SELECT запросы по всем партициям
- API для внешней системы: список файлов с диапазонами, attach/detach
- Минимальные изменения в ядре, изоляция кода в отдельном модуле

### Ограничения (by design)

- Кросс-партиционные транзакции для записи запрещены
- Глобальные уникальные индексы не поддерживаются
- TTL/ротация реализуется внешним механизмом
- SQL API управления партициями отключается в production (только Rust API)

### Vector Search и Full-Text Search

**Поддержка в партициях:**

| Функция | Поддержка | Ограничения |
|---------|-----------|-------------|
| Обычные индексы (B-tree) | ✅ Полная | Создаются в каждой партиции |
| Vector Search (`toy_vector_sparse_ivf`) | ✅ Работает | Результаты объединяются через UNION ALL |
| FTS (`fts`) | ⚠️ Частичная | Score несопоставим между партициями |

**Как работают индексы в партициях:**

```sql
-- При CREATE INDEX на партиционированной таблице:
CREATE INDEX idx_plate ON events(plate);

-- Индекс создаётся в КАЖДОЙ партиции:
-- events_20260120.db: CREATE INDEX idx_plate ON events(plate)
-- events_20260121.db: CREATE INDEX idx_plate ON events(plate)
-- events_20260122.db: CREATE INDEX idx_plate ON events(plate)
```

**Проблема с FTS score:**

FTS использует статистику корпуса (TF-IDF, BM25) для вычисления релевантности.
Каждая партиция имеет свою статистику → scores несопоставимы между партициями.

```sql
-- ПРОБЛЕМА: результаты из разных партиций нельзя корректно отсортировать по score
SELECT *, fts_score() as score FROM events WHERE events MATCH 'query' ORDER BY score;
```

**Рекомендации:**

1. **FTS** — использовать только в справочных таблицах (main.db), не в партиционированных
2. **Vector Search** — работает, но для точного KNN по всем данным нужен post-processing
3. **Обычные индексы** — работают без ограничений

**Для видеоаналитики:** Номера машин ищутся точным совпадением (`plate = 'A001BC'`), FTS не требуется. Индекс на `plate` будет работать корректно в каждой партиции.

### Архитектурные решения

**Множественные инстансы БД:**
- Каждый плагин видеоаналитики получает полностью независимый инстанс `Database`
- Разные пути к main.db = разные инстансы (через `DATABASE_MANAGER` registry)
- Никакого shared state между плагинами

**Гибкие пути к партициям:**
- Пути генерируются через callback (`PartitionPathResolver` trait)
- Разработчик реализует свою логику путей
- Поддержка произвольной структуры каталогов (например `/2026-01-05/{plugin_id}.bin`)

---

## Фаза 1: Parser и AST

### Задачи

- [ ] 1.1 Добавить `PartitionSpec` в AST
- [ ] 1.2 Добавить парсинг `PARTITION BY (column)` в `parse_create_table`
- [ ] 1.3 Добавить форматирование `PartitionSpec` в `ast/fmt.rs`
- [ ] 1.4 Тесты парсера

### Изменения в файлах

**`parser/src/ast.rs`**
```rust
/// Спецификация партиционирования таблицы
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct PartitionSpec {
    /// Колонка для партиционирования
    pub column: Name,
}
```

### Поддерживаемые типы timestamp

Колонка партиционирования может быть:

| SQL тип | Rust enum | Точность | Рекомендация |
|---------|-----------|----------|--------------|
| `INTEGER` | `UnixMicros` | микросекунды | **Рекомендуется** для видеоаналитики |
| `INTEGER` | `UnixMillis` | миллисекунды | Совместимость с JS |
| `INTEGER` | `UnixSeconds` | секунды | Простые случаи |
| `BLOB` | `TimeBlob` | наносекунды | Максимальная точность |

```rust
/// Тип timestamp колонки для партиционирования
#[derive(Clone, Copy, Debug, Default)]
pub enum PartitionTimestampType {
    #[default]
    UnixMicros,   // INTEGER: микросекунды с Unix epoch
    UnixMillis,   // INTEGER: миллисекунды
    UnixSeconds,  // INTEGER: секунды
    TimeBlob,     // BLOB: нативный Time (наносекунды)
}

impl PartitionTimestampType {
    /// Конвертировать значение в день (для определения партиции)
    pub fn to_day(&self, value: &Value) -> Result<chrono::NaiveDate> {
        let micros = match self {
            Self::UnixMicros => value.to_integer()?,
            Self::UnixMillis => value.to_integer()? * 1_000,
            Self::UnixSeconds => value.to_integer()? * 1_000_000,
            Self::TimeBlob => {
                let blob = value.to_blob()?;
                Time::try_from(blob)?.to_unix_micro()
            }
        };
        let dt = DateTime::from_timestamp_micros(micros)
            .ok_or(PartitionError::InvalidTimestamp)?;
        Ok(dt.date_naive())
    }
}
```

**Таймзоны:**
- Внутреннее хранение: **всегда UTC**
- Партиционирование: по UTC-дню
- Отображение: конвертация при выводе через `time_fmt_datetime(ts, offset_seconds)`

**Пример для видеоаналитики:**
```sql
-- Создание таблицы с микросекундным timestamp
CREATE TABLE events (
    plate TEXT,
    ts INTEGER,  -- Unix microseconds: 1737590400123456
    gate TEXT,
    confidence REAL
) PARTITION BY (ts);

-- INSERT с микросекундами
INSERT INTO events VALUES ('A001BC', 1737590400123456, 'gate1', 0.95);

-- SELECT с форматированием в локальную таймзону (UTC+3)
SELECT plate, time_fmt_datetime(time_micro(ts), 10800) as local_time
FROM events
WHERE ts >= 1737504000000000;  -- >= 2026-01-22 00:00:00 UTC
```

// Добавить в CreateTableBody::ColumnsAndConstraints:
pub enum CreateTableBody {
    ColumnsAndConstraints {
        columns: Vec<ColumnDefinition>,
        constraints: Vec<NamedTableConstraint>,
        options: TableOptions,
        partition: Option<PartitionSpec>,  // NEW
    },
    AsSelect(Select),
}
```

**`parser/src/parser.rs`**
```rust
fn parse_create_table_args(&mut self, tbl_name: &QualifiedName) -> Result<CreateTableBody> {
    // ... существующий код ...

    // После парсинга options (WITHOUT ROWID, STRICT)
    let partition = self.parse_partition_spec()?;

    // ...
}

fn parse_partition_spec(&mut self) -> Result<Option<PartitionSpec>> {
    // PARTITION BY (column_name)
    if !self.peek_keyword(TK_PARTITION) {
        return Ok(None);
    }
    eat_assert!(self, TK_PARTITION);
    eat_expect!(self, TK_BY);
    eat_expect!(self, TK_LP);  // (
    let column = self.parse_column_name()?;
    eat_expect!(self, TK_RP);  // )
    Ok(Some(PartitionSpec { column }))
}
```

### Тесты

```rust
#[test]
fn test_parse_partition_by() {
    let sql = "CREATE TABLE events (id INT, ts INTEGER, data TEXT) PARTITION BY (ts)";
    let stmt = parse(sql).unwrap();
    // assert partition spec
}

#[test]
fn test_parse_partition_by_with_options() {
    let sql = "CREATE TABLE events (id INT, ts INTEGER) WITHOUT ROWID PARTITION BY (ts)";
    // ...
}
```

---

## Фаза 2: Schema и метаданные

### Задачи

- [ ] 2.1 Добавить `PartitionSpec` в `BTreeTable`
- [ ] 2.2 Сохранение partition metadata в sqlite_master
- [ ] 2.3 Загрузка partition metadata при открытии БД
- [ ] 2.4 Валидация: partition column должна существовать и быть INTEGER

### Изменения в файлах

**`core/schema.rs`**
```rust
pub struct BTreeTable {
    // ... существующие поля ...

    /// Спецификация партиционирования (если таблица партиционирована)
    pub partition_spec: Option<PartitionSpec>,
}

#[derive(Clone, Debug)]
pub struct PartitionSpec {
    /// Имя колонки для партиционирования
    pub column: String,
    /// Индекс колонки в таблице
    pub column_index: usize,
}
```

**Хранение в sqlite_master:**
```sql
-- Партиционированные таблицы хранятся с модифицированным SQL
-- type='table', name='events', sql='CREATE TABLE events (...) PARTITION BY (ts)'
```

---

## Фаза 3: Partition Manager (новый модуль)

### Структура модуля

```
core/partition/
├── mod.rs              # Публичный API
├── manager.rs          # PartitionManager
├── file.rs             # PartitionFile, работа с файлами
├── routing.rs          # Роутинг INSERT
├── discovery.rs        # Обнаружение файлов партиций
└── error.rs            # Ошибки партиционирования
```

### Задачи

- [ ] 3.1 Создать структуру модуля
- [ ] 3.2 Реализовать `PartitionManager`
- [ ] 3.3 Реализовать `PartitionFile` и работу с метаданными
- [ ] 3.4 Реализовать роутинг по timestamp
- [ ] 3.5 Реализовать discovery файлов партиций
- [ ] 3.6 Интеграция с `Connection`
- [ ] 3.7 Тесты

### API

**`core/partition/mod.rs`**
```rust
pub use manager::PartitionManager;
pub use file::{PartitionFile, PartitionInfo};
pub use error::PartitionError;
pub use path_resolver::PartitionPathResolver;
```

**`core/partition/path_resolver.rs`** (новый файл)
```rust
use std::path::{Path, PathBuf};

/// Trait для генерации путей к файлам партиций.
/// Реализуется пользователем для гибкой настройки структуры каталогов.
pub trait PartitionPathResolver: Send + Sync {
    /// Сгенерировать путь к файлу партиции по timestamp.
    ///
    /// # Arguments
    /// * `table` - имя таблицы
    /// * `timestamp` - unix timestamp записи
    ///
    /// # Returns
    /// Полный путь к файлу партиции
    fn resolve_path(&self, table: &str, timestamp: i64) -> PathBuf;

    /// Распарсить путь обратно в диапазон timestamp.
    /// Используется при discovery существующих партиций.
    ///
    /// # Returns
    /// `Some((start, end))` - диапазон в unix timestamp
    /// `None` - если путь не соответствует формату
    fn parse_path(&self, path: &Path) -> Option<(i64, i64)>;

    /// Получить glob pattern для поиска существующих файлов партиций.
    ///
    /// # Example
    /// `/data/*/lpr.bin` или `/data/events_*.db`
    fn glob_pattern(&self, table: &str) -> String;

    /// Интервал партиции в секундах.
    /// По умолчанию 86400 (сутки).
    fn interval_seconds(&self) -> u64 {
        86400
    }
}

/// Стандартная реализация для простых случаев.
/// Файлы в формате: `{directory}/{table}_{YYYY-MM-DD}.db`
pub struct DefaultPathResolver {
    pub directory: PathBuf,
    pub interval_seconds: u64,
}

impl PartitionPathResolver for DefaultPathResolver {
    fn resolve_path(&self, table: &str, timestamp: i64) -> PathBuf {
        let date = chrono::DateTime::from_timestamp(timestamp, 0)
            .unwrap()
            .format("%Y-%m-%d")
            .to_string();
        self.directory.join(format!("{}_{}.db", table, date))
    }

    fn parse_path(&self, path: &Path) -> Option<(i64, i64)> {
        let filename = path.file_stem()?.to_str()?;
        // Parse "events_2026-01-22" -> extract date
        let date_part = filename.rsplit('_').next()?;
        let date = chrono::NaiveDate::parse_from_str(date_part, "%Y-%m-%d").ok()?;
        let start = date.and_hms_opt(0, 0, 0)?.and_utc().timestamp();
        let end = start + self.interval_seconds as i64;
        Some((start, end))
    }

    fn glob_pattern(&self, table: &str) -> String {
        format!("{}/{}_*.db", self.directory.display(), table)
    }

    fn interval_seconds(&self) -> u64 {
        self.interval_seconds
    }
}

/// Реализация для видеоаналитики.
/// Файлы в формате: `{base_dir}/{YYYY-MM-DD}/{plugin_id}.bin`
pub struct VideoAnalyticsPathResolver {
    pub base_dir: PathBuf,
    pub plugin_id: String,
    pub interval_seconds: u64,
}

impl PartitionPathResolver for VideoAnalyticsPathResolver {
    fn resolve_path(&self, _table: &str, timestamp: i64) -> PathBuf {
        let date = chrono::DateTime::from_timestamp(timestamp, 0)
            .unwrap()
            .format("%Y-%m-%d")
            .to_string();

        // /data/2026-01-05/plugin_lpr.bin
        self.base_dir
            .join(&date)
            .join(format!("{}.bin", self.plugin_id))
    }

    fn parse_path(&self, path: &Path) -> Option<(i64, i64)> {
        // Verify filename matches plugin_id
        let filename = path.file_stem()?.to_str()?;
        if filename != self.plugin_id {
            return None;
        }

        // Parse date from parent directory
        let parent = path.parent()?;
        let date_str = parent.file_name()?.to_str()?;
        let date = chrono::NaiveDate::parse_from_str(date_str, "%Y-%m-%d").ok()?;

        let start = date.and_hms_opt(0, 0, 0)?.and_utc().timestamp();
        let end = start + self.interval_seconds as i64;
        Some((start, end))
    }

    fn glob_pattern(&self, _table: &str) -> String {
        format!("{}/*/{}.bin", self.base_dir.display(), self.plugin_id)
    }

    fn interval_seconds(&self) -> u64 {
        self.interval_seconds
    }
}
```

**Конфигурация партиционирования:**
```rust
/// Конфигурация партиционирования таблицы
pub struct PartitionConfig {
    /// Resolver для генерации путей
    pub path_resolver: Box<dyn PartitionPathResolver>,
    /// SQL схемы таблицы (для создания новых партиций)
    pub schema_sql: String,
}
```

**`core/partition/manager.rs`**
```rust
pub struct PartitionManager {
    /// Таблица → конфигурация
    configs: HashMap<String, PartitionConfig>,
    /// Таблица → список партиций
    partitions: HashMap<String, Vec<PartitionFile>>,
    /// Ссылка на connection для ATTACH/DETACH
    connection: Weak<Connection>,
}

impl PartitionManager {
    /// Создать новый менеджер
    pub fn new() -> Self;

    /// Зарегистрировать партиционированную таблицу
    pub fn register_table(
        &mut self,
        table_name: &str,
        config: PartitionConfig,
    ) -> Result<()>;

    /// Получить список партиций с диапазонами
    pub fn list_partitions(&self, table: &str) -> Result<Vec<PartitionInfo>>;

    /// Подключить файл партиции
    pub fn attach_partition(&mut self, table: &str, path: &Path) -> Result<PartitionInfo>;

    /// Отключить файл партиции
    pub fn detach_partition(&mut self, table: &str, path: &Path) -> Result<()>;

    /// Определить партицию для INSERT по timestamp
    pub fn route_insert(&self, table: &str, timestamp: i64) -> Result<&PartitionFile>;

    /// Получить все подключённые партиции для SELECT
    pub fn get_attached_partitions(&self, table: &str) -> Vec<&PartitionFile>;

    /// Отфильтровать партиции по диапазону (для partition pruning)
    pub fn filter_by_range(
        &self,
        table: &str,
        start: Option<i64>,
        end: Option<i64>,
    ) -> Vec<&PartitionFile>;

    /// Создать новую партицию для timestamp (если не существует)
    pub fn ensure_partition(&mut self, table: &str, timestamp: i64) -> Result<&PartitionFile>;
}
```

**`core/partition/file.rs`**
```rust
#[derive(Clone, Debug)]
pub struct PartitionFile {
    /// Путь к файлу
    pub path: PathBuf,
    /// Алиас для ATTACH (например "events_20260122")
    pub db_alias: String,
    /// Начало диапазона (unix timestamp, включительно)
    pub range_start: i64,
    /// Конец диапазона (unix timestamp, исключительно)
    pub range_end: i64,
    /// Подключён ли файл в данный момент
    pub attached: bool,
    /// Database ID (после ATTACH)
    pub database_id: Option<usize>,
}

/// Информация о партиции для внешнего API
#[derive(Clone, Debug)]
pub struct PartitionInfo {
    pub file_path: String,
    pub db_alias: String,
    pub range_start: i64,
    pub range_end: i64,
    pub range_start_iso: String,  // "2026-01-22T00:00:00Z"
    pub range_end_iso: String,
    pub attached: bool,
    pub size_bytes: u64,
}

impl PartitionFile {
    /// Создать новый файл партиции
    pub fn create(
        directory: &Path,
        table_name: &str,
        timestamp: i64,
        interval_seconds: u64,
        schema_sql: &str,
    ) -> Result<Self>;

    /// Открыть существующий файл и прочитать метаданные
    pub fn open(path: &Path) -> Result<Self>;

    /// Проверить, попадает ли timestamp в диапазон
    pub fn contains(&self, timestamp: i64) -> bool {
        timestamp >= self.range_start && timestamp < self.range_end
    }
}
```

---

## Фаза 4: Translate — INSERT routing

### Задачи

- [ ] 4.1 Определение партиционированной таблицы в translate
- [ ] 4.2 Извлечение значения partition column из VALUES
- [ ] 4.3 Генерация INSERT в нужную партицию
- [ ] 4.4 Обработка batch INSERT (все записи в одну партицию)
- [ ] 4.5 Ошибка при кросс-партиционном INSERT в одной транзакции
- [ ] 4.6 Тесты

### Изменения в файлах

**`core/translate/insert.rs`**
```rust
pub fn translate_insert(...) -> Result<ProgramBuilder> {
    // Проверяем, партиционирована ли таблица
    if let Some(partition_spec) = table.partition_spec.as_ref() {
        return translate_partitioned_insert(
            table,
            partition_spec,
            columns,
            values,
            resolver,
            program,
            connection,
        );
    }

    // Существующая логика для обычных таблиц
    // ...
}
```

**`core/partition/insert.rs`** (новый файл)
```rust
pub fn translate_partitioned_insert(
    table: &BTreeTable,
    partition_spec: &PartitionSpec,
    columns: &[Name],
    values: &[Vec<Expr>],
    resolver: &Resolver,
    program: ProgramBuilder,
    connection: &Connection,
) -> Result<ProgramBuilder> {
    // 1. Найти индекс partition column в INSERT columns
    let partition_col_idx = find_partition_column_index(columns, partition_spec)?;

    // 2. Для каждой строки VALUES:
    //    - Извлечь значение timestamp
    //    - Определить целевую партицию
    //    - Сгруппировать строки по партициям

    // 3. Проверить, что все строки идут в одну партицию
    //    (или разрешить несколько, если не в транзакции)

    // 4. Сгенерировать INSERT для каждой партиции
    //    INSERT INTO partition_alias.table_name VALUES (...)

    // ...
}
```

### Генерируемый байткод

```
-- Исходный запрос:
INSERT INTO events (plate, ts, gate) VALUES ('A001', 1737500000, 'gate1');

-- После трансформации (ts=1737500000 → 2026-01-22):
-- 1. Определить партицию: events_20260122
-- 2. Убедиться, что она attached
-- 3. Выполнить:
INSERT INTO events_20260122.events (plate, ts, gate) VALUES ('A001', 1737500000, 'gate1');
```

---

## Фаза 5: Translate — SELECT federation

### Задачи

- [ ] 5.1 Определение партиционированной таблицы в SELECT
- [ ] 5.2 Partition pruning по WHERE условиям
- [ ] 5.3 Генерация UNION ALL по партициям
- [ ] 5.4 Оптимизация: передача WHERE внутрь каждой партиции
- [ ] 5.5 Обработка ORDER BY и LIMIT
- [ ] 5.6 Тесты

### Изменения в файлах

**`core/translate/select.rs`**
```rust
pub fn translate_select(...) -> Result<ProgramBuilder> {
    // При обработке FROM clause
    if is_partitioned_table(table_name, connection) {
        return translate_partitioned_select(...);
    }
    // ...
}
```

**`core/partition/select.rs`** (новый файл)
```rust
pub fn translate_partitioned_select(
    table: &BTreeTable,
    partition_spec: &PartitionSpec,
    select: &Select,
    resolver: &Resolver,
    program: ProgramBuilder,
    connection: &Connection,
) -> Result<ProgramBuilder> {
    // 1. Проанализировать WHERE clause для partition pruning
    let (range_start, range_end) = extract_partition_range(
        &select.where_clause,
        partition_spec,
    )?;

    // 2. Получить список релевантных партиций
    let partitions = connection
        .partition_manager()
        .filter_by_range(table.name, range_start, range_end);

    // 3. Сгенерировать UNION ALL
    //    SELECT ... FROM p1.table WHERE ...
    //    UNION ALL
    //    SELECT ... FROM p2.table WHERE ...
    //    ...

    // 4. Обработать ORDER BY (если есть) — добавить внешнюю сортировку

    // 5. Обработать LIMIT — добавить к каждой партиции + финальный LIMIT

    // ...
}

/// Извлечь диапазон timestamp из WHERE clause
fn extract_partition_range(
    where_clause: &Option<Expr>,
    partition_spec: &PartitionSpec,
) -> Result<(Option<i64>, Option<i64>)> {
    // Анализируем выражения вида:
    // - ts = X
    // - ts > X, ts >= X
    // - ts < X, ts <= X
    // - ts BETWEEN X AND Y
    // - ts IN (X, Y, Z)
    // ...
}
```

### Примеры трансформации

```sql
-- Исходный запрос:
SELECT * FROM events WHERE ts >= 1737500000;

-- После partition pruning (только партиции >= 2026-01-22):
SELECT * FROM events_20260122.events WHERE ts >= 1737500000
UNION ALL
SELECT * FROM events_20260123.events WHERE ts >= 1737500000;

-- С ORDER BY и LIMIT:
SELECT * FROM events WHERE ts >= 1737500000 ORDER BY ts LIMIT 100;

-- Трансформируется в:
SELECT * FROM (
    SELECT * FROM events_20260122.events WHERE ts >= 1737500000
    UNION ALL
    SELECT * FROM events_20260123.events WHERE ts >= 1737500000
) ORDER BY ts LIMIT 100;
```

---

## Фаза 6: SQL функции для управления партициями

### Задачи

- [ ] 6.1 Функция `partition_list(table)` — список партиций
- [ ] 6.2 Функция `partition_attach(table, path)` — подключить партицию
- [ ] 6.3 Функция `partition_detach(table, path)` — отключить партицию
- [ ] 6.4 Функция `partition_info(table)` — информация о конфигурации
- [ ] 6.5 Регистрация функций как table-valued functions
- [ ] 6.6 Тесты

### API

```sql
-- Получить список партиций
SELECT * FROM partition_list('events');
-- Результат:
-- | file_path                    | alias          | range_start | range_end   | attached | size_mb |
-- | /data/events_20260120.db     | events_20260120| 1737331200  | 1737417600  | true     | 125.5   |
-- | /data/events_20260121.db     | events_20260121| 1737417600  | 1737504000  | true     | 142.3   |
-- | /data/events_20260122.db     | events_20260122| 1737504000  | 1737590400  | true     | 98.7    |

-- Подключить партицию
SELECT partition_attach('events', '/data/events_20260119.db');

-- Отключить партицию
SELECT partition_detach('events', '/data/events_20260119.db');

-- Информация о конфигурации
SELECT * FROM partition_info('events');
-- | table  | partition_column | directory      | interval_seconds |
-- | events | ts               | /data/parking  | 86400            |
```

### Реализация

**`core/partition/functions.rs`**
```rust
pub fn register_partition_functions(ext_api: &mut ExtensionApi) {
    // partition_list как table-valued function
    register_vtab_module(ext_api, "partition_list", PartitionListModule);

    // partition_attach как scalar function
    register_scalar_function(ext_api, "partition_attach", partition_attach_fn);

    // partition_detach как scalar function
    register_scalar_function(ext_api, "partition_detach", partition_detach_fn);

    // partition_info как table-valued function
    register_vtab_module(ext_api, "partition_info", PartitionInfoModule);
}
```

---

## Фаза 7: Интеграция с Connection

### Задачи

- [ ] 7.1 Добавить `PartitionManager` в `Connection`
- [ ] 7.2 Инициализация партиций при открытии БД
- [ ] 7.3 Конфигурация через PRAGMA или отдельный API
- [ ] 7.4 Обработка ошибок партиционирования
- [ ] 7.5 Тесты

### Изменения в файлах

**`core/connection.rs`**
```rust
pub struct Connection {
    // ... существующие поля ...

    #[cfg(feature = "partitioning")]
    partition_manager: RwLock<PartitionManager>,
}

impl Connection {
    #[cfg(feature = "partitioning")]
    pub fn partition_manager(&self) -> &RwLock<PartitionManager> {
        &self.partition_manager
    }

    #[cfg(feature = "partitioning")]
    pub fn configure_partition(
        &self,
        table: &str,
        config: PartitionConfig,
    ) -> Result<()> {
        let mut pm = self.partition_manager.write();
        pm.register_table(table, config)?;
        pm.discover_existing_partitions(table)?;
        Ok(())
    }
}
```

**PRAGMA для конфигурации (опционально):**
```sql
-- Установить директорию для партиций
PRAGMA partition_directory('events', '/data/parking/events');

-- Установить интервал (в секундах)
PRAGMA partition_interval('events', 86400);
```

---

## Фаза 8: CREATE TABLE для партиционированных таблиц

### Задачи

- [ ] 8.1 Валидация partition column при CREATE TABLE
- [ ] 8.2 Создание таблицы в main.db (schema only, без данных)
- [ ] 8.3 Создание первой партиции
- [ ] 8.4 Автоматический ATTACH первой партиции
- [ ] 8.5 Тесты

### Логика

```sql
CREATE TABLE events (
    plate TEXT,
    ts INTEGER,
    gate TEXT
) PARTITION BY (ts);
```

**Что происходит:**
1. Валидация: `ts` существует и имеет тип INTEGER
2. В main.db создаётся запись в sqlite_master (schema)
3. Создаётся первый файл партиции для текущего дня
4. Файл автоматически ATTACH'ится

---

## Фаза 9: DROP TABLE и ALTER TABLE

### Задачи

- [ ] 9.1 DROP TABLE — удаление всех партиций
- [ ] 9.2 ALTER TABLE ADD COLUMN — применить ко всем партициям
- [ ] 9.3 ALTER TABLE DROP COLUMN — применить ко всем партициям
- [ ] 9.4 Запрет ALTER на partition column
- [ ] 9.5 Тесты

---

## Фаза 10: Индексы

### Задачи

- [ ] 10.1 CREATE INDEX — создать во всех attached партициях
- [ ] 10.2 DROP INDEX — удалить из всех партиций
- [ ] 10.3 Автоматическое создание индекса в новых партициях
- [ ] 10.4 Документация ограничений (нет глобальных уникальных индексов)
- [ ] 10.5 Тесты

### Логика

```sql
CREATE INDEX idx_plate ON events(plate);
```

**Что происходит:**
1. Индекс создаётся в каждой attached партиции
2. Метаданные индекса сохраняются в main.db
3. При ATTACH новой партиции — индекс создаётся автоматически

---

## Фаза 11: Транзакции и обработка ошибок

### Задачи

- [ ] 11.1 Отслеживание партиций в текущей транзакции
- [ ] 11.2 Ошибка при попытке записи в >1 партицию в транзакции
- [ ] 11.3 Корректный rollback при ошибке
- [ ] 11.4 Документация поведения
- [ ] 11.5 Тесты

### Логика

```rust
pub struct TransactionPartitionTracker {
    /// Партиция, в которую была первая запись
    write_partition: Option<String>,
}

impl TransactionPartitionTracker {
    pub fn track_write(&mut self, partition: &str) -> Result<()> {
        match &self.write_partition {
            None => {
                self.write_partition = Some(partition.to_string());
                Ok(())
            }
            Some(existing) if existing == partition => Ok(()),
            Some(existing) => Err(PartitionError::CrossPartitionWrite {
                first: existing.clone(),
                second: partition.to_string(),
            }),
        }
    }
}
```

---

## Фаза 12: Тестирование

### Задачи

- [ ] 12.1 Unit тесты для каждого модуля
- [ ] 12.2 Интеграционные тесты
- [ ] 12.3 Тесты производительности
- [ ] 12.4 Тесты recovery (crash + restart)
- [ ] 12.5 Stress тесты (много партиций)
- [ ] 12.6 Тесты совместимости с SQLite

### Тестовые сценарии

```rust
#[test]
fn test_insert_routes_to_correct_partition() { ... }

#[test]
fn test_select_union_all_partitions() { ... }

#[test]
fn test_partition_pruning() { ... }

#[test]
fn test_cross_partition_write_fails() { ... }

#[test]
fn test_attach_detach_partition() { ... }

#[test]
fn test_create_index_on_partitioned_table() { ... }

#[test]
fn test_recovery_after_crash() { ... }
```

---

## Фаза 13: Документация

### Задачи

- [ ] 13.1 Руководство пользователя
- [ ] 13.2 Описание ограничений
- [ ] 13.3 Примеры использования
- [ ] 13.4 API reference
- [ ] 13.5 Руководство по миграции с обычных таблиц

---

## Оценка трудозатрат

| Фаза | Описание | Оценка |
|------|----------|--------|
| 1 | Parser и AST | 2-3 дня |
| 2 | Schema и метаданные | 2-3 дня |
| 3 | Partition Manager | 5-7 дней |
| 4 | INSERT routing | 3-5 дней |
| 5 | SELECT federation | 5-7 дней |
| 6 | SQL функции управления | 2-3 дня |
| 7 | Интеграция с Connection | 2-3 дня |
| 8 | CREATE TABLE | 2-3 дня |
| 9 | DROP/ALTER TABLE | 2-3 дня |
| 10 | Индексы | 3-4 дня |
| 11 | Транзакции | 3-4 дня |
| 12 | Тестирование | 5-7 дней |
| 13 | Документация | 2-3 дня |
| **Итого** | | **38-55 дней** |

---

## Риски и митигация

| Риск | Вероятность | Влияние | Митигация |
|------|-------------|---------|-----------|
| Конфликты с upstream | Средняя | Среднее | Feature flag, изоляция кода |
| Производительность SELECT | Средняя | Высокое | Partition pruning, бенчмарки |
| Сложность транзакций | Высокая | Высокое | Запрет кросс-партиционных записей |
| Рост сложности кода | Средняя | Среднее | Code review, документация |

---

## Порядок реализации

**MVP (минимальная версия):**
1. Фаза 1 (Parser)
2. Фаза 2 (Schema)
3. Фаза 3 (Partition Manager)
4. Фаза 4 (INSERT)
5. Фаза 5 (SELECT) — базовая версия без pruning
6. Фаза 6 (SQL функции)
7. Фаза 12 (Базовые тесты)

**После MVP:**
- Фаза 5 (Partition pruning)
- Фазы 8-11 (DDL, индексы, транзакции)
- Фаза 12 (Полное тестирование)
- Фаза 13 (Документация)

---

---

## Приложение A: Множественные независимые инстансы

### Архитектура глобального состояния Turso

| Компонент | Тип | Разделяется между инстансами | Влияние |
|-----------|-----|------------------------------|---------|
| `DATABASE_MANAGER` | `HashMap<path, Weak<Database>>` | Да (registry) | Разные пути = разные инстансы |
| `EXTENSIONS` | `Vec<Library>` | Да | OK — расширения stateless |
| `VFS_MODULES` | `Vec<VfsMod>` | Да | OK — VFS это I/O backend |
| `VTAB_ID_COUNTER` | `AtomicU64` | Да | OK — только для уникальных ID |

**Вывод:** Каждый путь к main.db создаёт полностью независимый инстанс с собственным:
- Schema
- WAL
- Pager / Buffer Pool
- Attached databases (партиции)
- PartitionManager

### Пример использования для видеоаналитики

```rust
use std::path::Path;
use std::sync::Arc;
use turso_core::{Database, PlatformIO};
use turso_core::partition::{VideoAnalyticsPathResolver, PartitionConfig};

/// Создать независимую БД для плагина видеоаналитики
fn create_analytics_db(
    plugin_id: &str,
    data_dir: &Path,
) -> Result<Arc<Database>, Box<dyn std::error::Error>> {
    let io = Arc::new(PlatformIO::new()?);

    // Основная БД для справочников (уникальная для каждого плагина)
    let main_db_path = data_dir.join(format!("{}_main.db", plugin_id));
    let db = Database::open_file(io.clone(), main_db_path.to_str().unwrap())?;
    let conn = db.connect()?;

    // Создаём справочную таблицу (обычная, не партиционированная)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS known_plates (
            plate TEXT PRIMARY KEY,
            owner TEXT,
            registered_at INTEGER
        )",
        (),
    )?;

    // Создаём партиционированную таблицу для событий
    conn.execute(
        "CREATE TABLE IF NOT EXISTS events (
            plate TEXT,
            ts INTEGER,
            gate TEXT,
            confidence REAL
        ) PARTITION BY (ts)",
        (),
    )?;

    // Настраиваем path resolver для партиций
    let path_resolver = Box::new(VideoAnalyticsPathResolver {
        base_dir: data_dir.to_path_buf(),
        plugin_id: plugin_id.to_string(),
        interval_seconds: 86400, // сутки
    });

    conn.configure_partitioning("events", PartitionConfig {
        path_resolver,
        schema_sql: "CREATE TABLE events (plate TEXT, ts INTEGER, gate TEXT, confidence REAL)".into(),
    })?;

    // Discover существующих партиций
    conn.partition_manager().discover_partitions("events")?;

    Ok(db)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = Path::new("/var/lib/video-analytics");

    // Создаём независимые БД для каждого плагина
    let db_lpr = create_analytics_db("lpr", data_dir)?;      // License Plate Recognition
    let db_face = create_analytics_db("face", data_dir)?;    // Face Detection
    let db_motion = create_analytics_db("motion", data_dir)?; // Motion Detection

    // Работа с LPR
    let conn_lpr = db_lpr.connect()?;
    conn_lpr.execute(
        "INSERT INTO events (plate, ts, gate, confidence) VALUES (?, ?, ?, ?)",
        ("A001BC", 1737590400, "gate1", 0.95),
    )?;

    // Запрос по всем партициям (прозрачно)
    let plates = conn_lpr.query(
        "SELECT plate, ts FROM events WHERE ts >= ? ORDER BY ts",
        (1737504000,),
    )?;

    // Управление партициями
    let partitions = conn_lpr.partition_manager().list_partitions("events")?;
    for p in &partitions {
        println!("{}: {} - {} (attached: {})",
            p.file_path, p.range_start_iso, p.range_end_iso, p.attached);
    }

    // Отключить старую партицию
    conn_lpr.partition_manager().detach_partition(
        "events",
        Path::new("/var/lib/video-analytics/2026-01-20/lpr.bin"),
    )?;

    Ok(())
}
```

### Структура файлов

```
/var/lib/video-analytics/
├── lpr_main.db              # Справочники LPR
├── lpr_main.db-wal
├── face_main.db             # Справочники Face Detection
├── face_main.db-wal
├── motion_main.db           # Справочники Motion Detection
├── motion_main.db-wal
├── 2026-01-20/
│   ├── lpr.bin              # Партиция LPR за 20 января
│   ├── lpr.bin-wal
│   ├── face.bin             # Партиция Face за 20 января
│   └── motion.bin
├── 2026-01-21/
│   ├── lpr.bin
│   ├── face.bin
│   └── motion.bin
└── 2026-01-22/
    ├── lpr.bin              # Текущая партиция
    ├── face.bin
    └── motion.bin
```

---

## Приложение B: API управления партициями

### Rust API (программный)

```rust
impl Connection {
    /// Настроить партиционирование для таблицы
    pub fn configure_partitioning(
        &self,
        table: &str,
        config: PartitionConfig,
    ) -> Result<()>;

    /// Получить доступ к менеджеру партиций
    pub fn partition_manager(&self) -> &PartitionManager;
}

impl PartitionManager {
    /// Список партиций с метаданными
    pub fn list_partitions(&self, table: &str) -> Result<Vec<PartitionInfo>>;

    /// Подключить существующий файл партиции
    pub fn attach_partition(&mut self, table: &str, path: &Path) -> Result<PartitionInfo>;

    /// Отключить файл партиции
    pub fn detach_partition(&mut self, table: &str, path: &Path) -> Result<()>;

    /// Найти существующие партиции на диске
    pub fn discover_partitions(&mut self, table: &str) -> Result<Vec<PartitionInfo>>;

    /// Создать новую партицию для timestamp
    pub fn ensure_partition(&mut self, table: &str, timestamp: i64) -> Result<&PartitionFile>;
}
```

### SQL API (для отладки и администрирования)

**ВАЖНО:** SQL API отключается в runtime для production. Пользователи плагина не должны иметь доступ к информации о партициях.

```rust
/// Конфигурация партиционирования
pub struct PartitionConfig {
    pub path_resolver: Box<dyn PartitionPathResolver>,
    pub schema_sql: String,
    /// Отключить SQL функции управления партициями (по умолчанию: true)
    pub disable_sql_api: bool,
}

// При disable_sql_api = true:
// - partition_list() → Error: "Function not available"
// - partition_attach() → Error: "Function not available"
// - partition_detach() → Error: "Function not available"
// - partition_info() → Error: "Function not available"
```

**SQL функции (только если `disable_sql_api = false`):**

```sql
-- Список партиций
SELECT * FROM partition_list('events');

-- Подключить партицию
SELECT partition_attach('events', '/data/2026-01-19/lpr.bin');

-- Отключить партицию
SELECT partition_detach('events', '/data/2026-01-19/lpr.bin');

-- Информация о конфигурации
SELECT * FROM partition_info('events');
```

**Rust API всегда доступен** — управление партициями через код:

```rust
// Это работает всегда, независимо от disable_sql_api
conn.partition_manager().list_partitions("events")?;
conn.partition_manager().attach_partition("events", path)?;
conn.partition_manager().detach_partition("events", path)?;
```

---

## Чеклист синхронизации с upstream

При каждом merge с upstream:

- [ ] Проверить изменения в `parser/src/ast.rs`
- [ ] Проверить изменения в `parser/src/parser.rs`
- [ ] Проверить изменения в `core/schema.rs`
- [ ] Проверить изменения в `core/translate/insert.rs`
- [ ] Проверить изменения в `core/translate/select.rs`
- [ ] Запустить тесты партиционирования
- [ ] При конфликтах — временно отключить feature flag
