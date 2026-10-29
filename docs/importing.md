# Importing existing data

`leonsdb-import.jar` moves a network's existing player and plugin data into
LeonsDB: from MySQL (and MariaDB), MongoDB, SQLite, H2, and YAML files such as
EssentialsX's userdata. You describe the import in a small plan file; the
importer checks everything first and only writes when you ask it to.

```
java -jar leonsdb-import.jar plan.yml             # dry run: checks everything, writes nothing
java -jar leonsdb-import.jar plan.yml --apply     # imports
```

It needs Java 21 or newer, and runs on any machine that can reach both the
source and LeonsDB.

## How an import works

1. **Install your plugin with LeonsDB and start the server once.** The plugin
   registers its data classes, which creates the collection and its fields in
   LeonsDB. The importer converts every value to the type your plugin
   declared, so the plugin's code is the single source of truth. If the
   collection does not exist yet, the importer says: start the plugin once
   with LeonsDB, then run the import again.
2. **Stop the Minecraft servers** (or keep the players off). An import refuses
   to store data of players who are online, so it can never overwrite what
   a server is changing.
3. **Write a plan** (see the examples below) and run the **dry run**. It reads
   the whole source, converts every row, and reports what would be imported:
   how many records, which rows would be skipped and why (with where they
   are), how often each field has a value, the smallest and largest numbers
   and times, and the first records exactly as they would be stored. Nothing
   is written. The report is also saved next to the plan (`plan-report.txt`).
4. **Fix what the report shows**, in the plan or in the source, until it says
   *ready*.
5. **Run it with `--apply`.** The importer checks the source again, then loads
   each collection with one bulk import: all of it is stored, or none of it.
   If any row would now be skipped, nothing is imported unless you add
   `--allow-skips`.

### What is safe

- **The source is only read.** SQL sources get a single `SELECT` (the plan
  refuses anything else), over a read-only connection that is never
  committed. MongoDB is only queried. YAML files are only read, and YAML tags
  that would create Java objects are refused.
- **Nothing in LeonsDB is overwritten.** A bulk import only loads an empty
  collection. Importing into a collection that already has records is
  refused.
- **All or nothing.** If anything goes wrong while importing (the source goes
  away, LeonsDB refuses a record, a player comes online), nothing of that
  collection is stored, and you can simply run it again.
- **Values are never guessed.** `12.5` is not imported into an `int` field,
  and a number only becomes a time when the plan says whether it counts
  seconds or milliseconds. Rows that do not fit are skipped and reported,
  never changed.
- **Large sources fit in memory.** Rows are read and sent in batches.
- **Secrets stay out of files.** Passwords and the LeonsDB token come from
  environment variables, and are never printed or written to the report.

## The plan

```yaml
source:
  type: mysql                          # mysql, mongodb, sqlite, h2 or yaml
  url: jdbc:mysql://127.0.0.1:3306/survival
  user: importer
  password-env: MYSQL_PASSWORD         # the environment variable with the password
leonsdb:
  server: 127.0.0.1:7420
  token-env: LEONSDB_TOKEN             # a token that may write the collections (the admin token works)
imports:
  - from: { table: player_stats }      # what to read
    into: myplugin.stats               # namespace.collection, as your plugin declares it
    key: uuid                          # where each record's key comes from
    fields:                            # field in LeonsDB: where its value comes from
      kills: kills
      coins: { from: balance }
      lastSeen: { from: last_seen, as: epoch-seconds }
```

One plan can have several imports, each into its own collection; each one
is stored all or nothing on its own.

### `source`

| Type | `url` | Other settings |
|------|-------|----------------|
| `mysql` | `jdbc:mysql://host:3306/database` (MariaDB too) | `user`, `password-env`, `driver` |
| `mongodb` | `mongodb://host:27017` (or `mongodb+srv://...`) | `database` (or in the URL), `user`, `password-env` |
| `sqlite` | `jdbc:sqlite:path/to/file.db` | |
| `h2` | `jdbc:h2:path/to/file` (without `.mv.db`) | `user`, `password-env`, `driver` |
| `yaml` | none: `dir: path/to/folder` instead | |

Paths are relative to the plan file.

**Drivers for MySQL and H2.** Their licenses do not allow LeonsDB to ship
them inside the importer, so when a plan needs one, the importer downloads it
once from Maven Central (MySQL Connector/J 26.7.0, H2 2.5.250) into
`~/.leonsdb/import-drivers`, and only uses it if it is exactly the file the
importer expects (its SHA-256 is fixed in the importer). Without internet
access, download the jar yourself and set `driver: path/to/the.jar`.

### `from`

| Source | Settings |
|--------|----------|
| SQL | `table: name`, or `query: "SELECT ..."` (one `SELECT` only) |
| MongoDB | `collection: name`, and optionally `filter: { field: value }` |
| YAML, one file per record | `files: "userdata/*.yml"` |
| YAML, many records in one file | `file: data.yml`, and optionally `section: players` (the map whose entries are the records) |

### `key`

The column or field holding each record's key. For player collections that
is the player's UUID, in any common form: `0f3a2c1e-6b7d-4e8f-9a0b-1c2d3e4f5a6b`,
the same without dashes, 16 bytes (`BINARY(16)`), or a UUID value.

- `key: { from: name, as: offline-uuid }` makes the UUID an **offline-mode**
  server gives a player name. (Online-mode UUIDs cannot be worked out from
  names without asking Mojang; the importer does not.)
- `$file` is a YAML file's name without `.yml` (EssentialsX names its files
  after the UUID); `$key` is the entry's key in a `section`.
- Global (non-player) collections take a text key: any column, `as: id`.

A player (or id) that appears twice is imported once, from the first row;
the others are reported as skipped.

### `fields`

`field: column` takes the value as it is. For nested documents (MongoDB,
YAML) use a dotted path: `stats.kills`, `homes.home.world`. A row without a
value for a field leaves that field at its default.

| Your plugin's field | Takes |
|---------------------|-------|
| `int`, `long` | whole numbers, also as text (`"42"`) or `42.0`; never numbers with decimals or out of range |
| `double` | any finite number, also as text |
| `boolean` | `true`/`false`, `1`/`0`, `yes`/`no`, `on`/`off` |
| `String` | text; numbers and booleans become text |
| `UUID` | as for keys |
| `Instant` (times) | SQL `TIMESTAMP`/`DATETIME`/`DATE`, MongoDB dates, ISO 8601 text (`2026-10-29T12:00:00Z`; without a zone it is taken as UTC). Numbers only with `as: epoch-seconds` or `as: epoch-millis` |
| `byte[]` | binary columns; text with `as: base64` or `as: hex` |
| `List`, `Map` | lists and maps; their elements keep their own types |

Items, inventories and locations cannot be imported: plugins store them in
formats of their own. Leave such fields out and let the plugin fill them.

MongoDB UUIDs written by the old Java driver (`subtype 3`) need
`as: java-legacy-uuid`.

## Examples

Copy one, adjust the names to your plugin's fields, and run the dry run.

### EssentialsX userdata (YAML, one file per player)

EssentialsX keeps `plugins/Essentials/userdata/<uuid>.yml`. Into a plugin
whose data class has `long coins`, `String name` and `Instant lastSeen`:

```yaml
source:
  type: yaml
  dir: plugins/Essentials
leonsdb:
  server: 127.0.0.1:7420
  token-env: LEONSDB_TOKEN
imports:
  - from: { files: "userdata/*.yml" }
    into: economy.wallets
    key: $file
    fields:
      coins: money                      # "1250.0" -> 1250; "1250.5" is skipped (decimals)
      name: last-account-name
      lastSeen: { from: timestamps.logout, as: epoch-millis }
```

If balances have cents, declare the field as `double` in your plugin, or keep
cents in a `long` and convert the source first.

### A typical MySQL stats table

```sql
CREATE TABLE player_stats (uuid CHAR(36) PRIMARY KEY, name VARCHAR(16),
  kills INT, deaths INT, playtime_seconds BIGINT, last_join DATETIME);
```

```yaml
source:
  type: mysql
  url: jdbc:mysql://127.0.0.1:3306/minecraft
  user: importer                        # a user that may only SELECT is enough
  password-env: MYSQL_PASSWORD
leonsdb:
  server: 127.0.0.1:7420
  token-env: LEONSDB_TOKEN
imports:
  - from: { table: player_stats }
    into: pvp.stats
    key: uuid
    fields:
      name: name
      kills: kills
      deaths: deaths
      playtime: playtime_seconds
      lastJoin: last_join
```

A query works too, for example to leave out banned players or join tables:
`from: { query: "SELECT s.*, b.balance FROM player_stats s JOIN balances b ON b.uuid = s.uuid" }`.

### A typical MongoDB player collection

```js
{ _id: ObjectId(...), uuid: "0f3a2c1e-...", name: "Steve",
  stats: { kills: 12, deaths: 3 }, lastLogin: ISODate("2026-10-29T12:00:00Z"),
  titles: ["Newbie", "Builder"] }
```

```yaml
source:
  type: mongodb
  url: mongodb://127.0.0.1:27017
  database: minecraft
  user: importer
  password-env: MONGO_PASSWORD
leonsdb:
  server: 127.0.0.1:7420
  token-env: LEONSDB_TOKEN
imports:
  - from: { collection: players }
    into: pvp.stats
    key: uuid
    fields:
      name: name
      kills: stats.kills
      deaths: stats.deaths
      lastJoin: lastLogin
      titles: titles
```

### SQLite and H2 files

Plugins that keep a local database file work the same way; stop the server
first, as H2 files cannot be read while a server has them open.

```yaml
source:
  type: sqlite
  url: jdbc:sqlite:plugins/MyPlugin/data.db
# or
source:
  type: h2
  url: jdbc:h2:plugins/MyPlugin/data        # the file is data.mv.db
```

### A YAML file with all players in it

```yaml
# plugins/Levels/players.yml:   players: { Steve: { level: 12 }, Alex: { level: 30 } }
source: { type: yaml, dir: plugins/Levels }
leonsdb: { server: 127.0.0.1:7420, token-env: LEONSDB_TOKEN }
imports:
  - from: { file: players.yml, section: players }
    into: levels.players
    key: { from: $key, as: offline-uuid }   # names on an offline-mode server
    fields: { level: level }
```

## Reading the report

```
[1] table player_stats -> pvp.stats (player collection)
    rows read:     12,345
    would import:  12,340 records
    skipped:       5
      3 x "abc" is not a UUID (row 17, row 99, row 1201)
      2 x the same player twice (0f3a2c1e-...); the first is kept (row 50, row 51)
    fields:
      kills            int       from kills            12,340 rows (100%)   min 0 max 98,123
      lastJoin         timestamp from last_join        9,000 rows (73%)   2019-01-02T10:00:00Z to 2026-10-28T21:14:03Z
    first records:
      0f3a2c1e-6b7d-4e8f-9a0b-1c2d3e4f5a6b: kills=12, lastJoin=2026-10-28T21:14:03Z, name="Steve"
```

- **min/max** catch mistakes quickly: negative kills, times in 1970 (seconds
  read as milliseconds) or in the far future (the other way around).
- A field at **0%** usually means its `from` names the wrong column or path.
- Exit code 0: ready (dry run) or imported (`--apply`); 1: problems, nothing
  imported; 2: wrong command line.
