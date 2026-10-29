# Using LeonsDB in your plugin

This guide shows how a Paper plugin stores its data in LeonsDB. A complete
working example is in `connector/example` in the repository.

## Setup

1. Install the **LeonsDB** plugin on each Minecraft server, put a server
   token in `plugins/LeonsDB/config.yml` and give each server its own
   `server-name` (see [Running the server](running-the-server.md)).
2. In your plugin, compile against the LeonsDB API only (the `leonsdb-api`
   jar). Do not shade it: the LeonsDB plugin provides it at runtime.
3. Add LeonsDB as a dependency in your `plugin.yml`:

   ```yaml
   depend: [LeonsDB]
   ```

## Declare your data

```java
@PlayerData(namespace = "practicepvp", collection = "stats")
public class Stats {
    @Indexed public int kills;
    public int deaths;
    @Transactional public long coins = 100;   // default for new players
    public Location lastLocation;
    public ItemStack[] kit;
    public transient int streak;              // transient: not stored
}
```

- **Namespace**: your plugin's name. **Collection**: the kind of data.
  Both use lowercase letters, digits and underscores.
- Every field that is not `static` or `transient` is stored under its
  name.
- The class needs a constructor without parameters. Field initializers are
  the defaults for new players.

Register your classes once, in `onEnable`:

```java
@Override
public void onEnable() {
    LeonsDB.register(Stats.class, Clan.class);
}
```

If LeonsDB is unreachable, or a field's type conflicts with stored data,
`register` throws. Let it: running without your data would be worse.

## Player data

A player's data is loaded **before** they join, and kept in memory while
they are online. Reading it never touches the network:

```java
Stats stats = LeonsDB.player(player.getUniqueId()).get(Stats.class);
stats.kills++;
```

That is all. LeonsDB notices the change and saves it in the background,
about once per second, when the player leaves and when the server stops.
Forty kills between two saves become one "kills +40" write.

Use `get` and change fields on the **main thread**. Keep the reference only
while the player is online.

On **Folia** there is no single main thread: use `get`, `update` and `save`
on the thread of the region the player is in (in their own events, or in a
task from `player.getScheduler()`). LeonsDB saves each player on their
region's thread too. Futures and listeners that are not about one player
(transactions, messages, history) complete on the global region thread, as
on Paper they complete on the main thread.

### Money and other changes that must never be lost

Use `update`. The change is applied immediately and saved right away; the
future completes once LeonsDB has stored it durably:

```java
LeonsDB.player(player.getUniqueId())
       .update(Stats.class, s -> s.coins -= price)
       .whenComplete((ok, err) -> {
           // Back to the main thread before touching Bukkit.
           Bukkit.getScheduler().runTask(plugin, () -> {
               if (err == null) giveItem(player);
               else player.sendMessage("Purchase not confirmed yet, try again.");
           });
       });
```

If the future fails, the change is **not lost**: it stays in memory and
LeonsDB keeps retrying in the background. It only means it is not
confirmed yet.

To save everything of a player immediately (for example before
transferring them to another server), call `save()`; it returns a future
too.

### Server switches and quit events

On a network, a player's data goes to the new server **before** the old
server's `PlayerQuitEvent` fires: the proxy only lets go of the player once
the new server has them, and the new server only lets them in with their
data. Changes made in the quit event are still kept. Until the player
leaves the old server, their data stays usable there, and whatever changes
in it (in the quit event or just before) follows them to the new server:
numbers by their difference, so changes on both servers count, other
fields by their new value. It is applied exactly once, also if LeonsDB is
briefly unreachable. `update` and `save` there complete once it is stored.

**The trade-off:** such a change reaches the new server **a moment after
the player joined it**: once the old server's quit event has run, which
the proxy triggers after the join. The new server's join event still sees
the value from before. If something must be there from the first moment, for example a
location that the next server's join event reads, set it in a handover
listener instead. It runs on the player's thread right before their data is
handed over, so what it changes is part of what the new server loads:

```java
private Registration handover;

@Override
public void onEnable() {
    handover = LeonsDB.onHandover(uuid -> {
        Player player = Bukkit.getPlayer(uuid);
        if (player != null) {
            LeonsDB.player(uuid).get(Stats.class).lastLocation = player.getLocation();
        }
    });
}

@Override
public void onDisable() {
    handover.close();
}
```

The listener only runs for server switches; when a player simply leaves,
the quit event is the place, and it is saved before anything else can load
the player.

## Global data

Data that does not belong to one player (clans, bans, settings) lives in
global collections, keyed by any string:

```java
@GlobalData(namespace = "practicepvp", collection = "clans")
public class Clan {
    public UUID leader;
    public List<UUID> members = new ArrayList<>();
}

LeonsDB.global(Clan.class).update("red", clan -> clan.members.add(uuid))
       .thenAccept(clan -> ...);
```

Several servers may change the same record at the same time. `update`
re-reads the latest version and re-applies your change when that happens,
so **no change is ever lost**, but your change function may run more than
once: it must only modify the object it is given. Global operations are
asynchronous; never wait for them on the main thread.

## Transactions

When several records must change together (a payment between two
players, a trade, a clan paying out from its bank), use a transaction:
every change is stored, or none is.

```java
LeonsDB.transaction()
       .update(sender.getUniqueId(), Stats.class, s -> {
           if (s.coins < amount) throw new TransactionAbortedException("Not enough coins");
           s.coins -= amount;
       })
       .update(target.getUniqueId(), Stats.class, s -> s.coins += amount)
       .update(Clan.class, "red", clan -> clan.bank += fee)
       .commit()
       .whenComplete((ok, err) -> {
           // Runs on the main thread, with the cached data already updated.
           if (err == null) sender.sendMessage("Sent!");
           else sender.sendMessage("Not sent: " + err.getMessage());
       });
```

What you can rely on:

- **All or nothing.** Even if the Minecraft server crashes, LeonsDB is
  restarted or the network drops in the middle, a transaction is never
  stored in part.
- **Never twice.** If the answer is lost, the connector sends the same
  transaction again and LeonsDB recognises it.
- **No double spending.** Two transactions on the same online player run
  one after the other, so the second one sees the coins the first removed.
  Throw `TransactionAbortedException` to cancel; your message is what the
  future fails with.
- **Nothing visible until stored.** The cached objects of online players
  only change once the transaction is durably stored, just before the
  future completes. Changes the game made meanwhile (a kill, coins from a
  quest) are kept: numbers get the transaction's difference added.
- **Unsaved changes come along.** Changes to an online player that were
  not saved yet are stored in the same atomic write.

### Players on other servers

A transaction can include players wherever they are: on this server,
offline, or **online on another server of the network**. For a player on
another server, that server takes part. It reserves the player's data,
applies your change to their live data (so its own unsaved changes count
too) and the whole transaction is stored at once, or not at all. That
server's cached data is updated right after.

`/pay` from the lobby to someone playing survival is just:

```java
LeonsDB.transaction()
       .update(sender.getUniqueId(), Stats.class, s -> s.coins -= amount)
       .update(target.getUniqueId(), Stats.class, s -> s.coins += amount)
       .commit();
```

One thing differs: for a player on another server, your change function
runs on their **last saved** data (at most about a second old), and what
gets applied is the difference it made ("coins +100"). So adding to or
taking from numbers is always exact, but a check like
`if (s.coins < price)` on a player online elsewhere can see a slightly
old value. Make such checks on the player who is on your server, as in
the example.

If the other server cannot be reached, or does not answer within two
seconds, nothing is changed and the future fails with reason
`OWNER_UNAVAILABLE`:

```java
if (err instanceof LeonsDBException e && "OWNER_UNAVAILABLE".equals(e.reason())) {
    sender.sendMessage(target.getName() + "'s server is not reachable right now; nothing was sent.");
}
```

When two servers run transactions on each other's players at the same
moment, one of them waits briefly and tries again; you do not need to
handle that.

### Good to know

Like global updates, the change functions run on a background thread on
copies of the data, and may run more than once. Only modify the object
you are given, and do not call the Bukkit API inside them. A transaction
can change up to 100 records.

If LeonsDB cannot be reached for 10 seconds, the future fails with reason
`NOT_CONFIRMED`: the outcome is not known yet. The connector keeps trying
in the background until the transaction is either stored (once) or
refused, and logs which.

## Messages between servers

Channels carry messages to every server of the network: network chat,
private messages across servers, "the event starts now".

```java
Channel chat = LeonsDB.channel("network.chat");

// In onEnable: show every message, from any server.
chat.subscribe(msg -> Bukkit.broadcast(Component.text("[" + msg.sender() + "] " + msg.text())));

// When a player talks:
chat.publish(player.getName() + ": " + text);
```

- The listener runs on the main thread. Every subscriber gets the message,
  including the server that sent it; `msg.sender()` is the sending
  server's name.
- Messages are live only. A server that is restarting or cut off misses
  what was sent meanwhile. Keep anything that must not be lost in your
  data classes.
- A message is text up to 64 KiB. Send JSON for structured messages.
- Websites and Discord bots can use the same channels; see the HTTP API.

## History and rollbacks

LeonsDB keeps the past of your data. Staff tools can look at it and turn
it back, for online and offline players alike:

```java
History history = LeonsDB.history();
Instant tenMinutesAgo = Instant.now().minus(Duration.ofMinutes(10));

// Bob's stats as they were ten minutes ago
history.player(bob, Stats.class, tenMinutesAgo).thenAccept(old ->
        old.ifPresent(s -> staff.sendMessage("Bob had " + s.coins + " coins")));

// What changed them, newest first
history.changes(bob, Stats.class, 10).thenAccept(changes -> {
    for (History.Change c : changes) {
        staff.sendMessage(c.firstTime() + " by " + c.by() + ": " + c.fields());
    }
});
```

`c.fields()` maps each changed field to its value before and after, in your
class's own types (an `int`, an `ItemStack[]`, ...).

Rollbacks are previewed, then applied:

```java
Rollback rollback = history.rollbackPlayer(bob, tenMinutesAgo);
rollback.preview().thenAccept(r ->
        staff.sendMessage(r.recordsChanged() + " records would change, undoing changes by " + r.undoneBy()));
rollback.apply().thenAccept(r -> staff.sendMessage("Rolled back " + r.recordsChanged() + " records"));

// Only one class, only some fields: the kit back, the coins earned since kept
history.rollback(bob, Kit.class, tenMinutesAgo).onlyFields("items").apply();
```

- `rollbackPlayer` covers the player's data of every plugin; `rollback`
  one class (or one global record, by id).
- If the player is online, on this server or any other, their server
  applies the rollback to the live data right away; your cached object
  changes like after a transaction.
- A rollback undoes every change made since that time, legitimate ones
  too. Show the preview first.
- Writes within one minute are kept as one step: a time inside a minute
  gives the state from before that minute. Changes to `@Transactional`
  fields are kept one by one.
- The futures complete on the main thread. Rollbacks need a server token
  (what the LeonsDB plugin uses); how far back you can go is set on the
  LeonsDB server (7 days in full detail by default).

## Supported types

| Java type | Notes |
|-----------|-------|
| `int`, `long`, `double`, `boolean` and their boxed forms | Whole numbers are saved as increments, so merged changes are exact. |
| `String`, `UUID`, `Instant`, `byte[]` | |
| Enums | Stored by name. |
| `List<T>`, `Map<String, T>` | Of any supported type, nested as you like. Elements must not be null. |
| `ItemStack`, `ItemStack[]`, `Location` | Items use Paper's own format and are upgraded automatically when Minecraft updates. |

`float`, `short`, `byte` and `char` are not supported; use `double`, `int`
or `String`. A null field is stored as "no value".

## Changing your data classes

- **Add a field** at any time. Existing players get the field's default.
- **Remove a field** at any time. Its stored data is kept, so adding it back
  later restores it.
- **Changing a field's type is refused** by the server (`SCHEMA_CONFLICT`),
  because another version of your plugin could still be writing the old
  type. Use a new field name instead.
- If stored data contains a value your plugin version cannot read (for
  example an enum constant that a newer version added), that field keeps
  its default in memory and is **never written back**, so the stored value
  stays intact.

## What happens when things go wrong

| Situation | What LeonsDB does |
|-----------|-------------------|
| LeonsDB is unreachable when a player joins | The player is asked to try again. They never play with empty data, which would later overwrite their real data. |
| LeonsDB goes down while players are online | Players keep playing. Changes stay in memory and are saved as soon as LeonsDB is back. |
| A player rejoins before their data finished saving | The join waits for the save (and for the hand-back of ownership), so it never loads outdated data. |
| A player switches servers | The old server saves the player's latest data and hands them over; the new server loads it. It takes milliseconds: a Paper server saves as soon as its main thread is idle, without waiting for its next tick (it logs this at startup; start it with `-Dleonsdb.idleQueue=false` to wait for the tick instead). Folia saves at the player's next region tick. |
| A player's server switch fails (the new server refuses them or never lets them in) | They stay where they are, and get their data back as soon as the new server lets go, usually within milliseconds. Your plugin's data objects stay the same objects, and nothing changed meanwhile is lost. |
| A player is logged in on two servers at once | Only one server owns their data. The other one hands over and, if the player is still there 35 seconds later (`handover-grace-ms`), disconnects them, so nothing can be duplicated. |
| A Minecraft server crashes with players online | Their ownership expires after about 10 seconds; then they can join another server with their last saved data. |
| The Minecraft server stops | Everything is saved first. If LeonsDB cannot be reached within 10 seconds, unsaved changes are written to `plugins/LeonsDB/recovery/`. |
| The Minecraft server crashes | Changes from the last moment (up to about one second) may be lost. Changes made with `update` were already confirmed. |
| A save request is answered but the answer is lost | LeonsDB detects this and never applies the change twice. |

## Leaderboards

Mark a numeric field `@Indexed` and LeonsDB keeps it sorted on every save:

```java
Leaderboard<Stats> kills = LeonsDB.leaderboard(Stats.class, "kills");

kills.top(10).thenAccept(top -> {
    for (Ranking<Stats> r : top) {
        // r.rank(), r.player(), r.value() and the full record in r.data()
    }
});

kills.rankOf(player.getUniqueId()).thenAccept(rank -> ...); // OptionalLong
kills.page(10, 10);                                          // ranks 11-20

// Lowest first, for things like fastest times:
LeonsDB.leaderboard(Stats.class, "bestTime", Leaderboard.Order.LOWEST_FIRST);
```

- Equal values share a rank (1, 2, 2, 4). Players without a value are not ranked.
- A player's changes reach the leaderboard when they are saved (within about a
  second). To show someone their up-to-date rank, save first:
  `handle.save().thenCompose(ok -> kills.rankOf(uuid))`.
- **Store the player's name in your data** (update it on join) and show
  `r.data().name`. Bukkit only knows the names of players who have been on
  that same server, which is not enough on a network.
- Adding `@Indexed` to a field that already has data builds the index in the
  background; leaderboard requests fail until it is ready, rather than
  returning an incomplete ranking.
- Each indexed field uses about 80 bytes of server memory per ranked record
  (8 MB per 100,000 players).

## Rules of thumb

- Call `get`, `update` and `save` on the main thread (on Folia: the thread
  of the player's region).
- Never `join()` or `get()` a LeonsDB future on the main thread.
- Use `long` for money (in cents if you need fractions). `double` works, but
  floating-point rounding is not what you want for currency.
