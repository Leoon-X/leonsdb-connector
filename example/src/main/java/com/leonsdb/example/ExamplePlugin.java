// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.example;

import com.leonsdb.connector.api.LeonsDB;
import com.leonsdb.connector.api.LeonsDBException;
import com.leonsdb.connector.api.Registration;
import com.leonsdb.connector.api.TransactionAbortedException;
import java.time.Instant;
import java.util.concurrent.CompletionException;
import net.kyori.adventure.text.Component;
import org.bukkit.Bukkit;
import org.bukkit.OfflinePlayer;
import org.bukkit.command.Command;
import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.event.player.PlayerQuitEvent;
import org.bukkit.plugin.java.JavaPlugin;

/**
 * A small plugin that stores player stats, money, an inventory stash and
 * clan membership in LeonsDB. It shows every part of the API a typical
 * plugin needs.
 */
public final class ExamplePlugin extends JavaPlugin implements Listener {

    private Registration handover;

    @Override
    public void onEnable() {
        // Declare the data classes once. If LeonsDB is unreachable or a
        // field type conflicts with stored data, this throws and the plugin
        // is disabled instead of running without its data.
        LeonsDB.register(Stats.class, Clan.class);
        getServer().getPluginManager().registerEvents(this, this);
        // Network chat: every server of the network gets every message.
        LeonsDB.channel("example.chat").subscribe(msg ->
                Bukkit.broadcast(Component.text("[" + msg.sender() + "] " + msg.text())));
        // On a server switch, right before the player's data goes to the new
        // server: what is changed here is there when the player joins it.
        handover = LeonsDB.onHandover(uuid -> {
            Player player = Bukkit.getPlayer(uuid);
            if (player != null) {
                stats(player).lastLocation = player.getLocation();
            }
        });
    }

    @Override
    public void onDisable() {
        if (handover != null) {
            handover.close();
        }
    }

    private static Stats stats(Player player) {
        // Instant: the data was loaded before the player joined.
        return LeonsDB.player(player.getUniqueId()).get(Stats.class);
    }

    @EventHandler
    public void onJoin(PlayerJoinEvent event) {
        Stats stats = stats(event.getPlayer());
        stats.name = event.getPlayer().getName();
        if (stats.firstJoin == null) {
            stats.firstJoin = Instant.now(); // saved automatically
        }
    }

    @EventHandler
    public void onQuit(PlayerQuitEvent event) {
        // Saved on a server switch too: the change follows the player and
        // reaches their new server a moment after they joined it.
        Stats stats = stats(event.getPlayer());
        stats.lastLocation = event.getPlayer().getLocation();
        stats.quits++;
    }

    @Override
    public boolean onCommand(CommandSender sender, Command command, String label, String[] args) {
        if (!(sender instanceof Player player)) {
            sender.sendMessage("Players only.");
            return true;
        }
        Stats stats = stats(player);
        switch (command.getName()) {
            case "stats" -> player.sendMessage("kills=" + stats.kills + " deaths=" + stats.deaths
                    + " coins=" + stats.coins + " quits=" + stats.quits + " firstJoin=" + stats.firstJoin
                    + " stash=" + (stats.stash == null ? "empty" : stats.stash.length + " slots"));

            case "kill" -> {
                int n = args.length > 0 ? Integer.parseInt(args[0]) : 1;
                for (int i = 0; i < n; i++) {
                    stats.kills++; // merged and saved within a second
                    stats.streakThisSession++;
                }
                player.sendMessage("kills=" + stats.kills);
            }

            case "pay" -> {
                long amount = Long.parseLong(args[0]);
                // Money: saved right away; we only confirm once it is stored.
                LeonsDB.player(player.getUniqueId())
                        .update(Stats.class, s -> s.coins += amount)
                        // Back on the player's thread (Folia: their region's) to read their data.
                        .whenComplete((ok, err) -> player.getScheduler().run(this, t -> player.sendMessage(
                                err == null ? "coins=" + stats.coins + " (saved)" : "Not confirmed yet: " + err.getMessage()), null));
            }

            case "send" -> {
                // Coins move between two players atomically: both change, or
                // neither does. The other player may be online here or offline.
                OfflinePlayer target = Bukkit.getOfflinePlayerIfCached(args[0]);
                long amount = Long.parseLong(args[1]);
                if (target == null || amount <= 0) {
                    player.sendMessage("Usage: /send <player who joined before> <amount>");
                    return true;
                }
                LeonsDB.transaction()
                        .update(player.getUniqueId(), Stats.class, s -> {
                            if (s.coins < amount) {
                                throw new TransactionAbortedException("You only have " + s.coins + " coins");
                            }
                            s.coins -= amount;
                        })
                        .update(target.getUniqueId(), Stats.class, s -> s.coins += amount)
                        .commit()
                        // Completes with the cached data updated; read it on the
                        // player's thread (Folia: their region's).
                        .whenComplete((ok, err) -> player.getScheduler().run(this, t -> {
                            Throwable cause = err instanceof CompletionException && err.getCause() != null ? err.getCause() : err;
                            if (cause == null) {
                                player.sendMessage("Sent " + amount + " coins to " + target.getName() + "; you have " + stats(player).coins);
                            } else if (cause instanceof LeonsDBException e && "OWNER_UNAVAILABLE".equals(e.reason())) {
                                player.sendMessage(target.getName() + "'s server is not reachable right now; nothing was sent.");
                            } else {
                                player.sendMessage("Not sent: " + cause.getMessage());
                            }
                        }, null));
            }

            case "shout" -> LeonsDB.channel("example.chat").publish(player.getName() + ": " + String.join(" ", args));

            case "stash" -> {
                if (args.length > 0 && args[0].equals("restore")) {
                    if (stats.stash != null) {
                        player.getInventory().setContents(stats.stash);
                    }
                    player.sendMessage("Stash restored.");
                } else {
                    stats.stash = player.getInventory().getContents();
                    player.sendMessage("Stash saved.");
                }
            }

            case "clan" -> {
                String name = args[0].toLowerCase();
                LeonsDB.global(Clan.class).update(name, clan -> {
                    if (clan.leader == null) {
                        clan.leader = player.getUniqueId();
                    }
                    if (!clan.members.contains(player.getUniqueId())) {
                        clan.members.add(player.getUniqueId());
                    }
                }).whenComplete((clan, err) -> player.getScheduler().run(this, t -> player.sendMessage(
                        err == null ? "Clan " + name + " now has " + clan.members.size() + " member(s)"
                                : "Could not join: " + err.getMessage()), null));
            }

            case "top" -> LeonsDB.leaderboard(Stats.class, "kills").top(5).whenComplete((top, err) ->
                    player.getScheduler().run(this, t -> {
                        if (err != null) {
                            player.sendMessage("Leaderboard unavailable: " + err.getMessage());
                            return;
                        }
                        player.sendMessage("Top kills:");
                        for (var entry : top) {
                            player.sendMessage("#" + entry.rank() + " " + entry.data().name + " - " + entry.value());
                        }
                    }, null));

            case "rank" -> {
                // Save first so the rank includes this player's latest kills.
                var handle = LeonsDB.player(player.getUniqueId());
                handle.save()
                        .thenCompose(ok -> LeonsDB.leaderboard(Stats.class, "kills").rankOf(player.getUniqueId()))
                        .whenComplete((rank, err) -> player.getScheduler().run(this, t -> player.sendMessage(
                                err != null ? "Rank unavailable: " + err.getMessage()
                                        : rank.isPresent() ? "Your rank: #" + rank.getAsLong() : "You are not ranked yet"), null));
            }

            case "history" -> {
                // The last changes of a player's stats, online or offline.
                OfflinePlayer target = args.length > 0 ? Bukkit.getOfflinePlayerIfCached(args[0]) : player;
                if (target == null) {
                    player.sendMessage("Usage: /history <player who joined before>");
                    return true;
                }
                LeonsDB.history().changes(target.getUniqueId(), Stats.class, 5).whenComplete((changes, err) -> {
                    if (err != null) {
                        player.sendMessage("History unavailable: " + err.getMessage());
                        return;
                    }
                    for (var c : changes) {
                        player.sendMessage(c.firstTime() + " by " + c.by() + ": " + c.fields());
                    }
                });
            }

            case "rollback" -> {
                // Turn a player's data back by some minutes, after a dupe or a
                // scam. "/rollback Bob 10" shows what would change, adding
                // "confirm" does it. Works wherever Bob plays right now.
                OfflinePlayer target = args.length > 1 ? Bukkit.getOfflinePlayerIfCached(args[0]) : null;
                if (target == null) {
                    player.sendMessage("Usage: /rollback <player> <minutes> [confirm]");
                    return true;
                }
                Instant to = Instant.now().minusSeconds(60L * Long.parseLong(args[1]));
                var rollback = LeonsDB.history().rollbackPlayer(target.getUniqueId(), to);
                boolean confirm = args.length > 2 && args[2].equals("confirm");
                (confirm ? rollback.apply() : rollback.preview()).whenComplete((result, err) -> {
                    if (err != null) {
                        player.sendMessage("Rollback failed: " + err.getMessage());
                        return;
                    }
                    for (var r : result.records()) {
                        player.sendMessage(r.collection() + ": " + String.join(", ", r.fields()) + (r.error() == null ? "" : " FAILED: " + r.error()));
                    }
                    player.sendMessage(confirm
                            ? "Rolled back " + result.recordsChanged() + " records; undone changes by " + result.undoneBy()
                            : result.recordsChanged() + " records would change. Add \"confirm\" to do it.");
                });
            }

            default -> {
                return false;
            }
        }
        return true;
    }
}
