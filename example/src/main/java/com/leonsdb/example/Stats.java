// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.example;

import com.leonsdb.connector.api.Indexed;
import com.leonsdb.connector.api.PlayerData;
import com.leonsdb.connector.api.Transactional;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.bukkit.Location;
import org.bukkit.inventory.ItemStack;

/**
 * One record per player. Every non-transient field is stored; add new
 * fields at any time and existing players simply get the default value.
 */
@PlayerData(namespace = "example", collection = "stats")
public class Stats {

    /** Kept in a sorted index for leaderboards. */
    @Indexed public int kills;

    public int deaths;

    /** The player's current name, kept up to date on join, so leaderboards
     * can show names on every server of a network. */
    public String name;

    /** Money: changed with update(), which saves immediately. */
    @Transactional public long coins = 100;

    public Instant firstJoin;

    public Location lastLocation;

    public ItemStack[] stash;

    public List<String> titles = new ArrayList<>();

    /** Not stored: only lives in memory. */
    public transient int streakThisSession;
}
