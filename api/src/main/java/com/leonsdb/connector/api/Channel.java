// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.api;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * A message channel shared by every server of the network, and by
 * websites and bots with access to it: network chat, private messages,
 * events.
 *
 * <pre>{@code
 * Channel chat = LeonsDB.channel("network.chat");
 * chat.subscribe(msg -> Bukkit.broadcast(Component.text(msg.text())));
 * chat.publish(player.getName() + ": " + text);
 * }</pre>
 *
 * <p>Messages are live only: they reach whoever is subscribed at that
 * moment and are not stored. Servers that are restarting or cut off miss
 * them. Keep data in records, not in messages.
 */
public interface Channel {

    /**
     * The channel's name.
     *
     * @return {@code <namespace>.<name>}
     */
    String name();

    /**
     * Sends a message to everyone subscribed, this server included. Never
     * blocks.
     *
     * @param text at most 64 KiB; send JSON for structured messages
     * @return completes with how many subscribers received it
     */
    CompletableFuture<Integer> publish(String text);

    /**
     * Receives the channel's messages until the subscription is closed.
     * The listener runs on the main thread. If the connection to LeonsDB
     * drops, it is re-established automatically; messages sent meanwhile
     * are missed.
     *
     * @param listener called for every message
     * @return the subscription; close it to stop
     */
    ChannelSubscription subscribe(Consumer<ChannelMessage> listener);
}
