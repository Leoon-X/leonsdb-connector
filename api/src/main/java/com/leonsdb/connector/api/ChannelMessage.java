// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.api;

import java.time.Instant;

/**
 * A message received on a {@link Channel}.
 *
 * @param channel the channel's name
 * @param text    the message
 * @param sender  who sent it: the sending Minecraft server's name, or for
 *                websites and bots their token's name
 * @param time    when it was sent
 */
public record ChannelMessage(String channel, String text, String sender, Instant time) {
}
