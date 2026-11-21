// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.api;

import java.time.Instant;
import java.util.Objects;

/**
 * A message received on a {@link Channel}.
 */
public final class ChannelMessage {

    private final String channel;
    private final String text;
    private final String sender;
    private final Instant time;

    /**
     * Creates a message.
     *
     * @param channel the channel's name
     * @param text    the message
     * @param sender  who sent it: the sending Minecraft server's name, or for
     *                websites and bots their token's name
     * @param time    when it was sent
     */
    public ChannelMessage(String channel, String text, String sender, Instant time) {
        this.channel = channel;
        this.text = text;
        this.sender = sender;
        this.time = time;
    }

    /** @return the channel's name */
    public String channel() {
        return channel;
    }

    /** @return the message */
    public String text() {
        return text;
    }

    /**
     * @return who sent it: the sending Minecraft server's name, or for
     *         websites and bots their token's name
     */
    public String sender() {
        return sender;
    }

    /** @return when it was sent */
    public Instant time() {
        return time;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ChannelMessage)) {
            return false;
        }
        ChannelMessage m = (ChannelMessage) o;
        return Objects.equals(channel, m.channel) && Objects.equals(text, m.text)
                && Objects.equals(sender, m.sender) && Objects.equals(time, m.time);
    }

    @Override
    public int hashCode() {
        return Objects.hash(channel, text, sender, time);
    }

    @Override
    public String toString() {
        return "ChannelMessage[channel=" + channel + ", text=" + text + ", sender=" + sender + ", time=" + time + "]";
    }
}
