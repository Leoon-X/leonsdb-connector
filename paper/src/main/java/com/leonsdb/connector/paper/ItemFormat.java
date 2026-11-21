// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.paper;

import org.bukkit.inventory.ItemStack;

/**
 * One way of turning an item into bytes and back. See {@link Items} for
 * which one a server writes and which ones it reads.
 */
interface ItemFormat {

    /**
     * @return a short name for logs, for example "Paper NBT"
     */
    String name();

    /**
     * Writes an item that is not empty.
     *
     * @param item the item
     * @return its bytes
     * @throws Exception if it cannot be written
     */
    byte[] write(ItemStack item) throws Exception;

    /**
     * Reads an item this format wrote.
     *
     * @param data the bytes
     * @return the item
     * @throws Exception if it cannot be read
     */
    ItemStack read(byte[] data) throws Exception;
}
