// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.example;

import com.leonsdb.connector.api.GlobalData;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/** Data shared by all servers, keyed by the clan's name. */
@GlobalData(namespace = "example", collection = "clans")
public class Clan {

    public UUID leader;

    public List<UUID> members = new ArrayList<>();
}
