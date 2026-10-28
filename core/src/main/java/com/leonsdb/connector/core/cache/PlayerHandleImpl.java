// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.core.cache;

import com.leonsdb.connector.api.PlayerHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/** A lightweight view onto one player's cached data. */
final class PlayerHandleImpl implements PlayerHandle {

    private final LeonsDBService service;
    private final UUID uuid;

    PlayerHandleImpl(LeonsDBService service, UUID uuid) {
        this.service = service;
        this.uuid = uuid;
    }

    @Override
    public UUID uuid() {
        return uuid;
    }

    @Override
    public boolean isLoaded() {
        return service.isLoaded(uuid);
    }

    @Override
    public <T> T get(Class<T> type) {
        return service.cached(uuid, Objects.requireNonNull(type, "type"), false).instance();
    }

    @Override
    public <T> CompletableFuture<Void> update(Class<T> type, Consumer<? super T> change) {
        service.requirePlayerThread(uuid, "PlayerHandle.update");
        CachedObject<T> obj = service.cached(uuid, Objects.requireNonNull(type, "type"), true);
        change.accept(obj.instance());
        CompletableFuture<Void> saved = new CompletableFuture<>();
        obj.submit(obj.snapshot(), saved);
        return saved;
    }

    @Override
    public CompletableFuture<Void> save() {
        service.requirePlayerThread(uuid, "PlayerHandle.save");
        List<CompletableFuture<Void>> saves = new ArrayList<>();
        for (CachedObject<?> obj : service.objectsOf(uuid)) {
            CompletableFuture<Void> saved = new CompletableFuture<>();
            obj.submit(obj.snapshot(), saved);
            saves.add(saved);
        }
        return CompletableFuture.allOf(saves.toArray(new CompletableFuture<?>[0]));
    }
}
