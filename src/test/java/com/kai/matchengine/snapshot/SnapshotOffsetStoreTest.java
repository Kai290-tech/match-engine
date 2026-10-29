package com.kai.matchengine.snapshot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.kai.matchengine.match.EngineRegistry;
import com.kai.matchengine.match.SystemTimeSource;
import com.kai.matchengine.snapshot.delta.InMemoryBookDeltaBatchStore;
import org.junit.jupiter.api.Test;

class SnapshotOffsetStoreTest {

  @Test
  void loadStartingOffset_includesDeltaBatchChain() {
    InMemorySnapshotStore snapshotStore = new InMemorySnapshotStore();
    InMemoryBookDeltaBatchStore batchStore = new InMemoryBookDeltaBatchStore();
    SnapshotCodec codec = new SnapshotCodec();
    EngineRegistry registry = new EngineRegistry(SystemTimeSource.INSTANCE);
    registry.register(1);

    SnapshotPreparer preparer = new SnapshotPreparer(registry);
    EngineSnapshot full = preparer.prepare("match_event", 0, 0L, 1L);
    long baseId =
        snapshotStore.save(
            full,
            java.util.List.of(
                new SnapshotStore.Section(
                    SnapshotCodec.SECTION_ENGINE_META,
                    "",
                    SnapshotCodec.ENCODING_JSON_GZIP,
                    codec.encodeMeta(full)),
                new SnapshotStore.Section(
                    SnapshotCodec.SECTION_MATCH_SYMBOL,
                    "1",
                    SnapshotCodec.ENCODING_JSON_GZIP,
                    codec.encodeSymbol(full.symbols().getFirst()))));

    batchStore.save(baseId, "match_event", 0, 1L, 5L, 0, java.util.List.of());

    SnapshotOffsetStore offsets = new SnapshotOffsetStore(snapshotStore, batchStore, 0L);
    assertEquals(6L, offsets.loadStartingOffset("match_event", 0));
  }

  @Test
  void loadLatest_rejectsIncompleteSnapshot() {
    InMemorySnapshotStore store = new InMemorySnapshotStore();
    SnapshotCodec codec = new SnapshotCodec();
    EngineRegistry registry = new EngineRegistry(SystemTimeSource.INSTANCE);
    registry.register(1);
    EngineSnapshot snap = new SnapshotPreparer(registry).prepare("match_event", 0, 0L, 1L);
    store.save(
        snap,
        java.util.List.of(
            new SnapshotStore.Section(
                SnapshotCodec.SECTION_ENGINE_META,
                "",
                SnapshotCodec.ENCODING_JSON_GZIP,
                codec.encodeMeta(snap))));
    assertThrows(IllegalStateException.class, () -> store.loadLatest("match_event", 0));
  }
}
