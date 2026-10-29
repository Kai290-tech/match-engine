package com.kai.matchengine.snapshot;

import com.kai.matchengine.ingress.OffsetStore;
import com.kai.matchengine.snapshot.delta.BookDeltaBatchStore;
import com.kai.matchengine.snapshot.delta.BookDeltaBatchChain;
import com.kai.matchengine.snapshot.delta.BookDeltaCodec;
import java.util.Objects;
import java.util.Optional;

/** 启动 seek：全量 @F + batch 链至 G → 从 G+1 消费 Kafka。 */
public final class SnapshotOffsetStore implements OffsetStore {

  private final SnapshotStore snapshotStore;
  private final BookDeltaBatchStore batchStore;
  private final BookDeltaCodec deltaCodec;
  private final long fallbackOffset;

  public SnapshotOffsetStore(SnapshotStore snapshotStore, long fallbackOffset) {
    this(snapshotStore, null, fallbackOffset);
  }

  public SnapshotOffsetStore(
      SnapshotStore snapshotStore, BookDeltaBatchStore batchStore, long fallbackOffset) {
    this(snapshotStore, batchStore, new BookDeltaCodec(), fallbackOffset);
  }

  public SnapshotOffsetStore(
      SnapshotStore snapshotStore,
      BookDeltaBatchStore batchStore,
      BookDeltaCodec deltaCodec,
      long fallbackOffset) {
    this.snapshotStore = Objects.requireNonNull(snapshotStore, "snapshotStore");
    this.batchStore = batchStore;
    this.deltaCodec = Objects.requireNonNull(deltaCodec, "deltaCodec");
    if (fallbackOffset < 0) {
      throw new IllegalArgumentException("fallbackOffset must be >= 0");
    }
    this.fallbackOffset = fallbackOffset;
  }

  @Override
  public long loadStartingOffset(String topic, int partition) {
    Optional<SnapshotStore.StoredSnapshot> latest = snapshotStore.loadLatest(topic, partition);
    if (latest.isEmpty()) {
      return fallbackOffset;
    }
    SnapshotStore.StoredSnapshot stored = latest.get();
    long applied = stored.lastFullyProcessedOffset();
    if (batchStore != null) {
      applied =
          BookDeltaBatchChain.maxAppliedOffset(
              batchStore,
              deltaCodec,
              stored.id(),
              topic,
              partition,
              applied);
    }
    return applied + 1;
  }
}
