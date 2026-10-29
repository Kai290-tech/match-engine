package com.kai.matchengine.snapshot.delta;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/** 单测用内存增量 batch 库。 */
public final class InMemoryBookDeltaBatchStore implements BookDeltaBatchStore {

  private final AtomicLong seq = new AtomicLong();
  private final Map<Long, StoredBatch> byId = new ConcurrentHashMap<>();

  @Override
  public long save(
      long baseSnapshotId,
      String matchTopic,
      int matchPartition,
      long fromOffset,
      long toOffset,
      int deltaCount,
      List<Section> sections) {
    long id = seq.incrementAndGet();
    StoredBatch batch =
        new StoredBatch(
            id,
            baseSnapshotId,
            matchTopic,
            matchPartition,
            fromOffset,
            toOffset,
            deltaCount,
            List.copyOf(sections));
    byId.put(id, batch);
    return id;
  }

  @Override
  public List<StoredBatch> loadChain(long baseSnapshotId, String matchTopic, int matchPartition) {
    return byId.values().stream()
        .filter(
            b ->
                b.baseSnapshotId() == baseSnapshotId
                    && b.matchTopic().equals(matchTopic)
                    && b.matchPartition() == matchPartition)
        .sorted(Comparator.comparingLong(StoredBatch::fromOffset))
        .toList();
  }

  @Override
  public void pruneThrough(
      long baseSnapshotId, String matchTopic, int matchPartition, long throughOffset) {
    List<Long> remove = new ArrayList<>();
    for (StoredBatch b : byId.values()) {
      if (b.baseSnapshotId() == baseSnapshotId
          && b.matchTopic().equals(matchTopic)
          && b.matchPartition() == matchPartition
          && b.toOffset() <= throughOffset) {
        remove.add(b.id());
      }
    }
    remove.forEach(byId::remove);
  }

  public int size() {
    return byId.size();
  }
}
