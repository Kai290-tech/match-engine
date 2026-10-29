package com.kai.matchengine.snapshot.delta;

import java.util.List;

/** 增量 batch 持久化（挂在 {@code base_snapshot_id} 全量版本上）。 */
public interface BookDeltaBatchStore {

  record Section(int symbolId, byte[] payload) {}

  record StoredBatch(
      long id,
      long baseSnapshotId,
      String matchTopic,
      int matchPartition,
      long fromOffset,
      long toOffset,
      int deltaCount,
      List<Section> sections) {}

  long save(
      long baseSnapshotId,
      String matchTopic,
      int matchPartition,
      long fromOffset,
      long toOffset,
      int deltaCount,
      List<Section> sections);

  List<StoredBatch> loadChain(long baseSnapshotId, String matchTopic, int matchPartition);

  void pruneThrough(long baseSnapshotId, String matchTopic, int matchPartition, long throughOffset);
}
