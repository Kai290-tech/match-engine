package com.kai.matchengine.snapshot;

import java.util.List;
import java.util.Optional;

/** 快照持久化。 */
public interface SnapshotStore {

  record Section(String sectionType, String sectionKey, String encoding, byte[] payload) {}

  record StoredSnapshot(
      long id,
      int schemaVersion,
      String matchTopic,
      int matchPartition,
      long lastFullyProcessedOffset,
      List<Section> sections) {}

  long save(EngineSnapshot snapshot, List<Section> sections);

  Optional<StoredSnapshot> loadLatest(String matchTopic, int matchPartition);

  /** 同 topic+partition 只保留最新 keep 份（按 offset DESC）。 */
  void retainLatest(String matchTopic, int matchPartition, int keep);
}
