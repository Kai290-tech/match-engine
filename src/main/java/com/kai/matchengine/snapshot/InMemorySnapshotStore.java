package com.kai.matchengine.snapshot;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/** 单测用内存快照库。 */
public final class InMemorySnapshotStore implements SnapshotStore {

  private final AtomicLong seq = new AtomicLong();
  private final Map<Long, StoredSnapshot> byId = new ConcurrentHashMap<>();

  @Override
  public long save(EngineSnapshot snapshot, List<Section> sections) {
    long id = seq.incrementAndGet();
    StoredSnapshot stored =
        new StoredSnapshot(
            id,
            snapshot.schemaVersion(),
            snapshot.matchTopic(),
            snapshot.matchPartition(),
            snapshot.lastFullyProcessedOffset(),
            List.copyOf(sections));
    byId.put(id, stored);
    return id;
  }

  @Override
  public Optional<StoredSnapshot> loadLatest(String matchTopic, int matchPartition) {
    return byId.values().stream()
        .filter(s -> s.matchTopic().equals(matchTopic) && s.matchPartition() == matchPartition)
        .max(Comparator.comparingLong(StoredSnapshot::lastFullyProcessedOffset))
        .map(
            stored -> {
              SnapshotStoreValidator.validateStoredSnapshot(stored);
              return stored;
            });
  }

  @Override
  public void retainLatest(String matchTopic, int matchPartition, int keep) {
    List<StoredSnapshot> list =
        byId.values().stream()
            .filter(s -> s.matchTopic().equals(matchTopic) && s.matchPartition() == matchPartition)
            .sorted(Comparator.comparingLong(StoredSnapshot::lastFullyProcessedOffset).reversed())
            .toList();
    for (int i = keep; i < list.size(); i++) {
      byId.remove(list.get(i).id());
    }
  }

  public int size() {
    return byId.size();
  }

  public List<StoredSnapshot> all() {
    return new ArrayList<>(byId.values());
  }
}
