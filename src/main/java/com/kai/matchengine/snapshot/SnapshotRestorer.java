package com.kai.matchengine.snapshot;

import com.kai.matchengine.match.EngineRegistry;
import com.kai.matchengine.match.MatchingEngine;
import com.kai.matchengine.snapshot.delta.BookDelta;
import com.kai.matchengine.snapshot.delta.BookDeltaBatchChain;
import com.kai.matchengine.snapshot.delta.BookDeltaBatchStore;
import com.kai.matchengine.snapshot.delta.BookDeltaCodec;
import com.kai.matchengine.snapshot.delta.BookDeltaEngineReplayer;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 从 SnapshotStore 恢复 EngineRegistry（含 symbol 元数据）。 */
public final class SnapshotRestorer {

  private static final Logger log = LoggerFactory.getLogger(SnapshotRestorer.class);

  private final SnapshotStore store;
  private final BookDeltaBatchStore batchStore;
  private final SnapshotCodec codec = new SnapshotCodec();
  private final BookDeltaCodec deltaCodec = new BookDeltaCodec();

  public SnapshotRestorer(SnapshotStore store) {
    this(store, null);
  }

  public SnapshotRestorer(SnapshotStore store, BookDeltaBatchStore batchStore) {
    this.store = Objects.requireNonNull(store, "store");
    this.batchStore = batchStore;
  }

  /**
   * @return 恢复使用的 lastFullyProcessedOffset（含 batch 链末端 G）；无快照则 empty
   */
  public Optional<Long> restoreLatest(
      EngineRegistry registry, String matchTopic, int matchPartition) {
    Optional<SnapshotStore.StoredSnapshot> latest = store.loadLatest(matchTopic, matchPartition);
    if (latest.isEmpty()) {
      log.info("snapshot restore: none found topic={} partition={}", matchTopic, matchPartition);
      return Optional.empty();
    }
    SnapshotStore.StoredSnapshot stored = latest.get();
    int symbolCount = 0;
    int orderCount = 0;
    for (SnapshotStore.Section section : stored.sections()) {
      if (!SnapshotCodec.SECTION_MATCH_SYMBOL.equals(section.sectionType())) {
        continue;
      }
      EngineSnapshot.SymbolState state = codec.decodeSymbol(section.payload());
      MatchingEngine engine =
          registry.restoreSymbol(state.symbolId(), state.code(), state.status());
      engine.clearBook();
      engine.restoreSequences(state.eventId(), state.bookVersion());
      engine.restoreRecentOrderIds(state.recentOrderIds());
      for (EngineSnapshot.RestingOrderState o : state.orders()) {
        engine.restoreRestingOrder(
            o.orderId(), o.userId(), o.toSide(), o.priceTick(), o.qty());
      }
      symbolCount++;
      orderCount += state.orders().size();
      log.info(
          "snapshot restore symbolId={} code={} status={} eventId={} bookVersion={} orders={} recentOrderIds={}",
          state.symbolId(),
          state.code(),
          state.status(),
          state.eventId(),
          state.bookVersion(),
          state.orders().size(),
          state.recentOrderIds().size());
    }
    long offset = stored.lastFullyProcessedOffset();
    if (batchStore != null) {
      offset = replayDeltaBatches(registry, stored, matchTopic, matchPartition, offset);
    }
    log.info(
        "snapshot restore done topic={} partition={} offset={} symbols={} orders={}",
        matchTopic,
        matchPartition,
        offset,
        symbolCount,
        orderCount);
    return Optional.of(offset);
  }

  private long replayDeltaBatches(
      EngineRegistry registry,
      SnapshotStore.StoredSnapshot stored,
      String matchTopic,
      int matchPartition,
      long fullOffset) {
    List<BookDeltaBatchStore.StoredBatch> chain =
        BookDeltaBatchChain.loadValidatedChain(
            batchStore,
            deltaCodec,
            stored.id(),
            matchTopic,
            matchPartition,
            fullOffset);
    for (BookDeltaBatchStore.StoredBatch batch : chain) {
      for (BookDeltaBatchStore.Section section : batch.sections()) {
        for (BookDelta delta : deltaCodec.decode(section.payload())) {
          BookDeltaEngineReplayer.apply(registry, delta);
        }
      }
      log.info(
          "delta batch replay id={} from={} to={} deltas={}",
          batch.id(),
          batch.fromOffset(),
          batch.toOffset(),
          batch.deltaCount());
    }
    return BookDeltaBatchChain.chainEndOffset(chain, fullOffset);
  }
}
