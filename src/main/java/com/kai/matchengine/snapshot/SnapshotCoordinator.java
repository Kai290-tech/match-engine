package com.kai.matchengine.snapshot;

import com.kai.matchengine.ingress.FatalFailureHandler;
import com.kai.matchengine.match.EngineRegistry;
import com.kai.matchengine.outbound.OutboundWatermark;
import com.kai.matchengine.snapshot.delta.BookDelta;
import com.kai.matchengine.snapshot.delta.BookDeltaBatchStore;
import com.kai.matchengine.snapshot.delta.BookDeltaCodec;
import com.kai.matchengine.snapshot.delta.BookDeltaBatchChain;
import com.kai.matchengine.snapshot.delta.BookDeltaLog;
import com.kai.matchengine.snapshot.delta.MaterializerRegistrySync;
import com.kai.matchengine.snapshot.delta.SnapshotMaterializer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 快照协调：snapshot 线程 drain {@link BookDeltaLog} → 镜像；事件触发增量 batch / 全量 compaction。
 *
 * <p>撮合线程 {@link #onMatched} 只更新 offset 计数，并在跨过 {@code everyNOffsets} 时请求全量。
 */
public final class SnapshotCoordinator implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(SnapshotCoordinator.class);

  public enum State {
    IDLE,
    BUSY
  }

  private final EngineRegistry registry;
  private final SnapshotStore store;
  private final BookDeltaBatchStore batchStore;
  private final BookDeltaLog deltaLog;
  private final SnapshotMaterializer materializer;
  private final BookDeltaCodec deltaCodec = new BookDeltaCodec();
  private final SnapshotCodec snapshotCodec = new SnapshotCodec();
  private final OutboundWatermark watermark;
  private final FatalFailureHandler fatalFailureHandler;
  private final String matchTopic;
  private final int matchPartition;
  private final long deltaTriggerOffsetSpan;
  private final long deltaTriggerDeltaCount;
  private final long everyNOffsets;
  private final double fullMaxBookFillRatio;
  private final int retainCount;
  private final long persistAwaitTimeoutMs;
  private final JdbcSnapshotCompactionWriter jdbcCompactionWriter;

  private final AtomicReference<State> state = new AtomicReference<>(State.IDLE);
  private final AtomicBoolean fullCompactionRequested = new AtomicBoolean(false);
  private final AtomicLong processedSinceBatch = new AtomicLong(0);
  private final AtomicLong lastSeenMatchOffset = new AtomicLong(-1L);

  private volatile long baseSnapshotId = 0L;
  private volatile long lastBatchToOffset = -1L;
  private final AtomicLong lastFullSnapshotOffset = new AtomicLong(-1L);
  private final AtomicLong fullRequestedAt = new AtomicLong(-1L);

  private final BlockingQueue<EngineSnapshot> pendingFull = new ArrayBlockingQueue<>(1);
  private final List<BookDelta> pendingBatchDeltas = new ArrayList<>();
  private final AtomicBoolean running = new AtomicBoolean(false);
  private final AtomicBoolean fatalStopped = new AtomicBoolean(false);
  private volatile Thread persistThread;

  public SnapshotCoordinator(
      EngineRegistry registry,
      SnapshotStore store,
      BookDeltaBatchStore batchStore,
      BookDeltaLog deltaLog,
      SnapshotMaterializer materializer,
      OutboundWatermark watermark,
      FatalFailureHandler fatalFailureHandler,
      String matchTopic,
      int matchPartition,
      long deltaTriggerOffsetSpan,
      long deltaTriggerDeltaCount,
      double fullMaxBookFillRatio,
      int retainCount,
      long persistAwaitTimeoutMs) {
    this(
        registry,
        store,
        batchStore,
        deltaLog,
        materializer,
        watermark,
        fatalFailureHandler,
        matchTopic,
        matchPartition,
        deltaTriggerOffsetSpan,
        deltaTriggerDeltaCount,
        fullMaxBookFillRatio,
        retainCount,
        persistAwaitTimeoutMs,
        0L,
        null);
  }

  public SnapshotCoordinator(
      EngineRegistry registry,
      SnapshotStore store,
      BookDeltaBatchStore batchStore,
      BookDeltaLog deltaLog,
      SnapshotMaterializer materializer,
      OutboundWatermark watermark,
      FatalFailureHandler fatalFailureHandler,
      String matchTopic,
      int matchPartition,
      long deltaTriggerOffsetSpan,
      long deltaTriggerDeltaCount,
      double fullMaxBookFillRatio,
      int retainCount,
      long persistAwaitTimeoutMs,
      JdbcSnapshotCompactionWriter jdbcCompactionWriter) {
    this(
        registry,
        store,
        batchStore,
        deltaLog,
        materializer,
        watermark,
        fatalFailureHandler,
        matchTopic,
        matchPartition,
        deltaTriggerOffsetSpan,
        deltaTriggerDeltaCount,
        fullMaxBookFillRatio,
        retainCount,
        persistAwaitTimeoutMs,
        0L,
        jdbcCompactionWriter);
  }

  public SnapshotCoordinator(
      EngineRegistry registry,
      SnapshotStore store,
      BookDeltaBatchStore batchStore,
      BookDeltaLog deltaLog,
      SnapshotMaterializer materializer,
      OutboundWatermark watermark,
      FatalFailureHandler fatalFailureHandler,
      String matchTopic,
      int matchPartition,
      long deltaTriggerOffsetSpan,
      long deltaTriggerDeltaCount,
      double fullMaxBookFillRatio,
      int retainCount,
      long persistAwaitTimeoutMs,
      long everyNOffsets,
      JdbcSnapshotCompactionWriter jdbcCompactionWriter) {
    this.registry = Objects.requireNonNull(registry, "registry");
    this.store = Objects.requireNonNull(store, "store");
    this.batchStore = Objects.requireNonNull(batchStore, "batchStore");
    this.deltaLog = Objects.requireNonNull(deltaLog, "deltaLog");
    this.materializer = Objects.requireNonNull(materializer, "materializer");
    this.watermark = Objects.requireNonNull(watermark, "watermark");
    this.fatalFailureHandler = Objects.requireNonNull(fatalFailureHandler, "fatalFailureHandler");
    this.matchTopic = Objects.requireNonNull(matchTopic, "matchTopic");
    this.matchPartition = matchPartition;
    this.deltaTriggerOffsetSpan = deltaTriggerOffsetSpan;
    this.deltaTriggerDeltaCount = deltaTriggerDeltaCount;
    this.everyNOffsets = everyNOffsets;
    this.fullMaxBookFillRatio = fullMaxBookFillRatio;
    this.retainCount = Math.max(1, retainCount);
    this.persistAwaitTimeoutMs = persistAwaitTimeoutMs;
    this.jdbcCompactionWriter = jdbcCompactionWriter;
  }

  /** 启动前：从最新全量恢复镜像基底、batch 链与 {@code base_snapshot_id}。 */
  public void bootstrapFromStore() {
    syncMaterializerSymbols();
    store
        .loadLatest(matchTopic, matchPartition)
        .ifPresent(
            stored -> {
              loadBaseFromStored(stored);
              replayBatchesToMaterializer(stored);
            });
  }

  private void replayBatchesToMaterializer(SnapshotStore.StoredSnapshot stored) {
    long fullOffset = stored.lastFullyProcessedOffset();
    List<BookDeltaBatchStore.StoredBatch> chain =
        BookDeltaBatchChain.loadValidatedChain(
            batchStore, deltaCodec, stored.id(), matchTopic, matchPartition, fullOffset);
    BookDeltaBatchChain.replayToMaterializer(materializer, deltaCodec, chain);
    lastBatchToOffset = BookDeltaBatchChain.chainEndOffset(chain, fullOffset);
    if (!chain.isEmpty()) {
      log.info(
          "materializer replayed {} delta batches through offset={}",
          chain.size(),
          lastBatchToOffset);
    }
  }

  public void start() {
    if (!running.compareAndSet(false, true)) {
      return;
    }
    persistThread = new Thread(this::persistLoop, "snapshot-persist");
    persistThread.setDaemon(true);
    persistThread.start();
  }

  /** 撮合线程在 apply+出站入队成功后调用（O(1)）。 */
  public void onMatched(long matchOffset, long nowMillis) {
    if (fatalStopped.get()) {
      return;
    }
    lastSeenMatchOffset.set(matchOffset);
    processedSinceBatch.incrementAndGet();
    if (everyNOffsets <= 0) {
      return;
    }
    long lastFull = lastFullSnapshotOffset.get();
    long sinceFull = lastFull < 0 ? matchOffset + 1 : matchOffset - lastFull;
    long requestedAt = fullRequestedAt.get();
    if (sinceFull >= everyNOffsets && requestedAt <= lastFull
        && fullRequestedAt.compareAndSet(requestedAt, matchOffset)) {
      requestSnapshot();
    }
  }

  /** 请求全量 compaction（需 {@link #bookFillRatioAllowsFull()}）。 */
  public void requestSnapshot() {
    fullCompactionRequested.set(true);
  }

  public State state() {
    return state.get();
  }

  public long baseSnapshotId() {
    return baseSnapshotId;
  }

  public long lastBatchToOffset() {
    return lastBatchToOffset;
  }

  @Override
  public void close() {
    running.set(false);
    Thread t = persistThread;
    if (t != null) {
      t.interrupt();
      try {
        t.join(5_000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    flushPendingBestEffort();
  }

  /** 停机：drain 队列并尽力落库当前 batch（不阻塞撮合线程）。 */
  private void flushPendingBestEffort() {
    if (fatalStopped.get()) {
      return;
    }
    try {
      drainDeltas();
      if (pendingBatchDeltas.isEmpty() || deltaLog.gapActive() || baseSnapshotId <= 0) {
        return;
      }
      long toOffset = lastSeenMatchOffset.get();
      if (toOffset < 0) {
        return;
      }
      if (watermark.ackedOffset() < toOffset) {
        log.warn(
            "shutdown skip delta batch flush: outbound acked={} need={}",
            watermark.ackedOffset(),
            toOffset);
        return;
      }
      long fromOffset = lastBatchToOffset + 1;
      if (lastBatchToOffset < 0) {
        fromOffset = 0L;
      }
      persistDeltaBatch(fromOffset, toOffset);
      processedSinceBatch.set(0);
      lastBatchToOffset = toOffset;
      pendingBatchDeltas.clear();
      log.info("shutdown delta batch flushed to={}", toOffset);
    } catch (Exception e) {
      log.warn("shutdown delta batch flush failed", e);
    }
  }

  private void persistLoop() {
    try {
      while (running.get()) {
        try {
          drainDeltas();
          maybeFlushDeltaBatch();
          maybePrepareFullCompaction();
          EngineSnapshot snap = pendingFull.poll(50, TimeUnit.MILLISECONDS);
          if (snap != null) {
            persistFullSnapshot(snap);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        } catch (Throwable t) {
          // 快照线程异常不得拖垮撮合；告警后继续下一周期
          log.warn("snapshot persist cycle failed; will retry next cycle", t);
        }
      }
    } finally {
      running.set(false);
    }
  }

  private void drainDeltas() {
    BookDelta delta;
    while ((delta = deltaLog.poll()) != null) {
      materializer.apply(delta);
      pendingBatchDeltas.add(delta);
    }
  }

  private void maybeFlushDeltaBatch() {
    if (pendingBatchDeltas.isEmpty()) {
      return;
    }
    if (deltaLog.gapActive()) {
      log.warn("delta batch flush blocked: BookDeltaLog GAP active, requesting full snapshot");
      requestSnapshot();
      return;
    }
    if (baseSnapshotId <= 0) {
      log.debug("delta batch flush deferred: no base full snapshot yet (baseSnapshotId=0)");
      return;
    }
    long toOffset = lastSeenMatchOffset.get();
    if (toOffset < 0) {
      return;
    }
    long processed = processedSinceBatch.get();
    long offsetSpan = lastBatchToOffset >= 0 ? toOffset - lastBatchToOffset : toOffset + 1;
    boolean dueByCount = deltaTriggerDeltaCount > 0 && processed >= deltaTriggerDeltaCount;
    boolean dueByOffset = deltaTriggerOffsetSpan > 0 && offsetSpan >= deltaTriggerOffsetSpan;
    if (!dueByCount && !dueByOffset) {
      return;
    }

    long fromOffset = lastBatchToOffset + 1;
    if (lastBatchToOffset < 0) {
      fromOffset = 0L;
    }
    try {
      if (!watermark.awaitAtLeast(toOffset, persistAwaitTimeoutMs, TimeUnit.MILLISECONDS)) {
        return;
      }
      persistDeltaBatch(fromOffset, toOffset);
      // 仅成功后清空；失败则保留 pending，下次触发自然带上未写入数据
      processedSinceBatch.set(0);
      lastBatchToOffset = toOffset;
      pendingBatchDeltas.clear();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.warn("delta batch persist interrupted; will retry next cycle from={}", fromOffset);
    } catch (Throwable t) {
      log.warn(
          "delta batch persist failed; keep pending for next cycle from={} to={} deltas={}",
          fromOffset,
          toOffset,
          pendingBatchDeltas.size(),
          t);
    }
  }

  private void persistDeltaBatch(long fromOffset, long toOffset) {
    if (baseSnapshotId <= 0) {
      throw new IllegalStateException("refusing delta batch persist without base full snapshot");
    }
    Map<Integer, List<BookDelta>> bySymbol = new HashMap<>();
    for (BookDelta delta : pendingBatchDeltas) {
      bySymbol.computeIfAbsent(delta.symbolId(), k -> new ArrayList<>()).add(delta);
    }
    List<BookDeltaBatchStore.Section> sections = new ArrayList<>();
    for (Map.Entry<Integer, List<BookDelta>> e : bySymbol.entrySet()) {
      sections.add(
          new BookDeltaBatchStore.Section(e.getKey(), deltaCodec.encode(e.getValue())));
    }
    long batchId =
        batchStore.save(
            baseSnapshotId,
            matchTopic,
            matchPartition,
            fromOffset,
            toOffset,
            pendingBatchDeltas.size(),
            sections);
    log.info(
        "delta batch persisted id={} baseSnapshotId={} from={} to={} deltas={}",
        batchId,
        baseSnapshotId,
        fromOffset,
        toOffset,
        pendingBatchDeltas.size());
  }

  private void maybePrepareFullCompaction() {
    if (!fullCompactionRequested.get()) {
      return;
    }
    if (!bookFillRatioAllowsFull()) {
      return;
    }
    long offset = lastSeenMatchOffset.get();
    if (offset < 0) {
      return;
    }
    if (!state.compareAndSet(State.IDLE, State.BUSY)) {
      return;
    }
    if (!fullCompactionRequested.compareAndSet(true, false)) {
      state.set(State.IDLE);
      return;
    }
    try {
      syncMaterializerSymbols();
      EngineSnapshot snap = buildSnapshot(offset, System.currentTimeMillis());
      if (!pendingFull.offer(snap)) {
        log.warn("pending full snapshot slot full offset={}", offset);
        fullCompactionRequested.set(true);
        state.set(State.IDLE);
      }
    } catch (Throwable t) {
      state.set(State.IDLE);
      fullCompactionRequested.set(true);
      log.warn("full snapshot prepare failed offset={}; will retry next request", offset, t);
    }
  }

  private void persistFullSnapshot(EngineSnapshot snap) {
    long n = snap.lastFullyProcessedOffset();
    long oldBase = baseSnapshotId;
    try {
      if (!watermark.awaitAtLeast(n, persistAwaitTimeoutMs, TimeUnit.MILLISECONDS)) {
        log.warn(
            "full snapshot persist deferred: outbound acked={} need={}; will retry next cycle",
            watermark.ackedOffset(),
            n);
        fullCompactionRequested.set(true);
        return;
      }
      List<SnapshotStore.Section> sections = new ArrayList<>();
      sections.add(
          new SnapshotStore.Section(
              SnapshotCodec.SECTION_ENGINE_META,
              "",
              SnapshotCodec.ENCODING_JSON_GZIP,
              snapshotCodec.encodeMeta(snap)));
      for (EngineSnapshot.SymbolState symbol : snap.symbols()) {
        sections.add(
            new SnapshotStore.Section(
                SnapshotCodec.SECTION_MATCH_SYMBOL,
                Integer.toString(symbol.symbolId()),
                SnapshotCodec.ENCODING_JSON_GZIP,
                snapshotCodec.encodeSymbol(symbol)));
      }
      long newId;
      if (jdbcCompactionWriter != null) {
        newId =
            jdbcCompactionWriter.persistFullCompaction(
                snap, sections, retainCount, oldBase, matchTopic, matchPartition, n);
      } else {
        newId = store.save(snap, sections);
        store.retainLatest(matchTopic, matchPartition, retainCount);
        if (oldBase > 0) {
          batchStore.pruneThrough(oldBase, matchTopic, matchPartition, n);
        }
      }
      baseSnapshotId = newId;
      lastBatchToOffset = n;
      lastFullSnapshotOffset.set(n);
      pendingBatchDeltas.clear();
      processedSinceBatch.set(0);
      log.info(
          "full snapshot persisted id={} offset={} symbols={}",
          newId,
          n,
          snap.symbols().size());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.warn("full snapshot persist interrupted offset={}; will retry next cycle", n);
      fullCompactionRequested.set(true);
    } catch (Throwable t) {
      log.warn(
          "full snapshot persist failed offset={}; keep pending deltas for next cycle", n, t);
      fullCompactionRequested.set(true);
    } finally {
      state.set(State.IDLE);
      if (fullCompactionRequested.get()) {
        maybePrepareFullCompaction();
      }
    }
  }

  private EngineSnapshot buildSnapshot(long offset, long preparedAtMillis) {
    syncMaterializerSymbols();
    if (deltaLog.gapActive()) {
      log.warn("full snapshot export: rebuilding materializer from live registry after GAP");
      MaterializerRegistrySync.rebuildFromRegistry(registry, materializer);
      deltaLog.clearGap();
    }
    return new EngineSnapshot(
        EngineSnapshot.SCHEMA_VERSION,
        matchTopic,
        matchPartition,
        offset,
        preparedAtMillis,
        materializer.exportAllSymbolStates());
  }

  private void syncMaterializerSymbols() {
    for (Integer symbolId : registry.symbolIds()) {
      var info = registry.symbols().get(symbolId);
      if (info != null) {
        materializer.registerSymbolMeta(info);
      }
    }
  }

  private void loadBaseFromStored(SnapshotStore.StoredSnapshot stored) {
    baseSnapshotId = stored.id();
    lastBatchToOffset = stored.lastFullyProcessedOffset();
    lastFullSnapshotOffset.set(stored.lastFullyProcessedOffset());
    fullRequestedAt.set(stored.lastFullyProcessedOffset());
    for (SnapshotStore.Section section : stored.sections()) {
      if (!SnapshotCodec.SECTION_MATCH_SYMBOL.equals(section.sectionType())) {
        continue;
      }
      try {
        int symbolId = Integer.parseInt(section.sectionKey());
        materializer.loadSymbol(snapshotCodec.decodeSymbol(section.payload()));
        log.info("materializer loaded symbolId={} from snapshot id={}", symbolId, baseSnapshotId);
      } catch (NumberFormatException e) {
        log.warn("skip snapshot section bad symbol key={}", section.sectionKey());
      }
    }
  }

  private boolean bookFillRatioAllowsFull() {
    return registry.maxBookFillRatio() < fullMaxBookFillRatio;
  }

  private void stopFatal(String msg, Throwable t) {
    fatalStopped.set(true);
    running.set(false);
    state.set(State.BUSY);
    fatalFailureHandler.onFatal(msg, t);
  }
}
