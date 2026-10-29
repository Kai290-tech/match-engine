package com.kai.matchengine.snapshot;

import com.kai.matchengine.ingress.FatalFailureHandler;
import com.kai.matchengine.match.EngineRegistry;
import com.kai.matchengine.outbound.OutboundWatermark;
import com.kai.matchengine.snapshot.delta.BookDeltaBatchStore;
import com.kai.matchengine.snapshot.delta.BookDeltaLog;
import com.kai.matchengine.snapshot.delta.BookDeltaRecorder;
import com.kai.matchengine.snapshot.delta.InMemoryBookDeltaBatchStore;
import com.kai.matchengine.snapshot.delta.SnapshotMaterializer;

/** 单测用快照协调器装配。 */
public final class SnapshotTestFixtures {

  private SnapshotTestFixtures() {}

  public record CoordinatorBundle(
      EngineRegistry registry,
      InMemorySnapshotStore snapshotStore,
      BookDeltaLog deltaLog,
      BookDeltaRecorder recorder,
      BookDeltaBatchStore batchStore,
      SnapshotMaterializer materializer,
      SnapshotCoordinator coordinator) {}

  public static CoordinatorBundle newCoordinator(
      OutboundWatermark watermark,
      FatalFailureHandler fatal,
      long deltaTriggerOffsetSpan,
      long deltaTriggerDeltaCount,
      int retainCount,
      long persistAwaitTimeoutMs) {
    return newCoordinator(
        watermark,
        fatal,
        new InMemoryBookDeltaBatchStore(),
        deltaTriggerOffsetSpan,
        deltaTriggerDeltaCount,
        retainCount,
        persistAwaitTimeoutMs);
  }

  public static CoordinatorBundle newCoordinator(
      OutboundWatermark watermark,
      FatalFailureHandler fatal,
      BookDeltaBatchStore batchStore,
      long deltaTriggerOffsetSpan,
      long deltaTriggerDeltaCount,
      int retainCount,
      long persistAwaitTimeoutMs) {
    return newCoordinator(
        watermark,
        fatal,
        "match_event",
        0,
        batchStore,
        deltaTriggerOffsetSpan,
        deltaTriggerDeltaCount,
        0.30,
        retainCount,
        persistAwaitTimeoutMs);
  }

  public static CoordinatorBundle newCoordinator(
      OutboundWatermark watermark,
      FatalFailureHandler fatal,
      String matchTopic,
      int matchPartition,
      long deltaTriggerOffsetSpan,
      long deltaTriggerDeltaCount,
      double fullMaxBookFillRatio,
      int retainCount,
      long persistAwaitTimeoutMs) {
    return newCoordinator(
        watermark,
        fatal,
        matchTopic,
        matchPartition,
        new InMemoryBookDeltaBatchStore(),
        deltaTriggerOffsetSpan,
        deltaTriggerDeltaCount,
        fullMaxBookFillRatio,
        retainCount,
        persistAwaitTimeoutMs);
  }

  public static CoordinatorBundle newCoordinator(
      OutboundWatermark watermark,
      FatalFailureHandler fatal,
      String matchTopic,
      int matchPartition,
      BookDeltaBatchStore batchStore,
      long deltaTriggerOffsetSpan,
      long deltaTriggerDeltaCount,
      double fullMaxBookFillRatio,
      int retainCount,
      long persistAwaitTimeoutMs) {
    return newCoordinator(
        watermark,
        fatal,
        matchTopic,
        matchPartition,
        batchStore,
        deltaTriggerOffsetSpan,
        deltaTriggerDeltaCount,
        fullMaxBookFillRatio,
        retainCount,
        persistAwaitTimeoutMs,
        0L);
  }

  public static CoordinatorBundle newCoordinator(
      OutboundWatermark watermark,
      FatalFailureHandler fatal,
      String matchTopic,
      int matchPartition,
      BookDeltaBatchStore batchStore,
      long deltaTriggerOffsetSpan,
      long deltaTriggerDeltaCount,
      double fullMaxBookFillRatio,
      int retainCount,
      long persistAwaitTimeoutMs,
      long everyNOffsets) {
    BookDeltaLog deltaLog = new BookDeltaLog();
    BookDeltaRecorder recorder = new BookDeltaRecorder(deltaLog);
    EngineRegistry registry =
        new EngineRegistry(
            com.kai.matchengine.match.SystemTimeSource.INSTANCE,
            com.kai.matchengine.orderbook.OrderNodePool.DEFAULT_CAPACITY,
            recorder);
    InMemorySnapshotStore store = new InMemorySnapshotStore();
    SnapshotMaterializer materializer = new SnapshotMaterializer();
    SnapshotCoordinator coordinator =
        new SnapshotCoordinator(
            registry,
            store,
            batchStore,
            deltaLog,
            materializer,
            watermark,
            fatal,
            matchTopic,
            matchPartition,
            deltaTriggerOffsetSpan,
            deltaTriggerDeltaCount,
            fullMaxBookFillRatio,
            retainCount,
            persistAwaitTimeoutMs,
            everyNOffsets,
            null);
    return new CoordinatorBundle(
        registry, store, deltaLog, recorder, batchStore, materializer, coordinator);
  }
}
