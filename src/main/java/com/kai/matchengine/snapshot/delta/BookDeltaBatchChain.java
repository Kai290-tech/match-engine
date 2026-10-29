package com.kai.matchengine.snapshot.delta;

import com.kai.matchengine.snapshot.delta.BookDeltaBatchStore.StoredBatch;
import java.util.List;
import java.util.Objects;

/** 增量 batch 链校验与 offset 边界计算。 */
public final class BookDeltaBatchChain {

  private BookDeltaBatchChain() {}

  public static List<StoredBatch> loadValidatedChain(
      BookDeltaBatchStore batchStore,
      BookDeltaCodec deltaCodec,
      long baseSnapshotId,
      String matchTopic,
      int matchPartition,
      long fullOffset) {
    Objects.requireNonNull(batchStore, "batchStore");
    Objects.requireNonNull(deltaCodec, "deltaCodec");
    List<StoredBatch> chain =
        batchStore.loadChain(baseSnapshotId, matchTopic, matchPartition);
    validateChain(chain, deltaCodec, fullOffset);
    return chain;
  }

  public static void validateChain(
      List<StoredBatch> chain, BookDeltaCodec deltaCodec, long fullOffset) {
    if (chain.isEmpty()) {
      return;
    }
    long expectedFrom = fullOffset + 1;
    if (fullOffset < 0) {
      expectedFrom = 0L;
    }
    for (StoredBatch batch : chain) {
      if (batch.fromOffset() != expectedFrom) {
        throw new IllegalStateException(
            "delta batch chain gap expectedFrom="
                + expectedFrom
                + " got="
                + batch.fromOffset()
                + " batchId="
                + batch.id());
      }
      if (batch.toOffset() < batch.fromOffset()) {
        throw new IllegalStateException(
            "delta batch invalid range batchId=" + batch.id() + " from=" + batch.fromOffset());
      }
      int decoded = 0;
      for (BookDeltaBatchStore.Section section : batch.sections()) {
        List<BookDelta> deltas = deltaCodec.decode(section.payload());
        for (BookDelta delta : deltas) {
          if (delta.matchOffset() >= 0
              && (delta.matchOffset() < batch.fromOffset()
                  || delta.matchOffset() > batch.toOffset())) {
            throw new IllegalStateException(
                "delta matchOffset out of batch range batchId="
                    + batch.id()
                    + " offset="
                    + delta.matchOffset());
          }
        }
        decoded += deltas.size();
      }
      if (decoded != batch.deltaCount()) {
        throw new IllegalStateException(
            "delta batch count mismatch batchId="
                + batch.id()
                + " header="
                + batch.deltaCount()
                + " decoded="
                + decoded);
      }
      expectedFrom = batch.toOffset() + 1;
    }
  }

  /** 链为空时返回 {@code fullOffset}。 */
  public static long chainEndOffset(List<StoredBatch> chain, long fullOffset) {
    if (chain.isEmpty()) {
      return fullOffset;
    }
    return chain.get(chain.size() - 1).toOffset();
  }

  public static long maxAppliedOffset(
      BookDeltaBatchStore batchStore,
      BookDeltaCodec deltaCodec,
      long baseSnapshotId,
      String matchTopic,
      int matchPartition,
      long fullOffset) {
    List<StoredBatch> chain =
        batchStore.loadChain(baseSnapshotId, matchTopic, matchPartition);
    validateChain(chain, deltaCodec, fullOffset);
    return chainEndOffset(chain, fullOffset);
  }

  public static void replayToMaterializer(
      SnapshotMaterializer materializer, BookDeltaCodec deltaCodec, List<StoredBatch> chain) {
    Objects.requireNonNull(materializer, "materializer");
    for (StoredBatch batch : chain) {
      for (BookDeltaBatchStore.Section section : batch.sections()) {
        for (BookDelta delta : deltaCodec.decode(section.payload())) {
          materializer.apply(delta);
        }
      }
    }
  }
}
