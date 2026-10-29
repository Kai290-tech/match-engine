package com.kai.matchengine.snapshot.delta;

import java.util.ArrayDeque;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 撮合线程 → snapshot 线程的增量队列（非阻塞 append）。
 *
 * <p>ring 满时写入 spill；spill 满时丢弃并 {@link #markGap()}，不阻塞撮合。
 */
public final class BookDeltaLog {

  private static final Logger log = LoggerFactory.getLogger(BookDeltaLog.class);

  public static final int DEFAULT_RING_CAPACITY = 20_000;
  public static final int DEFAULT_SPILL_CAPACITY = 20_000;

  private final BookDelta[] ring;
  private final int ringCapacity;
  private final ArrayDeque<BookDelta> spill;
  private final int spillCapacity;

  private int ringHead;
  private int ringTail;
  private int ringSize;

  private volatile boolean gapActive;
  private final AtomicLong gapDroppedCount = new AtomicLong();
  private final AtomicLong appendedCount = new AtomicLong();
  private final AtomicLong consumedCount = new AtomicLong();

  private volatile long lastWarnDepthRing = -1L;

  public BookDeltaLog() {
    this(DEFAULT_RING_CAPACITY, DEFAULT_SPILL_CAPACITY);
  }

  public BookDeltaLog(int ringCapacity, int spillCapacity) {
    if (ringCapacity < 1) {
      throw new IllegalArgumentException("ringCapacity must be >= 1");
    }
    if (spillCapacity < 0) {
      throw new IllegalArgumentException("spillCapacity must be >= 0");
    }
    this.ringCapacity = ringCapacity;
    this.ring = new BookDelta[ringCapacity];
    this.spillCapacity = spillCapacity;
    this.spill = spillCapacity > 0 ? new ArrayDeque<>(Math.min(spillCapacity, 1024)) : null;
  }

  public int ringCapacity() {
    return ringCapacity;
  }

  public int spillCapacity() {
    return spillCapacity;
  }

  /** 待消费条数（ring + spill）。 */
  public int pendingCount() {
    return ringSize + spillSize();
  }

  public boolean gapActive() {
    return gapActive;
  }

  public long gapDroppedCount() {
    return gapDroppedCount.get();
  }

  public long appendedCount() {
    return appendedCount.get();
  }

  public long consumedCount() {
    return consumedCount.get();
  }

  /**
   * 撮合线程调用：永不阻塞。
   *
   * @return 写入位置；{@link BookDeltaAppendResult#GAP_DROPPED} 时本条未入队
   */
  public BookDeltaAppendResult tryAppend(BookDelta delta) {
    Objects.requireNonNull(delta, "delta");
    appendedCount.incrementAndGet();
    maybeWarnDepth(ringSize + 1, ringCapacity, "ring");

    if (ringSize < ringCapacity) {
      ring[ringTail] = delta;
      ringTail = (ringTail + 1) % ringCapacity;
      ringSize++;
      return BookDeltaAppendResult.RING;
    }

    if (spill != null && spill.size() < spillCapacity) {
      spill.addLast(delta);
      maybeWarnDepth(spill.size(), spillCapacity, "spill");
      return BookDeltaAppendResult.SPILL;
    }

    gapDroppedCount.incrementAndGet();
    gapActive = true;
    if (log.isWarnEnabled()) {
      log.warn(
          "book delta log overflow ring={} spill={} dropped offset={} symbolId={} type={}",
          ringCapacity,
          spillCapacity,
          delta.matchOffset(),
          delta.symbolId(),
          delta.type());
    }
    return BookDeltaAppendResult.GAP_DROPPED;
  }

  /** snapshot 线程：取出一条；无则 null。 */
  public BookDelta poll() {
    BookDelta delta;
    if (ringSize > 0) {
      delta = ring[ringHead];
      ring[ringHead] = null;
      ringHead = (ringHead + 1) % ringCapacity;
      ringSize--;
    } else if (spill != null && !spill.isEmpty()) {
      delta = spill.pollFirst();
    } else {
      return null;
    }
    consumedCount.incrementAndGet();
    return delta;
  }

  public boolean hasPending() {
    return pendingCount() > 0;
  }

  /** 增量 batch 成功落库后可清除 GAP 标记（由 coordinator 调用）。 */
  public void clearGap() {
    gapActive = false;
  }

  public void markGap() {
    gapActive = true;
  }

  private int spillSize() {
    return spill == null ? 0 : spill.size();
  }

  private void maybeWarnDepth(int depth, int capacity, String region) {
    if (capacity < 4) {
      return;
    }
    int pct = (int) ((depth * 100L) / capacity);
    int level = 0;
    if (pct >= 90) {
      level = 90;
    } else if (pct >= 75) {
      level = 75;
    } else if (pct >= 50) {
      level = 50;
    }
    if (level == 0) {
      return;
    }
    long warnKey = ((long) level << 32) | region.hashCode();
    if (lastWarnDepthRing == warnKey) {
      return;
    }
    lastWarnDepthRing = warnKey;
    log.warn(
        "book delta {} depth {}% ({}/{})",
        region,
        level,
        depth,
        capacity);
  }
}
