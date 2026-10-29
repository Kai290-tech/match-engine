package com.kai.matchengine.outbound;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 出站已成功发出的最大 {@code match_event} offset（单调）。
 *
 * <p>启动可用 {@link #seedBefore(long)} 设为 {@code firstInboundOffset - 1}，以支持从快照恢复后的非 0
 * 起点；之后仍要求连续 {@link #advanceTo(long)}。
 */
public final class OutboundWatermark {

  private final ReentrantLock lock = new ReentrantLock();
  private final Condition advanced = lock.newCondition();
  private long ackedOffset = -1L;
  private boolean seeded;

  public long ackedOffset() {
    lock.lock();
    try {
      return ackedOffset;
    } finally {
      lock.unlock();
    }
  }

  /**
   * 在开始出站前调用一次：令下一次合法推进为 {@code firstInboundOffset}。
   *
   * <p>例如 seek=1000 时调用 {@code seedBefore(1000)}，则 {@code ackedOffset=999}。
   */
  public void seedBefore(long firstInboundOffset) {
    if (firstInboundOffset < 0) {
      throw new IllegalArgumentException("firstInboundOffset must be >= 0");
    }
    lock.lock();
    try {
      if (seeded) {
        throw new IllegalStateException("outbound watermark already seeded");
      }
      ackedOffset = firstInboundOffset - 1;
      seeded = true;
      advanced.signalAll();
    } finally {
      lock.unlock();
    }
  }

  /**
   * 整批 outbound（同一 match_event offset）全部成功后推进。
   *
   * @throws IllegalStateException 若 {@code offset != ackedOffset + 1}（必须连续）
   */
  public void advanceTo(long offset) {
    if (offset < 0) {
      throw new IllegalArgumentException("offset must be >= 0");
    }
    lock.lock();
    try {
      if (offset != ackedOffset + 1) {
        throw new IllegalStateException(
            "outbound watermark must advance contiguously: acked="
                + ackedOffset
                + " next="
                + offset);
      }
      ackedOffset = offset;
      seeded = true;
      advanced.signalAll();
    } finally {
      lock.unlock();
    }
  }

  /** 阻塞直到 {@code ackedOffset >= target}；超时返回 false。 */
  public boolean awaitAtLeast(long target, long time, TimeUnit unit) throws InterruptedException {
    long nanos = unit.toNanos(time);
    lock.lock();
    try {
      while (ackedOffset < target) {
        if (nanos <= 0L) {
          return false;
        }
        nanos = advanced.awaitNanos(nanos);
      }
      return true;
    } finally {
      lock.unlock();
    }
  }
}
