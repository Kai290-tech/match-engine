package com.kai.matchengine.match;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 最近 {@code capacity} 个 orderId 的滑动窗口（FIFO），用于 Place 幂等。
 *
 * <p>单线程使用；窗口内容进入快照并在恢复时回放。
 */
public final class RecentOrderIdWindow {

  public static final int DEFAULT_CAPACITY = 10_000;

  private final int capacity;
  private final long[] ring;
  private final Set<Long> set;
  private int head;
  private int size;

  public RecentOrderIdWindow() {
    this(DEFAULT_CAPACITY);
  }

  public RecentOrderIdWindow(int capacity) {
    if (capacity < 1) {
      throw new IllegalArgumentException("capacity must be >= 1");
    }
    this.capacity = capacity;
    this.ring = new long[capacity];
    this.set = new HashSet<>(capacity * 2);
  }

  public boolean contains(long orderId) {
    return set.contains(orderId);
  }

  /** @return true 表示新记入；false 表示已存在未写入 */
  public boolean remember(long orderId) {
    if (set.contains(orderId)) {
      return false;
    }
    if (size == capacity) {
      long evicted = ring[head];
      set.remove(evicted);
      ring[head] = orderId;
      head = (head + 1) % capacity;
    } else {
      int idx = (head + size) % capacity;
      ring[idx] = orderId;
      size++;
    }
    set.add(orderId);
    return true;
  }

  /** 快照导出：从最旧到最新。 */
  public List<Long> snapshotOldestFirst() {
    List<Long> out = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      out.add(ring[(head + i) % capacity]);
    }
    return List.copyOf(out);
  }

  /** 恢复：按最旧→最新顺序 remember。 */
  public void restoreOldestFirst(List<Long> orderIds) {
    clear();
    if (orderIds == null || orderIds.isEmpty()) {
      return;
    }
    int from = Math.max(0, orderIds.size() - capacity);
    for (int i = from; i < orderIds.size(); i++) {
      remember(orderIds.get(i));
    }
  }

  public void clear() {
    set.clear();
    head = 0;
    size = 0;
  }

  public int size() {
    return size;
  }

  public int capacity() {
    return capacity;
  }
}
