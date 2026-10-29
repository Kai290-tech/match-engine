package com.kai.matchengine.orderbook;

import java.util.Arrays;

/** 单线程 {@code long orderId → OrderNode} 索引（开放寻址，无装箱）。 */
final class LongOrderMap {

  private static final long EMPTY = 0L;
  private static final long TOMBSTONE = Long.MIN_VALUE;

  private long[] keys = new long[16];
  private OrderNode[] values = new OrderNode[16];
  private int size;

  LongOrderMap() {
    Arrays.fill(keys, EMPTY);
  }

  int size() {
    return size;
  }

  boolean containsKey(long orderId) {
    return get(orderId) != null;
  }

  OrderNode get(long orderId) {
    if (orderId == EMPTY || orderId == TOMBSTONE) {
      return null;
    }
    int mask = keys.length - 1;
    for (int i = hash(orderId) & mask; ; i = (i + 1) & mask) {
      long k = keys[i];
      if (k == EMPTY) {
        return null;
      }
      if (k == orderId) {
        return values[i];
      }
    }
  }

  void put(long orderId, OrderNode node) {
    if (orderId == EMPTY || orderId == TOMBSTONE) {
      throw new IllegalArgumentException("invalid orderId key");
    }
    if (size * 2 >= keys.length) {
      grow();
    }
    int mask = keys.length - 1;
    int idx = hash(orderId) & mask;
    while (true) {
      long k = keys[idx];
      if (k == EMPTY || k == TOMBSTONE) {
        keys[idx] = orderId;
        values[idx] = node;
        size++;
        return;
      }
      if (k == orderId) {
        values[idx] = node;
        return;
      }
      idx = (idx + 1) & mask;
    }
  }

  OrderNode remove(long orderId) {
    if (orderId == EMPTY || orderId == TOMBSTONE) {
      return null;
    }
    int mask = keys.length - 1;
    for (int i = hash(orderId) & mask; ; i = (i + 1) & mask) {
      long k = keys[i];
      if (k == EMPTY) {
        return null;
      }
      if (k == orderId) {
        OrderNode removed = values[i];
        keys[i] = TOMBSTONE;
        values[i] = null;
        size--;
        return removed;
      }
    }
  }

  void clear() {
    Arrays.fill(keys, EMPTY);
    Arrays.fill(values, null);
    size = 0;
  }

  /** 快照 {@link OrderBook#clear()} 等批量操作使用。 */
  long[] copyOrderIds() {
    long[] ids = new long[size];
    int j = 0;
    for (int i = 0; i < keys.length && j < size; i++) {
      long k = keys[i];
      if (k != EMPTY && k != TOMBSTONE) {
        ids[j++] = k;
      }
    }
    return ids;
  }

  private static int hash(long orderId) {
    return (int) (orderId ^ (orderId >>> 32));
  }

  private void grow() {
    long[] oldKeys = keys;
    OrderNode[] oldValues = values;
    keys = new long[oldKeys.length * 2];
    values = new OrderNode[oldKeys.length * 2];
    Arrays.fill(keys, EMPTY);
    size = 0;
    for (int i = 0; i < oldKeys.length; i++) {
      long k = oldKeys[i];
      if (k != EMPTY && k != TOMBSTONE) {
        put(k, oldValues[i]);
      }
    }
  }
}
