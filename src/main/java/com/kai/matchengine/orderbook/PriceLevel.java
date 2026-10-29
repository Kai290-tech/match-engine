package com.kai.matchengine.orderbook;

/**
 * 同一价格档：FIFO 队列（侵入式双向链表）。
 *
 * <p>入队、队头出队、任意节点摘链均为 O(1)。
 */
final class PriceLevel {

  private final long priceTick;
  private long totalQty;
  private int orderCount;
  private OrderNode head;
  private OrderNode tail;

  PriceLevel(long priceTick) {
    this.priceTick = priceTick;
  }

  long priceTick() {
    return priceTick;
  }

  long totalQty() {
    return totalQty;
  }

  int orderCount() {
    return orderCount;
  }

  OrderNode head() {
    return head;
  }

  boolean isEmpty() {
    return head == null;
  }

  void append(OrderNode node) {
    node.level = this;
    node.prev = tail;
    node.next = null;
    if (tail != null) {
      tail.next = node;
    } else {
      head = node;
    }
    tail = node;
    totalQty += node.qty();
    orderCount++;
  }

  void unlink(OrderNode node) {
    if (node.level != this) {
      throw new IllegalStateException("node not on this level");
    }
    OrderNode prev = node.prev;
    OrderNode next = node.next;
    if (prev != null) {
      prev.next = next;
    } else {
      head = next;
    }
    if (next != null) {
      next.prev = prev;
    } else {
      tail = prev;
    }
    totalQty -= node.qty();
    orderCount--;
    node.prev = null;
    node.next = null;
    node.level = null;
  }

  /** 节点仍在档内时减量，同步档总量。 */
  void decreaseQty(OrderNode node, long delta) {
    if (node.level != this) {
      throw new IllegalStateException("node not on this level");
    }
    if (delta <= 0 || delta > node.qty()) {
      throw new IllegalArgumentException("invalid qty decrease");
    }
    node.setQty(node.qty() - delta);
    totalQty -= delta;
  }

  void forEachOrder(java.util.function.Consumer<OrderNode> consumer) {
    for (OrderNode n = head; n != null; n = n.next) {
      consumer.accept(n);
    }
  }
}
