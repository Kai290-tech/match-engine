package com.kai.matchengine.orderbook;

/**
 * 按 symbol 有界的 {@link OrderNode} 对象池（lazy grow 至 capacity）。
 *
 * <p>单线程使用。
 */
public final class OrderNodePool {

  public static final int DEFAULT_CAPACITY = 300_000;

  private final int capacity;
  private final OrderNode[] freeStack;
  private int freeTop;
  private int allocated;

  public OrderNodePool() {
    this(DEFAULT_CAPACITY);
  }

  public OrderNodePool(int capacity) {
    if (capacity < 1) {
      throw new IllegalArgumentException("capacity must be >= 1");
    }
    this.capacity = capacity;
    this.freeStack = new OrderNode[capacity];
  }

  public int capacity() {
    return capacity;
  }

  public int allocatedCount() {
    return allocated;
  }

  public int freeCount() {
    return freeTop;
  }

  /** 是否还能为新挂单分配节点。 */
  public boolean canAcquire() {
    return availableCount() > 0;
  }

  /** 当前可立即借出的节点数（= capacity - 在簿检出数）。 */
  public int availableCount() {
    return capacity - inUseCount();
  }

  /**
   * 取节点并初始化字段；池耗尽返回 null。
   *
   * <p>调用方负责将节点挂入 {@link OrderBook}。
   */
  public OrderNode acquire(long orderId, long userId, Side side, long priceTick, long qty) {
    OrderNode node;
    if (freeTop > 0) {
      node = freeStack[--freeTop];
      node.reinit(orderId, userId, side, priceTick, qty);
    } else if (allocated < capacity) {
      node = new OrderNode(orderId, userId, side, priceTick, qty);
      allocated++;
    } else {
      return null;
    }
    return node;
  }

  /** 订单离开簿后归还（已摘链、终态）。 */
  public void release(OrderNode node) {
    if (node == null) {
      return;
    }
    if (node.isLinked()) {
      throw new IllegalStateException(
          "cannot release linked order node orderId=" + node.orderId());
    }
    if (freeTop >= capacity) {
      throw new IllegalStateException(
          "order node free stack overflow capacity=" + capacity);
    }
    node.clearLinks();
    freeStack[freeTop++] = node;
  }

  /** 当前检出在簿上的节点数（= allocated - freeTop）。 */
  public int inUseCount() {
    return allocated - freeTop;
  }
}
