package com.kai.matchengine.orderbook;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

/**
 * 单交易对内存订单簿：有序树价格档 + 档内 FIFO + orderId 索引 + best 缓存。
 *
 * <p>仅提供簿结构操作（入簿、撤单、减量）；撮合状态机由上层驱动。单线程使用。
 */
public final class OrderBook {

  private final String symbol;
  private final OrderNodePool pool;

  /** 买盘：价格从高到低。 */
  private final NavigableMap<Long, PriceLevel> bids =
      new TreeMap<>(Collections.reverseOrder());

  /** 卖盘：价格从低到高。 */
  private final NavigableMap<Long, PriceLevel> asks = new TreeMap<>();

  private final LongOrderMap orders = new LongOrderMap();

  private long bestBid;
  private long bestAsk;
  private boolean hasBestBid;
  private boolean hasBestAsk;

  public OrderBook(String symbol) {
    this(symbol, new OrderNodePool());
  }

  public OrderBook(String symbol, OrderNodePool pool) {
    if (symbol == null || symbol.isBlank()) {
      throw new IllegalArgumentException("symbol required");
    }
    this.symbol = symbol;
    this.pool = Objects.requireNonNull(pool, "pool");
  }

  public OrderNodePool pool() {
    return pool;
  }

  public String symbol() {
    return symbol;
  }

  public int orderCount() {
    return orders.size();
  }

  public int bidLevelCount() {
    return bids.size();
  }

  public int askLevelCount() {
    return asks.size();
  }

  public boolean isEmpty() {
    return orders.size() == 0;
  }

  public boolean contains(long orderId) {
    return orders.containsKey(orderId);
  }

  public OrderNode get(long orderId) {
    return orders.get(orderId);
  }

  /** 是否还能挂新限价剩余。 */
  public boolean hasCapacityForNewOrder() {
    return pool.canAcquire();
  }

  public int availableOrderNodeCount() {
    return pool.availableCount();
  }

  public boolean hasBestBid() {
    return hasBestBid;
  }

  public boolean hasBestAsk() {
    return hasBestAsk;
  }

  /** 最优买价（无买盘时勿调用，先看 {@link #hasBestBid()}）。 */
  public long bestBid() {
    if (!hasBestBid) {
      throw new IllegalStateException("no best bid");
    }
    return bestBid;
  }

  /** 最优卖价（无卖盘时勿调用，先看 {@link #hasBestAsk()}）。 */
  public long bestAsk() {
    if (!hasBestAsk) {
      throw new IllegalStateException("no best ask");
    }
    return bestAsk;
  }

  public OrderNode bestBidHead() {
    return hasBestBid ? bids.get(bestBid).head() : null;
  }

  public OrderNode bestAskHead() {
    return hasBestAsk ? asks.get(bestAsk).head() : null;
  }

  public long bidLevelQty(long priceTick) {
    PriceLevel level = bids.get(priceTick);
    return level == null ? 0L : level.totalQty();
  }

  public long askLevelQty(long priceTick) {
    PriceLevel level = asks.get(priceTick);
    return level == null ? 0L : level.totalQty();
  }

  /**
   * 对手盘上、价格可交叉的数量是否不少于 {@code qty}。不修改簿。
   */
  public boolean canFill(Side takerSide, long limitPrice, long qty) {
    if (qty <= 0) {
      return false;
    }
    long need = qty;
    NavigableMap<Long, PriceLevel> opposite = takerSide == Side.BID ? asks : bids;
    for (PriceLevel level : opposite.values()) {
      long priceTick = level.priceTick();
      if (takerSide == Side.BID) {
        if (priceTick > limitPrice) {
          break;
        }
      } else if (priceTick < limitPrice) {
        break;
      }
      long levelQty = level.totalQty();
      if (levelQty >= need) {
        return true;
      }
      need -= levelQty;
    }
    return false;
  }

  /** 买盘深度，最优价在前。 */
  public List<DepthLevel> bidDepth() {
    return copyDepth(bids);
  }

  /** 卖盘深度，最优价在前。 */
  public List<DepthLevel> askDepth() {
    return copyDepth(asks);
  }

  /**
   * 全部存活挂单：先买盘（优到劣，同价 FIFO），再卖盘（优到劣，同价 FIFO）。
   *
   * <p>用于快照序列化；恢复时应按此顺序 {@link #add}，勿走撮合。
   */
  public List<RestingOrder> restingOrders() {
    List<RestingOrder> list = new ArrayList<>(orders.size());
    appendResting(bids, list);
    appendResting(asks, list);
    return List.copyOf(list);
  }

  /**
   * 限价剩余量入簿（不做撮合）。
   *
   * @throws IllegalArgumentException qty/side 非法或 orderId 已存在
   * @throws IllegalStateException 对象池耗尽
   */
  public OrderNode add(long orderId, long userId, Side side, long priceTick, long qty) {
    Objects.requireNonNull(side, "side");
    if (qty <= 0) {
      throw new IllegalArgumentException("qty must be positive");
    }
    if (orders.containsKey(orderId)) {
      throw new IllegalArgumentException("duplicate orderId: " + orderId);
    }
    OrderNode node = pool.acquire(orderId, userId, side, priceTick, qty);
    if (node == null) {
      throw new IllegalStateException("order node pool exhausted");
    }

    PriceLevel level = levelForAdd(side, priceTick);
    level.append(node);
    orders.put(orderId, node);
    touchBestOnAdd(side, priceTick);
    return node;
  }

  /**
   * 按 orderId 撤单并摘链。
   *
   * @return 被撤销的节点；不存在则 null
   */
  public OrderNode cancel(long orderId) {
    OrderNode node = orders.remove(orderId);
    if (node == null) {
      return null;
    }
    removeFromBook(node);
    pool.release(node);
    return node;
  }

  /**
   * 对已挂订单减量（撮合成交路径）。减至 0 则从簿中移除。
   *
   * @return 实际扣减数量
   */
  public long reduce(OrderNode node, long qty) {
    if (node == null || !node.isLinked()) {
      throw new IllegalArgumentException("order not on book");
    }
    if (qty <= 0) {
      throw new IllegalArgumentException("qty must be positive");
    }
    long fill = Math.min(qty, node.qty());
    if (fill == node.qty()) {
      orders.remove(node.orderId());
      removeFromBook(node);
      node.setQty(0L);
      pool.release(node);
    } else {
      node.level.decreaseQty(node, fill);
    }
    return fill;
  }

  /**
   * 从最优档队头成交。
   *
   * @param side 簿的一侧（BID 或 ASK）
   * @param qty 想要吃掉的数量
   * @return 实际成交数量；无盘返回 0
   */
  public long fillBestHead(Side side, long qty) {
    OrderNode head = side == Side.BID ? bestBidHead() : bestAskHead();
    if (head == null || qty <= 0) {
      return 0L;
    }
    return reduce(head, qty);
  }

  /** 清空簿（测试或重建前）：走 {@link #cancel} 以保证节点终态归还池。 */
  public void clear() {
    long[] ids = orders.copyOrderIds();
    for (long orderId : ids) {
      cancel(orderId);
    }
  }

  private static List<DepthLevel> copyDepth(NavigableMap<Long, PriceLevel> book) {
    List<DepthLevel> levels = new ArrayList<>(book.size());
    for (PriceLevel level : book.values()) {
      levels.add(new DepthLevel(level.priceTick(), level.totalQty(), level.orderCount()));
    }
    return List.copyOf(levels);
  }

  private static void appendResting(
      NavigableMap<Long, PriceLevel> book, List<RestingOrder> out) {
    for (PriceLevel level : book.values()) {
      level.forEachOrder(
          n ->
              out.add(
                  new RestingOrder(
                      n.orderId(), n.userId(), n.side(), n.priceTick(), n.qty())));
    }
  }

  private PriceLevel levelForAdd(Side side, long priceTick) {
    NavigableMap<Long, PriceLevel> book = side == Side.BID ? bids : asks;
    PriceLevel level = book.get(priceTick);
    if (level == null) {
      level = new PriceLevel(priceTick);
      book.put(priceTick, level);
    }
    return level;
  }

  private void removeFromBook(OrderNode node) {
    PriceLevel level = node.level;
    Side side = node.side();
    long priceTick = node.priceTick();
    level.unlink(node);
    if (level.isEmpty()) {
      NavigableMap<Long, PriceLevel> book = side == Side.BID ? bids : asks;
      book.remove(priceTick);
      refreshBestAfterLevelRemoved(side, priceTick);
    }
  }

  private void touchBestOnAdd(Side side, long priceTick) {
    if (side == Side.BID) {
      if (!hasBestBid || priceTick > bestBid) {
        bestBid = priceTick;
        hasBestBid = true;
      }
    } else {
      if (!hasBestAsk || priceTick < bestAsk) {
        bestAsk = priceTick;
        hasBestAsk = true;
      }
    }
  }

  private void refreshBestAfterLevelRemoved(Side side, long removedPrice) {
    if (side == Side.BID) {
      if (hasBestBid && removedPrice == bestBid) {
        if (bids.isEmpty()) {
          hasBestBid = false;
        } else {
          bestBid = bids.firstKey();
        }
      }
    } else {
      if (hasBestAsk && removedPrice == bestAsk) {
        if (asks.isEmpty()) {
          hasBestAsk = false;
        } else {
          bestAsk = asks.firstKey();
        }
      }
    }
  }
}
