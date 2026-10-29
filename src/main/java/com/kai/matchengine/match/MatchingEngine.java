package com.kai.matchengine.match;

import com.kai.matchengine.event.inbound.CancelOrder;
import com.kai.matchengine.event.inbound.MatchEvent;
import com.kai.matchengine.event.inbound.OrderType;
import com.kai.matchengine.event.inbound.PlaceOrder;
import com.kai.matchengine.event.inbound.QtyUnit;
import com.kai.matchengine.event.outbound.BookSnapshot;
import com.kai.matchengine.event.outbound.BookUpdate;
import com.kai.matchengine.event.outbound.CommandResult;
import com.kai.matchengine.event.outbound.OrderUpdate;
import com.kai.matchengine.event.outbound.OrderUpdateType;
import com.kai.matchengine.event.outbound.OutboundEvent;
import com.kai.matchengine.event.outbound.RejectReason;
import com.kai.matchengine.event.outbound.Trade;
import com.kai.matchengine.orderbook.DepthLevel;
import com.kai.matchengine.orderbook.OrderBook;
import com.kai.matchengine.orderbook.OrderNode;
import com.kai.matchengine.orderbook.OrderNodePool;
import com.kai.matchengine.orderbook.RestingOrder;
import com.kai.matchengine.orderbook.Side;
import com.kai.matchengine.snapshot.delta.BookDelta;
import com.kai.matchengine.snapshot.delta.BookDeltaSink;
import com.kai.matchengine.snapshot.delta.MatchDeltaContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 单交易对撮合状态机（单线程）。
 *
 * <p>对外只通过 {@link #apply(MatchEvent)} 处理入站事件；{@link #exportBookSnapshot()} 导出全量簿；
 * {@link #restoreSequences} / {@link #restoreRestingOrder} 用于快照恢复。
 */
public final class MatchingEngine {

  private final OrderBook book;
  private final TimeSource timeSource;
  private final int symbolId;
  private final BookDeltaSink bookDeltaSink;
  private final RecentOrderIdWindow recentOrderIds = new RecentOrderIdWindow();
  private final List<OutboundEvent> outboundScratch = new ArrayList<>(16);

  private long eventId;
  private long bookVersion;

  public MatchingEngine(String symbol) {
    this(symbol, SystemTimeSource.INSTANCE);
  }

  public MatchingEngine(String symbol, TimeSource timeSource) {
    this(symbol, 0, timeSource, OrderNodePool.DEFAULT_CAPACITY, null);
  }

  public MatchingEngine(String symbol, TimeSource timeSource, int orderNodePoolCapacity) {
    this(symbol, 0, timeSource, orderNodePoolCapacity, null);
  }

  public MatchingEngine(
      String symbol,
      int symbolId,
      TimeSource timeSource,
      int orderNodePoolCapacity,
      BookDeltaSink bookDeltaSink) {
    this.book =
        new OrderBook(
            Objects.requireNonNull(symbol, "symbol"),
            new OrderNodePool(orderNodePoolCapacity));
    this.symbolId = symbolId;
    this.timeSource = Objects.requireNonNull(timeSource, "timeSource");
    this.bookDeltaSink = bookDeltaSink;
  }

  public MatchingEngine(OrderBook book, TimeSource timeSource) {
    this(book, timeSource, 0, null);
  }

  public MatchingEngine(OrderBook book, TimeSource timeSource, int symbolId, BookDeltaSink bookDeltaSink) {
    this.book = Objects.requireNonNull(book, "book");
    this.symbolId = symbolId;
    this.timeSource = Objects.requireNonNull(timeSource, "timeSource");
    this.bookDeltaSink = bookDeltaSink;
  }

  public int symbolId() {
    return symbolId;
  }

  public OrderBook book() {
    return book;
  }

  public String symbol() {
    return book.symbol();
  }

  public long lastEventId() {
    return eventId;
  }

  public long lastBookVersion() {
    return bookVersion;
  }

  /**
   * 统一入站入口：每条事件处理结束必有一条 {@link CommandResult}。
   */
  public List<OutboundEvent> apply(MatchEvent event) {
    Objects.requireNonNull(event, "event");
    return switch (event) {
      case PlaceOrder place -> place(place);
      case CancelOrder cancel -> cancel(cancel);
      default ->
          throw new IllegalArgumentException(
              "MatchingEngine only accepts Place/Cancel, got " + event.getClass().getSimpleName());
    };
  }

  /**
   * 导出当前全量订单簿（不推进 bookVersion）。
   */
  public BookSnapshot exportBookSnapshot() {
    return new BookSnapshot(
        bookVersion,
        now(),
        book.symbol(),
        toSnapshotLevels(book.bidDepth()),
        toSnapshotLevels(book.askDepth()));
  }

  /**
   * 快照恢复：恢复对外序列水位（应为快照中的 last 值，之后新事件继续递增）。
   */
  public void restoreSequences(long lastEventId, long lastBookVersion) {
    if (lastEventId < 0 || lastBookVersion < 0) {
      throw new IllegalArgumentException("sequence must be >= 0");
    }
    this.eventId = lastEventId;
    this.bookVersion = lastBookVersion;
  }

  /**
   * 快照恢复：直接入簿，不撮合、不发事件。应按 {@link OrderBook#restingOrders()} 的顺序回放。
   *
   * <p>调用前应清空簿（{@link #clearBook()}），避免与旧状态叠加。
   */
  public void restoreRestingOrder(long orderId, long userId, Side side, long priceTick, long qty) {
    book.add(orderId, userId, side, priceTick, qty);
  }

  /** 清空订单簿并将节点终态归还对象池。 */
  public void clearBook() {
    book.clear();
  }

  /** 当前存活挂单（快照内容）。 */
  public List<RestingOrder> restingOrders() {
    return book.restingOrders();
  }

  /** 最近 orderId 窗口（最旧→最新），进入快照。 */
  public List<Long> recentOrderIdsOldestFirst() {
    return recentOrderIds.snapshotOldestFirst();
  }

  /** 快照恢复：回放 orderId 幂等窗口。 */
  public void restoreRecentOrderIds(List<Long> orderIds) {
    recentOrderIds.restoreOldestFirst(orderIds);
  }

  /** 增量 batch 回放：单条 recent orderId。 */
  public void replayRecentOrder(long orderId) {
    recentOrderIds.remember(orderId);
  }

  private List<OutboundEvent> place(PlaceOrder cmd) {
    outboundScratch.clear();
    List<OutboundEvent> out = outboundScratch;
    String symbol = book.symbol();

    RejectReason reject = validatePlace(cmd);
    if (reject != null) {
      out.add(
          CommandResult.Place.reject(
              cmd.clientRequestId(), now(), symbol, cmd.orderId(), reject));
      return List.copyOf(out);
    }

    // 限价单入撮合前须有空闲池节点；单线程下通过即可保证后续剩余入簿不缺对象。
    if (cmd.orderType() == OrderType.LIMIT && !book.hasCapacityForNewOrder()) {
      out.add(
          CommandResult.Place.reject(
              cmd.clientRequestId(), now(), symbol, cmd.orderId(), RejectReason.BOOK_FULL));
      return List.copyOf(out);
    }

    if (cmd.orderType() == OrderType.FOK
        && !book.canFillBeforeSelf(cmd.side(), cmd.userId(), cmd.priceTick(), cmd.qty())) {
      cancelMarketRemainder(cmd, 0L, out);
      rememberPlace(cmd);
      out.add(
          CommandResult.Place.ok(
              cmd.clientRequestId(), now(), symbol, cmd.orderId(), 0L, 0L, false));
      return List.copyOf(out);
    }

    if (cmd.orderType() == OrderType.POST_ONLY) {
      if (crosses(cmd)) {
        cancelMarketRemainder(cmd, 0L, out);
        rememberPlace(cmd);
        out.add(
            CommandResult.Place.ok(
                cmd.clientRequestId(), now(), symbol, cmd.orderId(), 0L, 0L, false));
        return List.copyOf(out);
      }
      if (!book.hasCapacityForNewOrder()) {
        out.add(
            CommandResult.Place.reject(
                cmd.clientRequestId(), now(), symbol, cmd.orderId(), RejectReason.BOOK_FULL));
        return List.copyOf(out);
      }
      restLimit(cmd, cmd.qty(), 0L, out);
      rememberPlace(cmd);
      out.add(
          CommandResult.Place.ok(
              cmd.clientRequestId(), now(), symbol, cmd.orderId(), 0L, cmd.qty(), true));
      return List.copyOf(out);
    }

    long remaining = cmd.qty();
    long filled = 0L;
    // 只有市价买单、且数量单位为计价预算时，才按预算截断。
    boolean quoteBudget = isQuoteBudget(cmd);
    boolean stoppedForSelf = false;

    while (remaining > 0 && crosses(cmd)) {
      OrderNode maker = oppositeHead(cmd.side());
      if (maker.userId() == cmd.userId()) {
        stoppedForSelf = true;
        break;
      }
      if (quoteBudget) {
        long makerPrice = maker.priceTick();
        if (makerPrice <= 0L || remaining < makerPrice) {
          break;
        }
        long fillQty = matchOne(cmd, remaining / makerPrice, out);
        filled += fillQty;
        remaining -= makerPrice * fillQty;
      } else {
        filled += matchOne(cmd, remaining, out);
        remaining = cmd.qty() - filled;
      }
    }

    boolean resting = false;
    if (remaining > 0) {
      if (cmd.orderType() == OrderType.LIMIT && !stoppedForSelf) {
        restLimit(cmd, remaining, filled, out);
        resting = true;
      } else {
        // 市价、IOC，以及碰到自己的挂单后停下的限价：剩余不入簿。FOK 在成交前已整单取消。
        cancelMarketRemainder(cmd, filled, out);
        remaining = 0L;
      }
    } else if (filled > 0) {
      emitTakerFilled(cmd, filled, out);
    }

    rememberPlace(cmd);

    out.add(
        CommandResult.Place.ok(
            cmd.clientRequestId(),
            now(),
            symbol,
            cmd.orderId(),
            filled,
            resting ? remaining : 0L,
            resting));
    return List.copyOf(out);
  }

  private List<OutboundEvent> cancel(CancelOrder cmd) {
    outboundScratch.clear();
    List<OutboundEvent> out = outboundScratch;
    String symbol = book.symbol();

    OrderNode node = book.get(cmd.orderId());
    if (node == null) {
      out.add(
          CommandResult.Cancel.reject(
              cmd.clientRequestId(),
              now(),
              symbol,
              cmd.orderId(),
              RejectReason.ORDER_NOT_FOUND));
      return List.copyOf(out);
    }
    if (node.userId() != cmd.userId()) {
      out.add(
          CommandResult.Cancel.reject(
              cmd.clientRequestId(),
              now(),
              symbol,
              cmd.orderId(),
              RejectReason.USER_MISMATCH));
      return List.copyOf(out);
    }

    long eventTime = now();
    Side side = node.side();
    long priceTick = node.priceTick();
    long orderId = node.orderId();
    long userId = node.userId();
    book.cancel(orderId);

    long removedEventId = nextEventId();
    recordDelta(
        BookDelta.restRemove(
            deltaOffset(), symbolId, orderId, removedEventId, bookVersion));

    out.add(
        new OrderUpdate(
            removedEventId,
            eventTime,
            symbol,
            orderId,
            userId,
            side,
            priceTick,
            0L,
            0L,
            OrderUpdateType.CANCELLED));
    emitBookUpdate(out, eventTime, side, priceTick);
    out.add(CommandResult.Cancel.ok(cmd.clientRequestId(), now(), symbol, orderId));
    return List.copyOf(out);
  }

  private RejectReason validatePlace(PlaceOrder cmd) {
    if (cmd.side() == null || cmd.orderType() == null) {
      throw new NullPointerException("side/orderType required");
    }
    if (cmd.qty() <= 0) {
      return RejectReason.INVALID_QTY;
    }
    if (cmd.qtyUnit() == QtyUnit.QUOTE && !isQuoteBudget(cmd)) {
      return RejectReason.INVALID_QTY;
    }
    if (requiresPrice(cmd.orderType()) && cmd.priceTick() <= 0) {
      return RejectReason.INVALID_PRICE;
    }
    if (book.contains(cmd.orderId()) || recentOrderIds.contains(cmd.orderId())) {
      return RejectReason.DUPLICATE_ORDER_ID;
    }
    return null;
  }

  /** @return 本笔成交数量 */
  private long matchOne(PlaceOrder taker, long remaining, List<OutboundEvent> out) {
    OrderNode maker = oppositeHead(taker.side());
    long fillQty = Math.min(remaining, maker.qty());
    long tradePrice = maker.priceTick();
    Side makerSide = maker.side();
    long makerOrderId = maker.orderId();
    long makerUserId = maker.userId();
    long makerRemainingBefore = maker.qty();
    long eventTime = now();
    String symbol = book.symbol();

    book.reduce(maker, fillQty);
    long makerRemaining = makerRemainingBefore - fillQty;

    long tradeEventId = nextEventId();
    long makerUpdateEventId = nextEventId();
    if (makerRemaining == 0) {
      recordDelta(
          BookDelta.restRemove(
              deltaOffset(), symbolId, makerOrderId, makerUpdateEventId, bookVersion));
    } else {
      recordDelta(
          BookDelta.restQty(
              deltaOffset(),
              symbolId,
              makerOrderId,
              makerRemaining,
              makerUpdateEventId,
              bookVersion));
    }

    out.add(
        new Trade(
            tradeEventId,
            eventTime,
            symbol,
            taker.orderId(),
            taker.userId(),
            makerOrderId,
            makerUserId,
            tradePrice,
            fillQty,
            taker.side()));

    out.add(
        new OrderUpdate(
            makerUpdateEventId,
            eventTime,
            symbol,
            makerOrderId,
            makerUserId,
            makerSide,
            tradePrice,
            makerRemaining,
            fillQty,
            makerRemaining == 0 ? OrderUpdateType.FILLED : OrderUpdateType.PARTIALLY_FILLED));

    emitBookUpdate(out, eventTime, makerSide, tradePrice);
    return fillQty;
  }

  private void restLimit(PlaceOrder cmd, long remaining, long filled, List<OutboundEvent> out) {
    long eventTime = now();
    String symbol = book.symbol();
    book.add(cmd.orderId(), cmd.userId(), cmd.side(), cmd.priceTick(), remaining);
    long restingEventId = nextEventId();
    recordDelta(
        BookDelta.restAdd(
            deltaOffset(),
            symbolId,
            cmd.orderId(),
            cmd.userId(),
            cmd.side(),
            cmd.priceTick(),
            remaining,
            restingEventId,
            bookVersion));
    out.add(
        new OrderUpdate(
            restingEventId,
            eventTime,
            symbol,
            cmd.orderId(),
            cmd.userId(),
            cmd.side(),
            cmd.priceTick(),
            remaining,
            filled,
            OrderUpdateType.RESTING));
    emitBookUpdate(out, eventTime, cmd.side(), cmd.priceTick());
  }

  private void cancelMarketRemainder(PlaceOrder cmd, long filled, List<OutboundEvent> out) {
    long eventTime = now();
    out.add(
        new OrderUpdate(
            nextEventId(),
            eventTime,
            book.symbol(),
            cmd.orderId(),
            cmd.userId(),
            cmd.side(),
            0L,
            0L,
            filled,
            OrderUpdateType.CANCELLED_REMAINDER));
  }

  private void emitTakerFilled(PlaceOrder cmd, long filled, List<OutboundEvent> out) {
    long eventTime = now();
    out.add(
        new OrderUpdate(
            nextEventId(),
            eventTime,
            book.symbol(),
            cmd.orderId(),
            cmd.userId(),
            cmd.side(),
            cmd.orderType() == OrderType.MARKET ? 0L : cmd.priceTick(),
            0L,
            filled,
            OrderUpdateType.FILLED));
  }

  private void emitBookUpdate(List<OutboundEvent> out, long eventTime, Side side, long priceTick) {
    bookVersion++;
    long levelQty =
        side == Side.BID ? book.bidLevelQty(priceTick) : book.askLevelQty(priceTick);
    out.add(new BookUpdate(bookVersion, eventTime, book.symbol(), side, priceTick, levelQty));
  }

  private void rememberPlace(PlaceOrder cmd) {
    recentOrderIds.remember(cmd.orderId());
    recordDelta(BookDelta.metaRecentOrder(deltaOffset(), symbolId, cmd.orderId()));
  }

  /** 市价买单且数量单位为计价预算。校验拒绝发生在进入该分支之前，这里只看已接受的命令。 */
  private static boolean isQuoteBudget(PlaceOrder cmd) {
    return cmd.orderType() == OrderType.MARKET
        && cmd.side() == Side.BID
        && cmd.qtyUnit() == QtyUnit.QUOTE;
  }

  /** LIMIT、IOC 与 FOK 必须带正价格；MARKET 忽略价格。 */
  private static boolean requiresPrice(OrderType orderType) {
    return orderType == OrderType.LIMIT
        || orderType == OrderType.IOC
        || orderType == OrderType.FOK
        || orderType == OrderType.POST_ONLY;
  }

  private boolean crosses(PlaceOrder cmd) {
    if (cmd.side() == Side.BID) {
      if (!book.hasBestAsk()) {
        return false;
      }
      if (cmd.orderType() == OrderType.MARKET) {
        return true;
      }
      return book.bestAsk() <= cmd.priceTick();
    }
    if (!book.hasBestBid()) {
      return false;
    }
    if (cmd.orderType() == OrderType.MARKET) {
      return true;
    }
    return book.bestBid() >= cmd.priceTick();
  }

  private OrderNode oppositeHead(Side takerSide) {
    return takerSide == Side.BID ? book.bestAskHead() : book.bestBidHead();
  }

  private long nextEventId() {
    return ++eventId;
  }

  private long now() {
    return timeSource.currentTimeMillis();
  }

  private long deltaOffset() {
    return MatchDeltaContext.currentOffset();
  }

  private void recordDelta(BookDelta delta) {
    if (bookDeltaSink != null && symbolId > 0) {
      bookDeltaSink.append(delta);
    }
  }

  private static List<BookSnapshot.Level> toSnapshotLevels(List<DepthLevel> depth) {
    List<BookSnapshot.Level> levels = new ArrayList<>(depth.size());
    for (DepthLevel d : depth) {
      levels.add(new BookSnapshot.Level(d.priceTick(), d.totalQty()));
    }
    return List.copyOf(levels);
  }
}
