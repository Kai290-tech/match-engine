package com.kai.matchengine.match;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.kai.matchengine.event.inbound.CancelOrder;
import com.kai.matchengine.event.inbound.OrderType;
import com.kai.matchengine.event.inbound.PlaceOrder;
import com.kai.matchengine.event.outbound.BookSnapshot;
import com.kai.matchengine.event.outbound.BookUpdate;
import com.kai.matchengine.event.outbound.CommandResult;
import com.kai.matchengine.event.outbound.OrderUpdate;
import com.kai.matchengine.event.outbound.OrderUpdateType;
import com.kai.matchengine.event.outbound.OutboundEvent;
import com.kai.matchengine.event.outbound.RejectReason;
import com.kai.matchengine.event.outbound.Trade;
import com.kai.matchengine.orderbook.RestingOrder;
import com.kai.matchengine.orderbook.Side;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MatchingEngineTest {

  private static final String SYMBOL = "BTC-USDT";

  private AtomicLong clock;
  private MatchingEngine engine;

  @BeforeEach
  void setUp() {
    clock = new AtomicLong(1_000_000L);
    engine = new MatchingEngine(SYMBOL, clock::getAndIncrement);
  }

  @Test
  void limit_noCross_restsOnBook() {
    List<OutboundEvent> out =
        engine.apply(new PlaceOrder(1, 1, 1, 10, Side.BID, OrderType.LIMIT, 100, 5));

    assertEquals(3, out.size());
    OrderUpdate resting = assertInstanceOf(OrderUpdate.class, out.get(0));
    assertEquals(OrderUpdateType.RESTING, resting.type());
    BookUpdate bookUpdate = assertInstanceOf(BookUpdate.class, out.get(1));
    assertEquals(resting.eventTime(), bookUpdate.eventTime());
    assertEquals(5, bookUpdate.levelQty());
    CommandResult.Place place = assertInstanceOf(CommandResult.Place.class, out.get(2));
    assertTrue(place.accepted());
    assertTrue(place.resting());
    assertEquals(5, place.remainingQty());
  }

  @Test
  void limit_invalidPrice_rejected() {
    List<OutboundEvent> out =
        engine.apply(new PlaceOrder(1, 1, 1, 1, Side.BID, OrderType.LIMIT, 0, 5));
    CommandResult.Place place = assertInstanceOf(CommandResult.Place.class, out.getFirst());
    assertEquals(RejectReason.INVALID_PRICE, place.rejectReason());
  }

  @Test
  void trade_includesUserIds_andSharedTime() {
    engine.apply(new PlaceOrder(1, 1, 1, 11, Side.ASK, OrderType.LIMIT, 100, 3));
    List<OutboundEvent> out =
        engine.apply(new PlaceOrder(1, 2, 2, 22, Side.BID, OrderType.LIMIT, 100, 3));

    Trade trade = assertInstanceOf(Trade.class, out.get(0));
    assertEquals(22, trade.takerUserId());
    assertEquals(11, trade.makerUserId());
    OrderUpdate maker = assertInstanceOf(OrderUpdate.class, out.get(1));
    BookUpdate book = assertInstanceOf(BookUpdate.class, out.get(2));
    assertEquals(trade.eventTime(), maker.eventTime());
    assertEquals(trade.eventTime(), book.eventTime());
  }

  @Test
  void limit_crosses_partialFillAndRest() {
    engine.apply(new PlaceOrder(1, 1, 1, 1, Side.ASK, OrderType.LIMIT, 100, 3));
    List<OutboundEvent> out =
        engine.apply(new PlaceOrder(1, 2, 2, 2, Side.BID, OrderType.LIMIT, 100, 5));

    Trade trade = assertInstanceOf(Trade.class, out.get(0));
    assertEquals(3, trade.qty());
    assertEquals(0, ((BookUpdate) out.get(2)).levelQty());
    assertEquals(2, ((OrderUpdate) out.get(3)).remainingQty());
    assertEquals(2, ((BookUpdate) out.get(4)).levelQty());
    CommandResult.Place place = assertInstanceOf(CommandResult.Place.class, out.get(5));
    assertEquals(3, place.filledQty());
    assertTrue(place.resting());
  }

  @Test
  void samePrice_fifo() {
    engine.apply(new PlaceOrder(1, 1, 1, 1, Side.ASK, OrderType.LIMIT, 100, 1));
    engine.apply(new PlaceOrder(1, 2, 2, 1, Side.ASK, OrderType.LIMIT, 100, 1));
    List<OutboundEvent> out =
        engine.apply(new PlaceOrder(1, 3, 3, 2, Side.BID, OrderType.LIMIT, 100, 1));
    assertEquals(1, ((Trade) out.get(0)).makerOrderId());
    assertEquals(1, ((BookUpdate) out.get(2)).levelQty());
  }

  @Test
  void market_fillsThenCancelsRemainder() {
    engine.apply(new PlaceOrder(1, 1, 1, 1, Side.ASK, OrderType.LIMIT, 100, 2));
    List<OutboundEvent> out =
        engine.apply(new PlaceOrder(1, 2, 2, 2, Side.BID, OrderType.MARKET, 0, 5));
    CommandResult.Place place =
        out.stream()
            .filter(CommandResult.Place.class::isInstance)
            .map(CommandResult.Place.class::cast)
            .findFirst()
            .orElseThrow();
    assertEquals(2, place.filledQty());
    assertFalse(place.resting());
    assertTrue(
        out.stream()
            .filter(OrderUpdate.class::isInstance)
            .map(OrderUpdate.class::cast)
            .anyMatch(u -> u.type() == OrderUpdateType.CANCELLED_REMAINDER));
  }

  @Test
  void cancel_andUserMismatch() {
    engine.apply(new PlaceOrder(1, 1, 1, 1, Side.BID, OrderType.LIMIT, 100, 5));
    assertEquals(
        RejectReason.USER_MISMATCH,
        ((CommandResult.Cancel) engine.apply(new CancelOrder(1, 1, 1, 99)).getFirst()).rejectReason());
    List<OutboundEvent> ok = engine.apply(new CancelOrder(1, 1, 1, 1));
    assertTrue(((CommandResult.Cancel) ok.getLast()).accepted());
    assertTrue(engine.book().isEmpty());
  }

  @Test
  void exportBookSnapshot_andRestore() {
    engine.apply(new PlaceOrder(1, 1, 1, 1, Side.BID, OrderType.LIMIT, 100, 5));
    engine.apply(new PlaceOrder(1, 2, 2, 1, Side.BID, OrderType.LIMIT, 99, 3));
    engine.apply(new PlaceOrder(1, 3, 3, 2, Side.ASK, OrderType.LIMIT, 101, 4));

    BookSnapshot snap = engine.exportBookSnapshot();
    assertEquals(SYMBOL, snap.symbol());
    assertEquals(engine.lastBookVersion(), snap.bookVersion());
    assertEquals(100, snap.bids().getFirst().priceTick());
    assertEquals(5, snap.bids().getFirst().qty());
    assertEquals(99, snap.bids().get(1).priceTick());
    assertEquals(101, snap.asks().getFirst().priceTick());

    List<RestingOrder> resting = engine.restingOrders();
    long eventId = engine.lastEventId();
    long bookVer = engine.lastBookVersion();

    MatchingEngine restored = new MatchingEngine(SYMBOL, clock::getAndIncrement);
    restored.restoreSequences(eventId, bookVer);
    for (RestingOrder o : resting) {
      restored.restoreRestingOrder(o.orderId(), o.userId(), o.side(), o.priceTick(), o.qty());
    }
    assertEquals(eventId, restored.lastEventId());
    assertEquals(bookVer, restored.lastBookVersion());
    assertEquals(3, restored.book().orderCount());
    assertEquals(100, restored.book().bestBid());
    assertEquals(101, restored.book().bestAsk());

    // 恢复后序列继续递增
    List<OutboundEvent> next =
        restored.apply(new PlaceOrder(1, 4, 4, 1, Side.ASK, OrderType.LIMIT, 102, 1));
    OrderUpdate u = assertInstanceOf(OrderUpdate.class, next.getFirst());
    assertEquals(eventId + 1, u.eventId());
  }

  @Test
  void duplicateAndMissing() {
    engine.apply(new PlaceOrder(1, 1, 1, 1, Side.BID, OrderType.LIMIT, 100, 1));
    assertEquals(
        RejectReason.DUPLICATE_ORDER_ID,
        ((CommandResult.Place)
                engine.apply(new PlaceOrder(1, 1, 1, 1, Side.BID, OrderType.LIMIT, 101, 1)).getFirst())
            .rejectReason());
    assertEquals(
        RejectReason.ORDER_NOT_FOUND,
        ((CommandResult.Cancel) engine.apply(new CancelOrder(1, 99, 99, 1)).getFirst()).rejectReason());
  }

  @Test
  void duplicateOrderId_rejectedAfterFillAndCancel() {
    engine.apply(new PlaceOrder(1, 1, 10, 1, Side.ASK, OrderType.LIMIT, 100, 1));
    engine.apply(new PlaceOrder(1, 2, 11, 2, Side.BID, OrderType.LIMIT, 100, 1)); // fills 10
    assertTrue(engine.book().isEmpty());
    assertEquals(
        RejectReason.DUPLICATE_ORDER_ID,
        ((CommandResult.Place)
                engine.apply(new PlaceOrder(1, 3, 10, 1, Side.BID, OrderType.LIMIT, 99, 1)).getFirst())
            .rejectReason());

    engine.apply(new PlaceOrder(1, 4, 20, 1, Side.BID, OrderType.LIMIT, 50, 1));
    engine.apply(new CancelOrder(1, 5, 20, 1));
    assertEquals(
        RejectReason.DUPLICATE_ORDER_ID,
        ((CommandResult.Place)
                engine.apply(new PlaceOrder(1, 6, 20, 1, Side.ASK, OrderType.LIMIT, 60, 1))
                    .getFirst())
            .rejectReason());
  }

  @Test
  void invalidQty_rejected() {
    CommandResult.Place place =
        (CommandResult.Place)
            engine.apply(new PlaceOrder(1, 1, 1, 1, Side.BID, OrderType.LIMIT, 100, 0)).getFirst();
    assertEquals(RejectReason.INVALID_QTY, place.rejectReason());
    assertFalse(engine.book().contains(1));
  }

  @Test
  void limit_negativePrice_rejected() {
    assertEquals(
        RejectReason.INVALID_PRICE,
        ((CommandResult.Place)
                engine.apply(new PlaceOrder(1, 1, 1, 1, Side.ASK, OrderType.LIMIT, -1, 1)).getFirst())
            .rejectReason());
  }

  @Test
  void market_emptyBook_cancelsAll() {
    List<OutboundEvent> out =
        engine.apply(new PlaceOrder(1, 1, 1, 1, Side.BID, OrderType.MARKET, 999, 5));
    assertEquals(2, out.size());
    assertEquals(OrderUpdateType.CANCELLED_REMAINDER, ((OrderUpdate) out.get(0)).type());
    CommandResult.Place place = (CommandResult.Place) out.get(1);
    assertTrue(place.accepted());
    assertEquals(0, place.filledQty());
    assertFalse(place.resting());
    assertEquals(0, engine.lastBookVersion());
  }

  @Test
  void market_sell_hitsBids() {
    engine.apply(new PlaceOrder(1, 1, 1, 1, Side.BID, OrderType.LIMIT, 100, 2));
    List<OutboundEvent> out =
        engine.apply(new PlaceOrder(1, 2, 2, 2, Side.ASK, OrderType.MARKET, 0, 2));
    Trade trade = (Trade) out.get(0);
    assertEquals(100, trade.priceTick());
    assertEquals(Side.ASK, trade.takerSide());
    assertTrue(engine.book().isEmpty());
  }

  @Test
  void limit_sell_crossesMultipleBidLevels() {
    engine.apply(new PlaceOrder(1, 1, 1, 1, Side.BID, OrderType.LIMIT, 102, 1));
    engine.apply(new PlaceOrder(1, 2, 2, 1, Side.BID, OrderType.LIMIT, 101, 1));
    engine.apply(new PlaceOrder(1, 3, 3, 1, Side.BID, OrderType.LIMIT, 100, 1));

    List<OutboundEvent> out =
        engine.apply(new PlaceOrder(1, 10, 10, 2, Side.ASK, OrderType.LIMIT, 101, 3));

    List<Trade> trades =
        out.stream().filter(Trade.class::isInstance).map(Trade.class::cast).toList();
    assertEquals(2, trades.size());
    assertEquals(102, trades.get(0).priceTick());
    assertEquals(1, trades.get(0).makerOrderId());
    assertEquals(101, trades.get(1).priceTick());
    assertEquals(2, trades.get(1).makerOrderId());

    CommandResult.Place place = (CommandResult.Place) out.getLast();
    assertEquals(2, place.filledQty());
    assertEquals(1, place.remainingQty());
    assertTrue(place.resting());
    assertEquals(101, engine.book().bestAsk());
    assertEquals(100, engine.book().bestBid());
  }

  @Test
  void limit_buy_walksMultipleAskLevels_exactFill() {
    engine.apply(new PlaceOrder(1, 1, 1, 1, Side.ASK, OrderType.LIMIT, 100, 2));
    engine.apply(new PlaceOrder(1, 2, 2, 1, Side.ASK, OrderType.LIMIT, 101, 2));

    List<OutboundEvent> out =
        engine.apply(new PlaceOrder(1, 3, 3, 2, Side.BID, OrderType.LIMIT, 101, 4));

    assertEquals(2, out.stream().filter(Trade.class::isInstance).count());
    CommandResult.Place place = (CommandResult.Place) out.getLast();
    assertEquals(4, place.filledQty());
    assertFalse(place.resting());
    assertTrue(
        out.stream()
            .filter(OrderUpdate.class::isInstance)
            .map(OrderUpdate.class::cast)
            .anyMatch(u -> u.orderId() == 3 && u.type() == OrderUpdateType.FILLED));
    assertTrue(engine.book().isEmpty());
  }

  @Test
  void maker_partiallyFilled_thenFullyFilled() {
    engine.apply(new PlaceOrder(1, 1, 1, 1, Side.ASK, OrderType.LIMIT, 100, 5));

    List<OutboundEvent> first =
        engine.apply(new PlaceOrder(1, 2, 2, 2, Side.BID, OrderType.LIMIT, 100, 2));
    OrderUpdate makerPartial = (OrderUpdate) first.get(1);
    assertEquals(OrderUpdateType.PARTIALLY_FILLED, makerPartial.type());
    assertEquals(3, makerPartial.remainingQty());
    assertEquals(3, engine.book().get(1).qty());

    List<OutboundEvent> second =
        engine.apply(new PlaceOrder(1, 3, 3, 2, Side.BID, OrderType.LIMIT, 100, 3));
    OrderUpdate makerFilled = (OrderUpdate) second.get(1);
    assertEquals(OrderUpdateType.FILLED, makerFilled.type());
    assertFalse(engine.book().contains(1));
  }

  @Test
  void everyApply_endsWithCommandResult() {
    List<List<OutboundEvent>> batches =
        List.of(
            engine.apply(new PlaceOrder(1, 1, 1, 1, Side.BID, OrderType.LIMIT, 100, 1)),
            engine.apply(new PlaceOrder(1, 2, 2, 1, Side.ASK, OrderType.LIMIT, 110, 1)),
            engine.apply(new PlaceOrder(1, 3, 3, 2, Side.BID, OrderType.MARKET, 0, 1)),
            engine.apply(new CancelOrder(1, 2, 2, 1)),
            engine.apply(new PlaceOrder(1, 1, 1, 1, Side.BID, OrderType.LIMIT, 100, 1)));
    for (List<OutboundEvent> batch : batches) {
      assertInstanceOf(CommandResult.class, batch.getLast());
    }
  }

  @Test
  void bookVersion_andEventId_monotonic() {
    engine.apply(new PlaceOrder(1, 1, 1, 1, Side.ASK, OrderType.LIMIT, 100, 2));
    long bookAfterRest = engine.lastBookVersion();
    long eventAfterRest = engine.lastEventId();
    assertTrue(bookAfterRest >= 1);
    assertTrue(eventAfterRest >= 1);

    List<OutboundEvent> out =
        engine.apply(new PlaceOrder(1, 2, 2, 2, Side.BID, OrderType.LIMIT, 100, 2));
    long prevEvent = eventAfterRest;
    long prevBook = bookAfterRest;
    for (OutboundEvent e : out) {
      if (e instanceof Trade t) {
        assertTrue(t.eventId() > prevEvent);
        prevEvent = t.eventId();
      } else if (e instanceof OrderUpdate u) {
        assertTrue(u.eventId() > prevEvent);
        prevEvent = u.eventId();
      } else if (e instanceof BookUpdate b) {
        assertTrue(b.bookVersion() > prevBook);
        prevBook = b.bookVersion();
      }
    }
    assertEquals(prevEvent, engine.lastEventId());
    assertEquals(prevBook, engine.lastBookVersion());
  }

  @Test
  void cancel_updateAndBookShareTime() {
    engine.apply(new PlaceOrder(1, 1, 1, 1, Side.BID, OrderType.LIMIT, 100, 5));
    List<OutboundEvent> out = engine.apply(new CancelOrder(1, 1, 1, 1));
    OrderUpdate update = (OrderUpdate) out.get(0);
    BookUpdate book = (BookUpdate) out.get(1);
    assertEquals(update.eventTime(), book.eventTime());
    assertEquals(0, book.levelQty());
  }

  @Test
  void exportBookSnapshot_empty() {
    BookSnapshot snap = engine.exportBookSnapshot();
    assertTrue(snap.bids().isEmpty());
    assertTrue(snap.asks().isEmpty());
    assertEquals(0, snap.bookVersion());
  }

  @Test
  void restoreSequences_rejectsNegative() {
    org.junit.jupiter.api.Assertions.assertThrows(
        IllegalArgumentException.class, () -> engine.restoreSequences(-1, 0));
    org.junit.jupiter.api.Assertions.assertThrows(
        IllegalArgumentException.class, () -> engine.restoreSequences(0, -1));
  }

  @Test
  void limit_noCross_askRests() {
    engine.apply(new PlaceOrder(1, 1, 1, 1, Side.BID, OrderType.LIMIT, 100, 1));
    List<OutboundEvent> out =
        engine.apply(new PlaceOrder(1, 2, 2, 1, Side.ASK, OrderType.LIMIT, 101, 1));
    assertEquals(OrderUpdateType.RESTING, ((OrderUpdate) out.get(0)).type());
    assertEquals(100, engine.book().bestBid());
    assertEquals(101, engine.book().bestAsk());
  }

  @Test
  void symbol_onAllOutbound() {
    engine.apply(new PlaceOrder(1, 1, 1, 1, Side.ASK, OrderType.LIMIT, 100, 1));
    for (OutboundEvent e :
        engine.apply(new PlaceOrder(1, 2, 2, 2, Side.BID, OrderType.LIMIT, 100, 1))) {
      switch (e) {
        case Trade t -> assertEquals(SYMBOL, t.symbol());
        case OrderUpdate u -> assertEquals(SYMBOL, u.symbol());
        case BookUpdate b -> assertEquals(SYMBOL, b.symbol());
        case CommandResult.Place p -> assertEquals(SYMBOL, p.symbol());
        default -> {}
      }
    }
  }
  @Test
  void clientRequestId_echoedInCommandResult() {
    List<OutboundEvent> placeOut =
        engine.apply(new PlaceOrder(1, 9001L, 1, 1, Side.BID, OrderType.LIMIT, 100, 1));
    CommandResult.Place place = assertInstanceOf(CommandResult.Place.class, placeOut.getLast());
    assertEquals(9001L, place.clientRequestId());

    List<OutboundEvent> cancelOut = engine.apply(new CancelOrder(1, 9002L, 1, 1));
    CommandResult.Cancel cancel = assertInstanceOf(CommandResult.Cancel.class, cancelOut.getLast());
    assertEquals(9002L, cancel.clientRequestId());
  }

}
