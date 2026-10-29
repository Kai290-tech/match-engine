package com.kai.matchengine.match;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.kai.matchengine.event.inbound.OrderType;
import com.kai.matchengine.event.inbound.PlaceOrder;
import com.kai.matchengine.event.outbound.CommandResult;
import com.kai.matchengine.event.outbound.OrderUpdate;
import com.kai.matchengine.event.outbound.OrderUpdateType;
import com.kai.matchengine.event.outbound.OutboundEvent;
import com.kai.matchengine.event.outbound.RejectReason;
import com.kai.matchengine.event.outbound.Trade;
import com.kai.matchengine.orderbook.Side;
import com.kai.matchengine.snapshot.delta.BookDelta;
import com.kai.matchengine.snapshot.delta.BookDeltaType;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** ME-IOC-001：F17 部分成交，F18 无交叉，F19 池满仍可成交。 */
class IocMatchingTest {

  @Test
  void f17_partialFillCancelsRemainderAtMakerPriceFifo() {
    List<BookDelta> deltas = new ArrayList<>();
    MatchingEngine engine = new MatchingEngine("S-1", 1, () -> 1L, 8, deltas::add);
    engine.apply(new PlaceOrder(1, 1, 1, 1, Side.ASK, OrderType.LIMIT, 100, 1));
    engine.apply(new PlaceOrder(1, 2, 2, 1, Side.ASK, OrderType.LIMIT, 100, 2));

    List<OutboundEvent> out =
        engine.apply(new PlaceOrder(1, 3, 30, 2, Side.BID, OrderType.IOC, 100, 4));

    List<Trade> trades = out.stream().filter(Trade.class::isInstance).map(Trade.class::cast).toList();
    assertEquals(2, trades.size());
    assertEquals(1L, trades.get(0).makerOrderId());
    assertEquals(2L, trades.get(1).makerOrderId());
    assertEquals(100L, trades.get(0).priceTick());
    assertEquals(100L, trades.get(1).priceTick());
    assertEquals(1L, trades.get(0).qty());
    assertEquals(2L, trades.get(1).qty());

    CommandResult.Place place = placeResult(out);
    assertTrue(place.accepted());
    assertEquals(3L, place.filledQty());
    assertEquals(0L, place.remainingQty());
    assertFalse(place.resting());
    assertTrue(takerUpdate(out, 30L, OrderUpdateType.CANCELLED_REMAINDER));
    assertFalse(engine.book().contains(30L));
    assertTrue(engine.book().isEmpty());
    assertTrue(
        deltas.stream().noneMatch(d -> d.orderId() == 30L && d.type() == BookDeltaType.REST_ADD));
    assertTrue(deltas.stream().anyMatch(d -> d.orderId() == 30L && d.type() == BookDeltaType.META_RECENT_ORDER));
  }

  @Test
  void f18_noCrossAcceptsAndCancelsAll() {
    MatchingEngine engine = new MatchingEngine("S-1", () -> 1L);
    engine.apply(new PlaceOrder(1, 1, 1, 1, Side.ASK, OrderType.LIMIT, 200, 5));

    List<OutboundEvent> out =
        engine.apply(new PlaceOrder(1, 2, 20, 2, Side.BID, OrderType.IOC, 100, 3));

    assertTrue(out.stream().noneMatch(Trade.class::isInstance));
    CommandResult.Place place = placeResult(out);
    assertTrue(place.accepted());
    assertEquals(0L, place.filledQty());
    assertEquals(0L, place.remainingQty());
    assertFalse(place.resting());
    assertTrue(takerUpdate(out, 20L, OrderUpdateType.CANCELLED_REMAINDER));
    assertFalse(engine.book().contains(20L));
    assertEquals(1, engine.book().orderCount());

    CommandResult.Place duplicate =
        placeResult(engine.apply(new PlaceOrder(1, 3, 20, 2, Side.BID, OrderType.IOC, 200, 1)));
    assertEquals(RejectReason.DUPLICATE_ORDER_ID, duplicate.rejectReason());
    assertEquals(1, engine.book().orderCount());
  }

  @Test
  void f18_invalidPriceDoesNotEnterIdempotencyWindow() {
    MatchingEngine engine = new MatchingEngine("S-1", () -> 1L);
    CommandResult.Place rejected =
        placeResult(engine.apply(new PlaceOrder(1, 1, 7, 1, Side.BID, OrderType.IOC, 0, 1)));
    assertEquals(RejectReason.INVALID_PRICE, rejected.rejectReason());

    CommandResult.Place accepted =
        placeResult(engine.apply(new PlaceOrder(1, 2, 7, 1, Side.BID, OrderType.IOC, 100, 1)));
    assertTrue(accepted.accepted());
    assertEquals(0L, accepted.filledQty());
  }

  @Test
  void f19_bookFullStillMatches() {
    MatchingEngine engine = new MatchingEngine("S-1", () -> 1L, 1);
    engine.apply(new PlaceOrder(1, 1, 1, 1, Side.ASK, OrderType.LIMIT, 100, 2));
    assertFalse(engine.book().hasCapacityForNewOrder());

    List<OutboundEvent> out =
        engine.apply(new PlaceOrder(1, 2, 9, 2, Side.BID, OrderType.IOC, 100, 5));

    assertTrue(out.stream().noneMatch(e -> e instanceof CommandResult.Place p && p.rejectReason() == RejectReason.BOOK_FULL));
    CommandResult.Place place = placeResult(out);
    assertTrue(place.accepted());
    assertEquals(2L, place.filledQty());
    assertEquals(0L, place.remainingQty());
    assertFalse(place.resting());
    Trade trade = out.stream().filter(Trade.class::isInstance).map(Trade.class::cast).findFirst().orElseThrow();
    assertEquals(100L, trade.priceTick());
    assertEquals(2L, trade.qty());
    assertTrue(engine.book().isEmpty());
  }

  @Test
  void bookFull_noCross_stillAccepted() {
    MatchingEngine engine = new MatchingEngine("S-1", () -> 1L, 1);
    engine.apply(new PlaceOrder(1, 1, 1, 1, Side.BID, OrderType.LIMIT, 100, 1));

    CommandResult.Place place =
        placeResult(engine.apply(new PlaceOrder(1, 2, 8, 2, Side.ASK, OrderType.IOC, 200, 1)));
    assertTrue(place.accepted());
    assertEquals(0L, place.filledQty());
    assertFalse(place.resting());
    assertTrue(engine.book().contains(1L));
    assertFalse(engine.book().contains(8L));
  }

  private static CommandResult.Place placeResult(List<OutboundEvent> out) {
    return assertInstanceOf(CommandResult.Place.class, out.getLast());
  }

  private static boolean takerUpdate(List<OutboundEvent> out, long orderId, OrderUpdateType type) {
    return out.stream()
        .filter(OrderUpdate.class::isInstance)
        .map(OrderUpdate.class::cast)
        .anyMatch(u -> u.orderId() == orderId && u.type() == type);
  }
}
