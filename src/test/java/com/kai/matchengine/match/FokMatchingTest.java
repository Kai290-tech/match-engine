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

/** ME-FOK-001：F20 全部成交，F21 深度不足，F22 池满仍可全部成交。 */
class FokMatchingTest {

  @Test
  void f20_fillsEntireOrderAtMakerPriceFifo() {
    List<BookDelta> deltas = new ArrayList<>();
    MatchingEngine engine = new MatchingEngine("S-1", 1, () -> 1L, 8, deltas::add);
    engine.apply(new PlaceOrder(1, 1, 1, 1, Side.ASK, OrderType.LIMIT, 100, 1));
    engine.apply(new PlaceOrder(1, 2, 2, 1, Side.ASK, OrderType.LIMIT, 100, 2));

    List<OutboundEvent> out =
        engine.apply(new PlaceOrder(1, 3, 30, 2, Side.BID, OrderType.FOK, 100, 3));

    List<Trade> trades = out.stream().filter(Trade.class::isInstance).map(Trade.class::cast).toList();
    assertEquals(2, trades.size());
    assertEquals(1L, trades.get(0).makerOrderId());
    assertEquals(1L, trades.get(0).qty());
    assertEquals(2L, trades.get(1).makerOrderId());
    assertEquals(2L, trades.get(1).qty());
    assertEquals(100L, trades.get(0).priceTick());
    assertEquals(100L, trades.get(1).priceTick());

    CommandResult.Place place = placeResult(out);
    assertTrue(place.accepted());
    assertEquals(3L, place.filledQty());
    assertEquals(0L, place.remainingQty());
    assertFalse(place.resting());
    assertTrue(engine.book().isEmpty());
    assertTrue(deltas.stream().noneMatch(d -> d.orderId() == 30L && d.type() == BookDeltaType.REST_ADD));
    assertTrue(deltas.stream().anyMatch(d -> d.orderId() == 30L && d.type() == BookDeltaType.META_RECENT_ORDER));
  }

  @Test
  void f21_insufficientDepthCancelsWithoutTrading() {
    MatchingEngine engine = new MatchingEngine("S-1", () -> 1L);
    engine.apply(new PlaceOrder(1, 1, 1, 1, Side.ASK, OrderType.LIMIT, 100, 2));
    engine.apply(new PlaceOrder(1, 2, 2, 1, Side.ASK, OrderType.LIMIT, 150, 10));

    List<OutboundEvent> out =
        engine.apply(new PlaceOrder(1, 3, 30, 2, Side.BID, OrderType.FOK, 100, 5));

    assertTrue(out.stream().noneMatch(Trade.class::isInstance));
    CommandResult.Place place = placeResult(out);
    assertTrue(place.accepted());
    assertEquals(0L, place.filledQty());
    assertEquals(0L, place.remainingQty());
    assertFalse(place.resting());
    assertTrue(cancelledRemainder(out, 30L));
    assertEquals(2L, engine.book().get(1).qty());
    assertEquals(10L, engine.book().get(2).qty());

    CommandResult.Place duplicate =
        placeResult(engine.apply(new PlaceOrder(1, 4, 30, 2, Side.BID, OrderType.FOK, 150, 5)));
    assertEquals(RejectReason.DUPLICATE_ORDER_ID, duplicate.rejectReason());
    assertEquals(2, engine.book().orderCount());
  }

  @Test
  void f21_noCrossLeavesBookUnchanged() {
    MatchingEngine engine = new MatchingEngine("S-1", () -> 1L);
    engine.apply(new PlaceOrder(1, 1, 1, 1, Side.ASK, OrderType.LIMIT, 200, 4));

    CommandResult.Place place =
        placeResult(engine.apply(new PlaceOrder(1, 2, 8, 2, Side.BID, OrderType.FOK, 100, 1)));
    assertTrue(place.accepted());
    assertEquals(0L, place.filledQty());
    assertFalse(place.resting());
    assertEquals(4L, engine.book().get(1).qty());
  }

  @Test
  void f21_invalidPriceDoesNotEnterIdempotencyWindow() {
    MatchingEngine engine = new MatchingEngine("S-1", () -> 1L);
    CommandResult.Place rejected =
        placeResult(engine.apply(new PlaceOrder(1, 1, 7, 1, Side.BID, OrderType.FOK, 0, 1)));
    assertEquals(RejectReason.INVALID_PRICE, rejected.rejectReason());

    CommandResult.Place accepted =
        placeResult(engine.apply(new PlaceOrder(1, 2, 7, 1, Side.BID, OrderType.FOK, 100, 1)));
    assertTrue(accepted.accepted());
    assertEquals(0L, accepted.filledQty());
  }

  @Test
  void f22_bookFullStillFillsWhenDepthIsEnough() {
    MatchingEngine engine = new MatchingEngine("S-1", () -> 1L, 1);
    engine.apply(new PlaceOrder(1, 1, 1, 1, Side.ASK, OrderType.LIMIT, 100, 3));
    assertFalse(engine.book().hasCapacityForNewOrder());

    List<OutboundEvent> out =
        engine.apply(new PlaceOrder(1, 2, 9, 2, Side.BID, OrderType.FOK, 100, 3));

    assertTrue(out.stream().noneMatch(e -> e instanceof CommandResult.Place p && p.rejectReason() == RejectReason.BOOK_FULL));
    CommandResult.Place place = placeResult(out);
    assertTrue(place.accepted());
    assertEquals(3L, place.filledQty());
    assertEquals(0L, place.remainingQty());
    assertFalse(place.resting());
    Trade trade = out.stream().filter(Trade.class::isInstance).map(Trade.class::cast).findFirst().orElseThrow();
    assertEquals(100L, trade.priceTick());
    assertEquals(3L, trade.qty());
    assertTrue(engine.book().isEmpty());
  }

  private static CommandResult.Place placeResult(List<OutboundEvent> out) {
    return assertInstanceOf(CommandResult.Place.class, out.getLast());
  }

  private static boolean cancelledRemainder(List<OutboundEvent> out, long orderId) {
    return out.stream()
        .filter(OrderUpdate.class::isInstance)
        .map(OrderUpdate.class::cast)
        .anyMatch(u -> u.orderId() == orderId && u.type() == OrderUpdateType.CANCELLED_REMAINDER);
  }
}
