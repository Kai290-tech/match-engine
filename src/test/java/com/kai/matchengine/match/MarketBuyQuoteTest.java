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
import java.util.List;
import org.junit.jupiter.api.Test;

/** ME-MKT-BUY-001：F23 预算向下取整，F24 买不起一档，F04 卖单仍按基础数量。 */
class MarketBuyQuoteTest {

  @Test
  void f23_floorsAcrossLevelsAndNeverExceedsBudget() {
    MatchingEngine engine = new MatchingEngine("S-1", () -> 1L);
    engine.apply(new PlaceOrder(1, 1, 1, 1, Side.ASK, OrderType.LIMIT, 100, 2));
    engine.apply(new PlaceOrder(1, 2, 2, 1, Side.ASK, OrderType.LIMIT, 150, 2));

    List<OutboundEvent> out =
        engine.apply(new PlaceOrder(1, 3, 30, 2, Side.BID, OrderType.MARKET, 0, 450));

    List<Trade> trades = trades(out);
    assertEquals(2, trades.size());
    assertEquals(1L, trades.get(0).makerOrderId());
    assertEquals(100L, trades.get(0).priceTick());
    assertEquals(2L, trades.get(0).qty());
    assertEquals(2L, trades.get(1).makerOrderId());
    assertEquals(150L, trades.get(1).priceTick());
    assertEquals(1L, trades.get(1).qty());
    assertTrue(notional(trades) <= 450L);
    assertEquals(350L, notional(trades));

    CommandResult.Place place = placeResult(out);
    assertTrue(place.accepted());
    assertEquals(3L, place.filledQty());
    assertEquals(0L, place.remainingQty());
    assertFalse(place.resting());
    OrderUpdate taker = takerUpdate(out, 30L);
    assertEquals(OrderUpdateType.CANCELLED_REMAINDER, taker.type());
    assertEquals(3L, taker.filledDelta());
    assertEquals(0L, taker.remainingQty());
    assertFalse(engine.book().contains(1));
    assertEquals(1L, engine.book().get(2).qty());
    assertFalse(engine.book().contains(30));
  }

  @Test
  void f23_samePriceFifoAndExactBudgetFills() {
    MatchingEngine engine = new MatchingEngine("S-1", () -> 1L);
    engine.apply(new PlaceOrder(1, 1, 1, 1, Side.ASK, OrderType.LIMIT, 100, 1));
    engine.apply(new PlaceOrder(1, 2, 2, 1, Side.ASK, OrderType.LIMIT, 100, 2));

    List<OutboundEvent> out =
        engine.apply(new PlaceOrder(1, 3, 30, 2, Side.BID, OrderType.MARKET, 0, 200));

    List<Trade> trades = trades(out);
    assertEquals(2, trades.size());
    assertEquals(1L, trades.get(0).makerOrderId());
    assertEquals(1L, trades.get(0).qty());
    assertEquals(2L, trades.get(1).makerOrderId());
    assertEquals(1L, trades.get(1).qty());
    assertEquals(200L, notional(trades));
    CommandResult.Place place = placeResult(out);
    assertEquals(2L, place.filledQty());
    assertEquals(OrderUpdateType.FILLED, takerUpdate(out, 30L).type());
    assertEquals(1L, engine.book().get(2).qty());
  }

  @Test
  void f23_singleLevelLeavesIntegerDust() {
    MatchingEngine engine = new MatchingEngine("S-1", () -> 1L);
    engine.apply(new PlaceOrder(1, 1, 1, 1, Side.ASK, OrderType.LIMIT, 3, 4_000));

    List<OutboundEvent> out =
        engine.apply(new PlaceOrder(1, 2, 20, 2, Side.BID, OrderType.MARKET, 0, 10_000));

    List<Trade> trades = trades(out);
    assertEquals(1, trades.size());
    assertEquals(3L, trades.get(0).priceTick());
    assertEquals(3_333L, trades.get(0).qty());
    assertEquals(9_999L, notional(trades));
    CommandResult.Place place = placeResult(out);
    assertEquals(3_333L, place.filledQty());
    assertEquals(0L, place.remainingQty());
    assertEquals(OrderUpdateType.CANCELLED_REMAINDER, takerUpdate(out, 20L).type());
    assertEquals(667L, engine.book().get(1).qty());
  }

  @Test
  void f23_fullPoolStillMatches() {
    MatchingEngine engine = new MatchingEngine("S-1", () -> 1L, 1);
    engine.apply(new PlaceOrder(1, 1, 1, 1, Side.ASK, OrderType.LIMIT, 100, 5));
    assertFalse(engine.book().hasCapacityForNewOrder());

    List<OutboundEvent> out =
        engine.apply(new PlaceOrder(1, 2, 9, 2, Side.BID, OrderType.MARKET, 0, 250));

    assertTrue(
        out.stream()
            .noneMatch(e -> e instanceof CommandResult.Place p && p.rejectReason() == RejectReason.BOOK_FULL));
    CommandResult.Place place = placeResult(out);
    assertTrue(place.accepted());
    assertEquals(2L, place.filledQty());
    assertEquals(3L, engine.book().get(1).qty());
    assertFalse(engine.book().contains(9));
  }

  @Test
  void f24_cannotAffordOneLotCancelsWithoutTrading() {
    MatchingEngine engine = new MatchingEngine("S-1", () -> 1L);
    engine.apply(new PlaceOrder(1, 1, 1, 1, Side.ASK, OrderType.LIMIT, 100, 2));

    List<OutboundEvent> out =
        engine.apply(new PlaceOrder(1, 2, 8, 2, Side.BID, OrderType.MARKET, 0, 99));

    assertTrue(out.stream().noneMatch(Trade.class::isInstance));
    CommandResult.Place place = placeResult(out);
    assertTrue(place.accepted());
    assertEquals(0L, place.filledQty());
    assertEquals(0L, place.remainingQty());
    assertFalse(place.resting());
    assertEquals(OrderUpdateType.CANCELLED_REMAINDER, takerUpdate(out, 8L).type());
    assertEquals(2L, engine.book().get(1).qty());

    CommandResult.Place duplicate =
        placeResult(engine.apply(new PlaceOrder(1, 3, 8, 2, Side.BID, OrderType.MARKET, 0, 10_000)));
    assertEquals(RejectReason.DUPLICATE_ORDER_ID, duplicate.rejectReason());
    assertEquals(2L, engine.book().get(1).qty());
  }

  @Test
  void f04_marketSellStillUsesBaseQty() {
    MatchingEngine engine = new MatchingEngine("S-1", () -> 1L);
    engine.apply(new PlaceOrder(1, 1, 1, 1, Side.BID, OrderType.LIMIT, 100, 2));

    List<OutboundEvent> out =
        engine.apply(new PlaceOrder(1, 2, 4, 2, Side.ASK, OrderType.MARKET, 0, 5));

    List<Trade> trades = trades(out);
    assertEquals(1, trades.size());
    assertEquals(100L, trades.get(0).priceTick());
    assertEquals(2L, trades.get(0).qty());
    CommandResult.Place place = placeResult(out);
    assertEquals(2L, place.filledQty());
    assertEquals(OrderUpdateType.CANCELLED_REMAINDER, takerUpdate(out, 4L).type());
    assertEquals(2L, takerUpdate(out, 4L).filledDelta());
    assertTrue(engine.book().isEmpty());
  }

  private static List<Trade> trades(List<OutboundEvent> out) {
    return out.stream().filter(Trade.class::isInstance).map(Trade.class::cast).toList();
  }

  private static long notional(List<Trade> trades) {
    long spent = 0L;
    for (Trade trade : trades) {
      spent += trade.priceTick() * trade.qty();
    }
    return spent;
  }

  private static CommandResult.Place placeResult(List<OutboundEvent> out) {
    return assertInstanceOf(CommandResult.Place.class, out.getLast());
  }

  private static OrderUpdate takerUpdate(List<OutboundEvent> out, long orderId) {
    return out.stream()
        .filter(OrderUpdate.class::isInstance)
        .map(OrderUpdate.class::cast)
        .filter(u -> u.orderId() == orderId)
        .findFirst()
        .orElseThrow();
  }
}
