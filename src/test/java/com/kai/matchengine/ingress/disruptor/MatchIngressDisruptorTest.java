package com.kai.matchengine.ingress.disruptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import com.kai.matchengine.event.inbound.CancelOrder;
import com.kai.matchengine.event.inbound.OrderType;
import com.kai.matchengine.event.inbound.PlaceOrder;
import com.kai.matchengine.event.outbound.OrderUpdate;
import com.kai.matchengine.event.outbound.OrderUpdateType;
import com.kai.matchengine.event.outbound.OutboundEvent;
import com.kai.matchengine.ingress.IngressValidator;
import com.kai.matchengine.ingress.MatchEventProcessor;
import com.kai.matchengine.ingress.SbeMatchEventCodec;
import com.kai.matchengine.ingress.SbeMatchEventEncoder;
import com.kai.matchengine.ingress.FatalFailureHandler;
import com.kai.matchengine.ingress.ValidatedInbound;
import com.kai.matchengine.match.EngineRegistry;
import com.kai.matchengine.match.SystemTimeSource;
import com.kai.matchengine.orderbook.Side;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class MatchIngressDisruptorTest {

  private MatchIngressDisruptor disruptor;

  @AfterEach
  void tearDown() {
    if (disruptor != null) {
      disruptor.close();
    }
  }

  @Test
  void validatedEvents_areAppliedOnConsumerThread() throws Exception {
    EngineRegistry registry = new EngineRegistry(SystemTimeSource.INSTANCE);
    registry.register(1);
    registry.register(2);
    disruptor = newDisruptor(registry, MatchingApplyHandler.OutboundListener.NOOP);

    SbeMatchEventEncoder encoder = new SbeMatchEventEncoder();
    MatchEventProcessor processor =
        new MatchEventProcessor(
            new SbeMatchEventCodec(), new IngressValidator(), disruptor.publisher());

    assertTrue(
        processor.process(
            encoder.encode(new PlaceOrder(1, 10, 10, 1, Side.BID, OrderType.LIMIT, 100, 5)),
            "match_event",
            0,
            1L));
    assertTrue(
        processor.process(
            encoder.encode(new PlaceOrder(2, 20, 20, 1, Side.ASK, OrderType.LIMIT, 200, 3)),
            "match_event",
            0,
            2L));

    waitUntil(
        () -> registry.get(1).book().orderCount() == 1 && registry.get(2).book().orderCount() == 1);

    assertEquals(100, registry.get(1).book().bestBid());
    assertEquals(200, registry.get(2).book().bestAsk());
  }

  @Test
  void customOutboundListener_receivesEvents() throws Exception {
    EngineRegistry registry = new EngineRegistry(SystemTimeSource.INSTANCE);
    registry.register(1);
    List<Long> offsets = new CopyOnWriteArrayList<>();
    disruptor = newDisruptor(registry, (symbolId, offset, events) -> offsets.add(offset));

    disruptor
        .publisher()
        .onValidated(
            new ValidatedInbound(
                new PlaceOrder(1, 1, 1, 1, Side.ASK, OrderType.LIMIT, 101, 1), 1, "t", 0, 99L));

    waitUntil(() -> offsets.size() == 1);
    assertEquals(List.of(99L), offsets);
    assertEquals(101, registry.get(1).book().bestAsk());
  }

  @Test
  void applies_inPublishOrder_sameSymbol() throws Exception {
    EngineRegistry registry = new EngineRegistry(SystemTimeSource.INSTANCE);
    registry.register(1);
    List<OrderUpdateType> updateTypes = new CopyOnWriteArrayList<>();
    disruptor =
        newDisruptor(
            registry,
            (symbolId, offset, events) -> {
              for (OutboundEvent e : events) {
                if (e instanceof OrderUpdate u && u.orderId() == 1L) {
                  updateTypes.add(u.type());
                }
              }
            });

    disruptor
        .publisher()
        .onValidated(
            new ValidatedInbound(
                new PlaceOrder(1, 1, 1, 1, Side.BID, OrderType.LIMIT, 100, 5), 1, "t", 0, 1L));
    disruptor
        .publisher()
        .onValidated(new ValidatedInbound(new CancelOrder(1, 1, 1, 1), 1, "t", 0, 2L));

    waitUntil(
        () ->
            updateTypes.contains(OrderUpdateType.RESTING)
                && updateTypes.contains(OrderUpdateType.CANCELLED));

    assertTrue(
        updateTypes.indexOf(OrderUpdateType.RESTING)
            < updateTypes.indexOf(OrderUpdateType.CANCELLED));
    assertTrue(registry.get(1).book().isEmpty());
  }

  @Test
  void rejectedOffset_publishedToRing_withoutBookMutation() throws Exception {
    EngineRegistry registry = new EngineRegistry(SystemTimeSource.INSTANCE);
    registry.register(1);
    List<Long> offsets = new CopyOnWriteArrayList<>();
    disruptor = newDisruptor(registry, (symbolId, offset, events) -> offsets.add(offset));

    MatchEventProcessor processor =
        new MatchEventProcessor(
            new SbeMatchEventCodec(),
            new IngressValidator(),
            disruptor.publisher(),
            (message, cause) -> {
              throw new AssertionError("unexpected fatal: " + message, cause);
            },
            disruptor.publisher()::onRejected);

    assertFalse(processor.process(new byte[] {1, 2, 3}, "match_event", 0, 1L));

    waitUntil(() -> offsets.contains(1L));
    assertEquals(List.of(1L), offsets);
    assertTrue(registry.get(1).book().isEmpty());
  }

  @Test
  void applyError_invokesFatalHandler() throws Exception {
    EngineRegistry registry = new EngineRegistry(SystemTimeSource.INSTANCE);
    registry.register(1);
    AtomicReference<Throwable> fatal = new AtomicReference<>();
    FatalFailureHandler capture = (message, cause) -> fatal.set(cause);
    disruptor =
        new MatchIngressDisruptor(
            registry,
            1024,
            MatchIngressDisruptor.WaitMode.BLOCKING,
            null,
            (symbolId, offset, events) -> {
              throw new IllegalStateException("outbound boom");
            },
            capture);
    disruptor.start();

    disruptor
        .publisher()
        .onValidated(
            new ValidatedInbound(
                new PlaceOrder(1, 1, 1, 1, Side.BID, OrderType.LIMIT, 100, 1), 1, "t", 0, 1L));

    waitUntil(() -> fatal.get() != null);
    assertTrue(fatal.get() instanceof IllegalStateException);
  }

  @Test
  void ringFull_waitsThenPublishes_withoutFatal() throws Exception {
    EngineRegistry registry = new EngineRegistry(SystemTimeSource.INSTANCE);
    registry.register(1);

    CountDownLatch blockFirst = new CountDownLatch(1);
    CountDownLatch firstEntered = new CountDownLatch(1);
    AtomicBoolean fatalCalled = new AtomicBoolean(false);
    List<Long> doneOffsets = new CopyOnWriteArrayList<>();

    disruptor =
        new MatchIngressDisruptor(
            registry,
            4,
            MatchIngressDisruptor.WaitMode.BLOCKING,
            null,
            (symbolId, offset, events) -> {
              if (offset == 1L) {
                firstEntered.countDown();
                try {
                  assertTrue(blockFirst.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  throw new RuntimeException(e);
                }
              }
              doneOffsets.add(offset);
            },
            (message, cause) -> fatalCalled.set(true));
    disruptor.start();

    // 占住消费者，再塞满小环（size=4），随后一次 publish 应等待而不是 fatal
    disruptor
        .publisher()
        .onValidated(
            new ValidatedInbound(
                new PlaceOrder(1, 1, 1, 1, Side.BID, OrderType.LIMIT, 100, 1), 1, "t", 0, 1L));
    assertTrue(firstEntered.await(2, TimeUnit.SECONDS));

    for (long off = 2; off <= 4; off++) {
      long orderId = off;
      disruptor
          .publisher()
          .onValidated(
              new ValidatedInbound(
                  new PlaceOrder(1, orderId, orderId, 1, Side.BID, OrderType.LIMIT, 100 + orderId, 1),
                  1,
                  "t",
                  0,
                  off));
    }

    AtomicBoolean publishFinished = new AtomicBoolean(false);
    Thread publisherThread =
        new Thread(
            () -> {
              disruptor
                  .publisher()
                  .onValidated(
                      new ValidatedInbound(
                          new PlaceOrder(1, 99, 99, 1, Side.BID, OrderType.LIMIT, 199, 1),
                          1,
                          "t",
                          0,
                          99L));
              publishFinished.set(true);
            },
            "ring-full-publisher");
    publisherThread.start();

    Thread.sleep(80);
    assertFalse(publishFinished.get(), "publish should block while ring is full");
    assertFalse(fatalCalled.get());

    blockFirst.countDown();
    publisherThread.join(5_000);
    assertFalse(publisherThread.isAlive());
    assertTrue(publishFinished.get());
    assertFalse(fatalCalled.get());

    waitUntil(() -> doneOffsets.contains(99L));
    assertEquals(199, registry.get(1).book().bestBid());
  }


  @Test
  void ringSize_mustBePowerOfTwo() {
    EngineRegistry registry = new EngineRegistry(SystemTimeSource.INSTANCE);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new MatchIngressDisruptor(
                registry, 3, MatchIngressDisruptor.WaitMode.BLOCKING, null));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new MatchIngressDisruptor(
                registry, 0, MatchIngressDisruptor.WaitMode.BLOCKING, null));
  }

  @Test
  void outboundListenerFailure_isFatal_afterBookMutated() throws Exception {
    EngineRegistry registry = new EngineRegistry(SystemTimeSource.INSTANCE);
    registry.register(1);
    AtomicReference<Throwable> fatal = new AtomicReference<>();
    disruptor =
        new MatchIngressDisruptor(
            registry,
            1024,
            MatchIngressDisruptor.WaitMode.BLOCKING,
            null,
            (symbolId, offset, events) -> {
              throw new IllegalStateException("egress down");
            },
            (message, cause) -> fatal.set(cause));
    disruptor.start();

    disruptor
        .publisher()
        .onValidated(
            new ValidatedInbound(
                new PlaceOrder(1, 7, 7, 1, Side.BID, OrderType.LIMIT, 55, 1), 1, "t", 0, 1L));

    waitUntil(() -> fatal.get() != null);
    assertTrue(fatal.get() instanceof IllegalStateException);
    // apply 已成功：簿已变；fatal 保证进程退出，避免在无出站状态下继续撮合
    assertEquals(1, registry.get(1).book().orderCount());
    assertEquals(55, registry.get(1).book().bestBid());
  }

  @Test
  void ringFull_interruptedWhileWaiting_isFatal() throws Exception {
    EngineRegistry registry = new EngineRegistry(SystemTimeSource.INSTANCE);
    registry.register(1);

    CountDownLatch blockFirst = new CountDownLatch(1);
    CountDownLatch firstEntered = new CountDownLatch(1);
    AtomicReference<Throwable> fatal = new AtomicReference<>();

    disruptor =
        new MatchIngressDisruptor(
            registry,
            4,
            MatchIngressDisruptor.WaitMode.BLOCKING,
            null,
            (symbolId, offset, events) -> {
              if (offset == 1L) {
                firstEntered.countDown();
                try {
                  assertTrue(blockFirst.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  throw new RuntimeException(e);
                }
              }
            },
            (message, cause) -> fatal.set(cause));
    disruptor.start();

    disruptor
        .publisher()
        .onValidated(
            new ValidatedInbound(
                new PlaceOrder(1, 1, 1, 1, Side.BID, OrderType.LIMIT, 100, 1), 1, "t", 0, 1L));
    assertTrue(firstEntered.await(2, TimeUnit.SECONDS));

    for (long off = 2; off <= 4; off++) {
      long orderId = off;
      disruptor
          .publisher()
          .onValidated(
              new ValidatedInbound(
                  new PlaceOrder(1, orderId, orderId, 1, Side.BID, OrderType.LIMIT, 100 + orderId, 1),
                  1,
                  "t",
                  0,
                  off));
    }

    AtomicBoolean sawIllegalState = new AtomicBoolean(false);
    Thread publisherThread =
        new Thread(
            () -> {
              try {
                disruptor
                    .publisher()
                    .onValidated(
                        new ValidatedInbound(
                            new PlaceOrder(1, 99, 99, 1, Side.BID, OrderType.LIMIT, 199, 1),
                            1,
                            "t",
                            0,
                            99L));
              } catch (IllegalStateException e) {
                sawIllegalState.set(true);
              }
            },
            "ring-full-interrupt");
    publisherThread.start();
    Thread.sleep(80);
    assertTrue(publisherThread.isAlive());
    publisherThread.interrupt();

    waitUntil(() -> fatal.get() != null);
    publisherThread.join(5_000);
    assertFalse(publisherThread.isAlive());
    assertTrue(sawIllegalState.get());
    assertTrue(
        fatal.get() instanceof InterruptedException,
        "cause=" + fatal.get());
    blockFirst.countDown();
  }

  @Test
  void duplicateOrder_appliedAsReject_notFatal() throws Exception {
    EngineRegistry registry = new EngineRegistry(SystemTimeSource.INSTANCE);
    registry.register(1);
    List<OutboundEvent> all = new CopyOnWriteArrayList<>();
    disruptor = newDisruptor(registry, (symbolId, offset, events) -> all.addAll(events));

    PlaceOrder place = new PlaceOrder(1, 1, 1, 1, Side.BID, OrderType.LIMIT, 100, 1);
    disruptor.publisher().onValidated(new ValidatedInbound(place, 1, "t", 0, 1L));
    disruptor.publisher().onValidated(new ValidatedInbound(place, 1, "t", 0, 2L));

    waitUntil(
        () ->
            all.stream()
                .anyMatch(
                    e ->
                        e instanceof com.kai.matchengine.event.outbound.CommandResult.Place p
                            && !p.accepted()));
    assertEquals(1, registry.get(1).book().orderCount());
  }

  @Test
  void processorWired_rejectStillDoesNotTouchBook_orFatal() throws Exception {
    EngineRegistry registry = new EngineRegistry(SystemTimeSource.INSTANCE);
    registry.register(1);
    AtomicBoolean fatal = new AtomicBoolean(false);
    List<Long> offsets = new CopyOnWriteArrayList<>();
    disruptor =
        new MatchIngressDisruptor(
            registry,
            1024,
            MatchIngressDisruptor.WaitMode.BLOCKING,
            null,
            (symbolId, offset, events) -> offsets.add(offset),
            (message, cause) -> fatal.set(true));
    disruptor.start();

    MatchEventProcessor processor =
        new MatchEventProcessor(
            new SbeMatchEventCodec(),
            new IngressValidator(),
            disruptor.publisher(),
            (message, cause) -> fatal.set(true),
            disruptor.publisher()::onRejected);

    assertFalse(
        processor.process(
            new SbeMatchEventEncoder()
                .encode(new PlaceOrder(1, 1, 1, 1, Side.BID, OrderType.LIMIT, 0, 1)),
            "t",
            0,
            1L));
    waitUntil(() -> !offsets.isEmpty());
    assertFalse(fatal.get());
    assertEquals(List.of(1L), offsets);
    assertTrue(registry.get(1).book().isEmpty());
  }


  private MatchIngressDisruptor newDisruptor(
      EngineRegistry registry, MatchingApplyHandler.OutboundListener listener) {
    MatchIngressDisruptor d =
        new MatchIngressDisruptor(
            registry,
            1024,
            MatchIngressDisruptor.WaitMode.BLOCKING,
            null,
            listener,
            (message, cause) -> {
              throw new AssertionError("unexpected fatal: " + message, cause);
            });
    d.start();
    return d;
  }

  private static void waitUntil(Condition condition) throws InterruptedException {
    long deadline = System.nanoTime() + 2_000_000_000L;
    while (!condition.ok()) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("timeout waiting condition");
      }
      Thread.sleep(5);
    }
  }

  @FunctionalInterface
  private interface Condition {
    boolean ok();
  }
}
