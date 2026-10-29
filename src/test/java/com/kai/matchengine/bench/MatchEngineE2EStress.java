package com.kai.matchengine.bench;

import com.kai.matchengine.event.inbound.CancelOrder;
import com.kai.matchengine.event.inbound.MatchEvent;
import com.kai.matchengine.event.inbound.OrderType;
import com.kai.matchengine.event.inbound.PlaceOrder;
import com.kai.matchengine.event.outbound.BookUpdate;
import com.kai.matchengine.event.outbound.CommandResult;
import com.kai.matchengine.event.outbound.OrderUpdate;
import com.kai.matchengine.event.outbound.OutboundEvent;
import com.kai.matchengine.event.outbound.Trade;
import com.kai.matchengine.match.MatchingEngine;
import com.kai.matchengine.match.SystemTimeSource;
import com.kai.matchengine.orderbook.Side;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 当前实现的端到端压测：{@code MatchEvent → MatchingEngine.apply → List&lt;OutboundEvent&gt;}。
 *
 * <p>覆盖真实热路径（校验、撮合、出站事件构造、{@code List.copyOf}），不含 Kafka。
 *
 * <pre>
 *   ./mvnw -q -DskipTests test-compile exec:java \
 *     -Dexec.classpathScope=test \
 *     -Dexec.mainClass=com.kai.matchengine.bench.MatchEngineE2EStress \
 *     -Dexec.args="--scenario MIXED --ops 3000000 --warmup 500000"
 * </pre>
 */
public final class MatchEngineE2EStress {

  public enum Scenario {
    /** 挂 / 吃 / 撤混合（默认）。 */
    MIXED,
    /** 只挂不交叉。 */
    REST_ONLY,
    /** 浅层吃单（对侧少量流动性）。 */
    TAKE_SHALLOW,
    /** 深层扫单（预先堆很多档）。 */
    TAKE_DEEP,
    /** 先堆簿再密集撤单。 */
    CANCEL_HEAVY
  }

  private MatchEngineE2EStress() {}

  public static void main(String[] args) {
    Config cfg = Config.parse(args);
    System.out.printf(
        Locale.ROOT,
        "E2E stress: scenario=%s ops=%d warmup=%d seed=%d%n",
        cfg.scenario,
        cfg.ops,
        cfg.warmup,
        cfg.seed);

    MatchingEngine engine = new MatchingEngine("BTC-USDT", SystemTimeSource.INSTANCE);
    Workload workload = new Workload(engine, cfg.seed);

    // 场景预处理（堆簿等）
    workload.prepare(cfg.scenario);

    // warmup
    run(engine, workload, cfg.scenario, cfg.warmup, null);
    System.gc();

    workload.resetCounters();
    LatencyRecorder latency = new LatencyRecorder(Math.min(cfg.ops, 2_000_000));
    long outboundEvents = run(engine, workload, cfg.scenario, cfg.ops, latency);

    double seconds = latency.totalNanos / 1_000_000_000.0;
    double opsPerSec = cfg.ops / seconds;
    System.out.printf(Locale.ROOT, "ops=%d  wall=%.3fs  throughput=%.0f apply/s%n", cfg.ops, seconds, opsPerSec);
    System.out.printf(
        Locale.ROOT,
        "outboundEvents=%d  avgEvents/apply=%.2f  trades=%d  avgTrades/apply=%.3f%n",
        outboundEvents,
        outboundEvents * 1.0 / cfg.ops,
        workload.tradeCount,
        workload.tradeCount * 1.0 / cfg.ops);
    System.out.printf(
        Locale.ROOT,
        "latency_us p50=%.1f p90=%.1f p99=%.1f p999=%.1f max=%.1f%n",
        latency.percentileUs(0.50),
        latency.percentileUs(0.90),
        latency.percentileUs(0.99),
        latency.percentileUs(0.999),
        latency.maxUs());
    System.out.printf(
        Locale.ROOT,
        "book: orders=%d bidLevels=%d askLevels=%d eventId=%d bookVersion=%d sink=%d%n",
        engine.book().orderCount(),
        engine.book().bidLevelCount(),
        engine.book().askLevelCount(),
        engine.lastEventId(),
        engine.lastBookVersion(),
        workload.sink);
  }

  private static long run(
      MatchingEngine engine,
      Workload workload,
      Scenario scenario,
      int ops,
      LatencyRecorder latency) {
    long outbound = 0L;
    for (int i = 0; i < ops; i++) {
      MatchEvent event = workload.next(scenario);
      long t0 = System.nanoTime();
      List<OutboundEvent> out = engine.apply(event);
      long t1 = System.nanoTime();
      if (latency != null) {
        latency.record(t1 - t0);
      }
      outbound += out.size();
      workload.observe(event, out);
    }
    return outbound;
  }

  private record Config(Scenario scenario, int ops, int warmup, long seed) {
    static Config parse(String[] args) {
      Scenario scenario = Scenario.MIXED;
      int ops = 2_000_000;
      int warmup = 200_000;
      long seed = 1L;
      for (int i = 0; i < args.length; i++) {
        switch (args[i]) {
          case "--scenario" -> scenario = Scenario.valueOf(args[++i]);
          case "--ops" -> ops = Integer.parseInt(args[++i]);
          case "--warmup" -> warmup = Integer.parseInt(args[++i]);
          case "--seed" -> seed = Long.parseLong(args[++i]);
          case "--help" -> {
            printHelp();
            System.exit(0);
          }
          default -> throw new IllegalArgumentException("unknown arg: " + args[i]);
        }
      }
      return new Config(scenario, ops, warmup, seed);
    }

    private static void printHelp() {
      System.out.println(
          """
          MatchEngineE2EStress
            --scenario MIXED|REST_ONLY|TAKE_SHALLOW|TAKE_DEEP|CANCEL_HEAVY
            --ops N
            --warmup N
            --seed N
          """);
    }
  }

  /** 生成入站事件并消化出站，防止 JIT 消除。 */
  static final class Workload {
    private final MatchingEngine engine;
    private final ThreadLocalRandom rnd;
    private long nextOrderId = 1;
    /** orderId → userId */
    private final Map<Long, Long> resting = new HashMap<>(1 << 16);
    private final List<Long> restingIds = new java.util.ArrayList<>(1 << 16);
    /** orderId → index in restingIds，保证摘除 O(1) */
    private final Map<Long, Integer> restingIndex = new HashMap<>(1 << 16);
    long sink;
    long tradeCount;
    private boolean trackResting = true;

    /** 中间价附近（tick）。 */
    private static final long MID = 100_000L;

    private int replenishLevels = 50;
    private int minLevels = 10;
    private int takeQtyMax = 5;

    Workload(MatchingEngine engine, long seed) {
      this.engine = engine;
      this.rnd = ThreadLocalRandom.current();
      this.sink = seed;
    }

    void resetCounters() {
      tradeCount = 0L;
    }

    void prepare(Scenario scenario) {
      switch (scenario) {
        case TAKE_SHALLOW -> {
          trackResting = false;
          replenishLevels = 20;
          minLevels = 15;
          takeQtyMax = 8;
          seedLiquidity(20, 5);
        }
        case TAKE_DEEP -> {
          trackResting = false;
          replenishLevels = 200;
          minLevels = 120;
          takeQtyMax = 80;
          seedLiquidity(200, 5);
        }
        case CANCEL_HEAVY -> {
          trackResting = true;
          seedLiquidity(100, 50);
        }
        case MIXED, REST_ONLY -> {
          trackResting = true;
          seedLiquidity(50, 10);
        }
      }
    }

    private void seedLiquidity(int levels, int perLevel) {
      for (int i = 0; i < levels; i++) {
        for (int j = 0; j < perLevel; j++) {
          long bidId = nextOrderId++;
          long askId = nextOrderId++;
          long bidPx = MID - 1 - i;
          long askPx = MID + 1 + i;
          applyTrack(new PlaceOrder(1, bidId, bidId, 1, Side.BID, OrderType.LIMIT, bidPx, 1));
          applyTrack(new PlaceOrder(1, askId, askId, 1, Side.ASK, OrderType.LIMIT, askPx, 1));
        }
      }
    }

    private void applyTrack(PlaceOrder place) {
      List<OutboundEvent> out = engine.apply(place);
      observe(place, out);
    }

    void observe(MatchEvent event, List<OutboundEvent> out) {
      consume(out);
      for (OutboundEvent e : out) {
        if (e instanceof Trade) {
          tradeCount++;
        }
        if (!trackResting) {
          continue;
        }
        if (e instanceof OrderUpdate u) {
          switch (u.type()) {
            case RESTING -> addResting(u.orderId(), u.userId());
            case FILLED, CANCELLED -> removeResting(u.orderId());
            default -> {}
          }
        }
      }
    }

    MatchEvent next(Scenario scenario) {
      return switch (scenario) {
        case REST_ONLY -> restOnly();
        case TAKE_SHALLOW, TAKE_DEEP -> takeWithReplenish();
        case CANCEL_HEAVY -> cancelOrReplenish();
        case MIXED -> mixed();
      };
    }

    /**
     * 持续吃单：簿变薄时先补 maker 流动性（计入 ops），保证 take 路径始终有深度可扫。
     */
    private MatchEvent takeWithReplenish() {
      int bidLv = engine.book().bidLevelCount();
      int askLv = engine.book().askLevelCount();
      if (bidLv < minLevels || askLv < minLevels) {
        return replenishMaker();
      }
      return marketTake(1 + rnd.nextInt(takeQtyMax));
    }

    private MatchEvent replenishMaker() {
      long id = nextOrderId++;
      boolean needBid = engine.book().bidLevelCount() <= engine.book().askLevelCount();
      int offset = rnd.nextInt(Math.max(1, replenishLevels));
      if (needBid) {
        long px = Math.max(1, MID - 1 - offset);
        return new PlaceOrder(1, id, id, 1, Side.BID, OrderType.LIMIT, px, 1 + rnd.nextInt(3));
      }
      long px = MID + 1 + offset;
      return new PlaceOrder(1, id, id, 1, Side.ASK, OrderType.LIMIT, px, 1 + rnd.nextInt(3));
    }

    private MatchEvent restOnly() {
      long id = nextOrderId++;
      boolean bid = rnd.nextBoolean();
      long px = bid ? MID - 1_000 - rnd.nextInt(1_000) : MID + 1_000 + rnd.nextInt(1_000);
      return new PlaceOrder(1, id, id, 1, bid ? Side.BID : Side.ASK, OrderType.LIMIT, px, 1);
    }

    private MatchEvent aggressiveTake() {
      return marketTake(1 + rnd.nextInt(5));
    }

    /** 市价买单的 qty 是计价预算，按当前最优卖价换算，才能在整数档上吃到约 baseQty。 */
    private MatchEvent marketTake(int baseQty) {
      long id = nextOrderId++;
      if (rnd.nextBoolean()) {
        long budget = engine.book().hasBestAsk() ? baseQty * engine.book().bestAsk() : baseQty;
        return new PlaceOrder(1, id, id, 2, Side.BID, OrderType.MARKET, 0, budget);
      }
      return new PlaceOrder(1, id, id, 2, Side.ASK, OrderType.MARKET, 0, baseQty);
    }

    private MatchEvent cancelOrReplenish() {
      if (!restingIds.isEmpty() && rnd.nextInt(100) < 70) {
        int idx = rnd.nextInt(restingIds.size());
        long orderId = restingIds.get(idx);
        Long userId = removeRestingAt(idx);
        return new CancelOrder(1, orderId, orderId, userId);
      }
      return restOnly();
    }

    private MatchEvent mixed() {
      int r = rnd.nextInt(100);
      if (r < 55) {
        long id = nextOrderId++;
        boolean bid = rnd.nextBoolean();
        long px = bid ? MID - rnd.nextInt(50) : MID + rnd.nextInt(50);
        return new PlaceOrder(1, id, id, 1, bid ? Side.BID : Side.ASK, OrderType.LIMIT, Math.max(1, px), 1);
      }
      if (r < 75 && !restingIds.isEmpty()) {
        int idx = rnd.nextInt(restingIds.size());
        long orderId = restingIds.get(idx);
        Long userId = removeRestingAt(idx);
        return new CancelOrder(1, orderId, orderId, userId);
      }
      return aggressiveTake();
    }

    private void addResting(long orderId, long userId) {
      if (resting.put(orderId, userId) == null) {
        restingIndex.put(orderId, restingIds.size());
        restingIds.add(orderId);
      }
    }

    private void removeResting(long orderId) {
      if (resting.remove(orderId) == null) {
        return;
      }
      Integer idxObj = restingIndex.remove(orderId);
      if (idxObj == null) {
        return;
      }
      int idx = idxObj;
      int last = restingIds.size() - 1;
      if (idx != last) {
        long moved = restingIds.get(last);
        restingIds.set(idx, moved);
        restingIndex.put(moved, idx);
      }
      restingIds.remove(last);
    }

    private Long removeRestingAt(int idx) {
      long orderId = restingIds.get(idx);
      Long userId = resting.remove(orderId);
      restingIndex.remove(orderId);
      int last = restingIds.size() - 1;
      if (idx != last) {
        long moved = restingIds.get(last);
        restingIds.set(idx, moved);
        restingIndex.put(moved, idx);
      }
      restingIds.remove(last);
      return userId;
    }

    void consume(List<OutboundEvent> out) {
      for (OutboundEvent e : out) {
        sink +=
            switch (e) {
              case Trade t -> t.eventId() ^ t.qty() ^ t.priceTick();
              case OrderUpdate u -> u.eventId() ^ u.remainingQty() ^ u.type().ordinal();
              case BookUpdate b -> b.bookVersion() ^ b.levelQty() ^ b.priceTick();
              case CommandResult.Place p -> p.orderId() ^ p.filledQty() ^ (p.accepted() ? 1 : 0);
              case CommandResult.Cancel c -> c.orderId() ^ (c.accepted() ? 1 : 0);
              default -> 1;
            };
      }
    }
  }

  static final class LatencyRecorder {
    private final long[] samples;
    private int size;
    private long totalNanos;
    private long maxNanos;

    LatencyRecorder(int capacity) {
      this.samples = new long[capacity];
    }

    void record(long nanos) {
      totalNanos += nanos;
      if (nanos > maxNanos) {
        maxNanos = nanos;
      }
      if (size < samples.length) {
        samples[size++] = nanos;
      } else {
        // 水库抽样简化：随机覆盖
        int i = ThreadLocalRandom.current().nextInt(size + 1);
        if (i < samples.length) {
          samples[i] = nanos;
        }
      }
    }

    double percentileUs(double p) {
      if (size == 0) {
        return 0;
      }
      long[] copy = Arrays.copyOf(samples, size);
      Arrays.sort(copy);
      int idx = Math.min(size - 1, (int) Math.ceil(p * size) - 1);
      if (idx < 0) {
        idx = 0;
      }
      return copy[idx] / 1_000.0;
    }

    double maxUs() {
      return maxNanos / 1_000.0;
    }
  }
}
