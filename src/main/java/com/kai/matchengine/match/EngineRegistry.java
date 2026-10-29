package com.kai.matchengine.match;

import com.kai.matchengine.event.inbound.CancelOrder;
import com.kai.matchengine.event.inbound.CreateSymbol;
import com.kai.matchengine.event.inbound.HaltSymbol;
import com.kai.matchengine.event.inbound.InboundEventType;
import com.kai.matchengine.event.inbound.MatchEvent;
import com.kai.matchengine.event.inbound.PlaceOrder;
import com.kai.matchengine.event.inbound.ResumeSymbol;
import com.kai.matchengine.event.inbound.UpdateSymbol;
import com.kai.matchengine.event.outbound.CommandResult;
import com.kai.matchengine.event.outbound.OutboundEvent;
import com.kai.matchengine.event.outbound.RejectReason;
import com.kai.matchengine.snapshot.delta.BookDelta;
import com.kai.matchengine.snapshot.delta.MatchDeltaContext;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 多 symbol 撮合路由：{@code symbolId → MatchingEngine}，并维护 {@link SymbolRegistry}。
 *
 * <p>单线程 apply；可用 {@link #register} 做测试预置，生产路径靠快照恢复 + {@code CreateSymbol}。
 */
public final class EngineRegistry {

  private static final Logger log = LoggerFactory.getLogger(EngineRegistry.class);

  private final Map<Integer, MatchingEngine> engines = new ConcurrentHashMap<>();
  private final SymbolRegistry symbols = new SymbolRegistry();
  private final TimeSource timeSource;
  private final int orderNodePoolCapacity;
  private final com.kai.matchengine.snapshot.delta.BookDeltaSink bookDeltaSink;

  public EngineRegistry(TimeSource timeSource) {
    this(timeSource, com.kai.matchengine.orderbook.OrderNodePool.DEFAULT_CAPACITY, null);
  }

  public EngineRegistry(TimeSource timeSource, int orderNodePoolCapacity) {
    this(timeSource, orderNodePoolCapacity, null);
  }

  public EngineRegistry(
      TimeSource timeSource,
      int orderNodePoolCapacity,
      com.kai.matchengine.snapshot.delta.BookDeltaSink bookDeltaSink) {
    this.timeSource = Objects.requireNonNull(timeSource, "timeSource");
    if (orderNodePoolCapacity < 1) {
      throw new IllegalArgumentException("orderNodePoolCapacity must be >= 1");
    }
    this.orderNodePoolCapacity = orderNodePoolCapacity;
    this.bookDeltaSink = bookDeltaSink;
  }

  public EngineRegistry() {
    this(SystemTimeSource.INSTANCE);
  }

  public SymbolRegistry symbols() {
    return symbols;
  }

  public TimeSource timeSource() {
    return timeSource;
  }

  /** 测试 / 快照恢复预置：注册交易对（code 默认 {@code S-{id}}）。 */
  public MatchingEngine register(int symbolId) {
    return register(symbolId, "S-" + symbolId, SymbolStatus.TRADING);
  }

  public MatchingEngine register(int symbolId, String code, SymbolStatus status) {
    if (symbolId <= 0) {
      throw new IllegalArgumentException("symbolId must be > 0");
    }
    SymbolInfo info = new SymbolInfo(symbolId, code, status);
    symbols.put(info);
    return engines.computeIfAbsent(
        symbolId,
        id -> new MatchingEngine(info.code(), symbolId, timeSource, orderNodePoolCapacity, bookDeltaSink));
  }

  /** 所有 symbol 中最大池占用率（全量 compaction 门控）。 */
  public double maxBookFillRatio() {
    double max = 0.0;
    for (Integer symbolId : symbolIds()) {
      MatchingEngine engine = engines.get(symbolId);
      if (engine == null) {
        continue;
      }
      var pool = engine.book().pool();
      max = Math.max(max, (double) pool.inUseCount() / pool.capacity());
    }
    return max;
  }

  /** 快照恢复：写入元数据并确保引擎存在（簿随后 restore）。 */
  public MatchingEngine restoreSymbol(int symbolId, String code, SymbolStatus status) {
    return register(symbolId, code, status);
  }

  public MatchingEngine get(int symbolId) {
    return engines.get(symbolId);
  }

  public boolean contains(int symbolId) {
    return symbols.contains(symbolId);
  }

  public Set<Integer> symbolIds() {
    return symbols.symbolIds();
  }

  /** 按事件中的 symbolId 路由；Symbol 管理命令在本类处理。 */
  public List<OutboundEvent> apply(MatchEvent event) {
    return switch (event) {
      case CreateSymbol c -> applyCreate(c);
      case UpdateSymbol u -> applyUpdate(u);
      case HaltSymbol h -> applyHalt(h);
      case ResumeSymbol r -> applyResume(r);
      case PlaceOrder p -> applyPlace(p);
      case CancelOrder c -> applyCancel(c);
    };
  }

  private List<OutboundEvent> applyCreate(CreateSymbol cmd) {
    long now = timeSource.currentTimeMillis();
    String code = cmd.code() == null ? "" : cmd.code().trim();
    if (cmd.symbolId() <= 0) {
      return List.of(
          CommandResult.Symbol.reject(
              InboundEventType.CREATE_SYMBOL,
              cmd.clientRequestId(),
              now,
              code,
              cmd.symbolId(),
              RejectReason.UNKNOWN_SYMBOL));
    }
    if (code.isEmpty() || code.length() > 16) {
      return List.of(
          CommandResult.Symbol.reject(
              InboundEventType.CREATE_SYMBOL,
              cmd.clientRequestId(),
              now,
              code,
              cmd.symbolId(),
              RejectReason.INVALID_SYMBOL_CODE));
    }
    if (symbols.contains(cmd.symbolId())) {
      SymbolInfo existing = symbols.get(cmd.symbolId());
      log.info(
          "symbol CREATE reject ALREADY_EXISTS symbolId={} code={} clientRequestId={}",
          cmd.symbolId(),
          existing.code(),
          cmd.clientRequestId());
      return List.of(
          CommandResult.Symbol.reject(
              InboundEventType.CREATE_SYMBOL,
              cmd.clientRequestId(),
              now,
              existing.code(),
              cmd.symbolId(),
              RejectReason.SYMBOL_ALREADY_EXISTS));
    }
    register(cmd.symbolId(), code, SymbolStatus.TRADING);
    log.info(
        "symbol CREATE ok symbolId={} code={} clientRequestId={}",
        cmd.symbolId(),
        code,
        cmd.clientRequestId());
    return List.of(
        CommandResult.Symbol.ok(
            InboundEventType.CREATE_SYMBOL,
            cmd.clientRequestId(),
            now,
            code,
            cmd.symbolId(),
            SymbolStatus.TRADING));
  }

  private List<OutboundEvent> applyUpdate(UpdateSymbol cmd) {
    long now = timeSource.currentTimeMillis();
    String code = cmd.code() == null ? "" : cmd.code().trim();
    SymbolInfo info = symbols.get(cmd.symbolId());
    if (info == null) {
      return List.of(
          CommandResult.Symbol.reject(
              InboundEventType.UPDATE_SYMBOL,
              cmd.clientRequestId(),
              now,
              code,
              cmd.symbolId(),
              RejectReason.UNKNOWN_SYMBOL));
    }
    if (code.isEmpty() || code.length() > 16) {
      return List.of(
          CommandResult.Symbol.reject(
              InboundEventType.UPDATE_SYMBOL,
              cmd.clientRequestId(),
              now,
              info.code(),
              cmd.symbolId(),
              RejectReason.INVALID_SYMBOL_CODE));
    }
    // code / symbolId 创建后不可变；同 code 视为幂等成功
    if (!info.code().equals(code)) {
      log.info(
          "symbol UPDATE reject CODE_IMMUTABLE symbolId={} existing={} requested={} clientRequestId={}",
          cmd.symbolId(),
          info.code(),
          code,
          cmd.clientRequestId());
      return List.of(
          CommandResult.Symbol.reject(
              InboundEventType.UPDATE_SYMBOL,
              cmd.clientRequestId(),
              now,
              info.code(),
              cmd.symbolId(),
              RejectReason.SYMBOL_CODE_IMMUTABLE));
    }
    log.info(
        "symbol UPDATE ok (noop) symbolId={} code={} clientRequestId={}",
        cmd.symbolId(),
        info.code(),
        cmd.clientRequestId());
    return List.of(
        CommandResult.Symbol.ok(
            InboundEventType.UPDATE_SYMBOL,
            cmd.clientRequestId(),
            now,
            info.code(),
            cmd.symbolId(),
            info.status()));
  }

  private List<OutboundEvent> applyHalt(HaltSymbol cmd) {
    long now = timeSource.currentTimeMillis();
    SymbolInfo info = symbols.get(cmd.symbolId());
    if (info == null) {
      return List.of(
          CommandResult.Symbol.reject(
              InboundEventType.HALT_SYMBOL,
              cmd.clientRequestId(),
              now,
              "",
              cmd.symbolId(),
              RejectReason.UNKNOWN_SYMBOL));
    }
    if (info.status() != SymbolStatus.HALTED) {
      symbols.setStatus(cmd.symbolId(), SymbolStatus.HALTED);
      info = symbols.get(cmd.symbolId());
      recordSymbolStatusDelta(cmd.symbolId(), SymbolStatus.HALTED);
    }
    log.info(
        "symbol HALT ok symbolId={} code={} clientRequestId={}",
        cmd.symbolId(),
        info.code(),
        cmd.clientRequestId());
    return List.of(
        CommandResult.Symbol.ok(
            InboundEventType.HALT_SYMBOL,
            cmd.clientRequestId(),
            now,
            info.code(),
            cmd.symbolId(),
            SymbolStatus.HALTED));
  }

  private List<OutboundEvent> applyResume(ResumeSymbol cmd) {
    long now = timeSource.currentTimeMillis();
    SymbolInfo info = symbols.get(cmd.symbolId());
    if (info == null) {
      return List.of(
          CommandResult.Symbol.reject(
              InboundEventType.RESUME_SYMBOL,
              cmd.clientRequestId(),
              now,
              "",
              cmd.symbolId(),
              RejectReason.UNKNOWN_SYMBOL));
    }
    if (info.status() != SymbolStatus.TRADING) {
      symbols.setStatus(cmd.symbolId(), SymbolStatus.TRADING);
      info = symbols.get(cmd.symbolId());
      recordSymbolStatusDelta(cmd.symbolId(), SymbolStatus.TRADING);
    }
    log.info(
        "symbol RESUME ok symbolId={} code={} clientRequestId={}",
        cmd.symbolId(),
        info.code(),
        cmd.clientRequestId());
    return List.of(
        CommandResult.Symbol.ok(
            InboundEventType.RESUME_SYMBOL,
            cmd.clientRequestId(),
            now,
            info.code(),
            cmd.symbolId(),
            SymbolStatus.TRADING));
  }

  private List<OutboundEvent> applyPlace(PlaceOrder cmd) {
    long now = timeSource.currentTimeMillis();
    SymbolInfo info = symbols.get(cmd.symbolId());
    if (info == null) {
      log.info(
          "place REJECT UNKNOWN_SYMBOL symbolId={} orderId={} clientRequestId={}",
          cmd.symbolId(),
          cmd.orderId(),
          cmd.clientRequestId());
      return List.of(
          CommandResult.Place.reject(
              cmd.clientRequestId(),
              now,
              "",
              cmd.orderId(),
              RejectReason.UNKNOWN_SYMBOL));
    }
    if (!info.canPlace()) {
      log.info(
          "place REJECT NOT_TRADABLE symbolId={} code={} status={} orderId={} clientRequestId={}",
          cmd.symbolId(),
          info.code(),
          info.status(),
          cmd.orderId(),
          cmd.clientRequestId());
      return List.of(
          CommandResult.Place.reject(
              cmd.clientRequestId(),
              now,
              info.code(),
              cmd.orderId(),
              RejectReason.SYMBOL_NOT_TRADABLE));
    }
    MatchingEngine engine = engines.get(cmd.symbolId());
    if (engine == null) {
      log.info(
          "place REJECT engine-missing symbolId={} code={} orderId={} clientRequestId={}",
          cmd.symbolId(),
          info.code(),
          cmd.orderId(),
          cmd.clientRequestId());
      return List.of(
          CommandResult.Place.reject(
              cmd.clientRequestId(),
              now,
              info.code(),
              cmd.orderId(),
              RejectReason.UNKNOWN_SYMBOL));
    }
    return engine.apply(cmd);
  }

  private List<OutboundEvent> applyCancel(CancelOrder cmd) {
    long now = timeSource.currentTimeMillis();
    SymbolInfo info = symbols.get(cmd.symbolId());
    if (info == null) {
      log.info(
          "cancel REJECT UNKNOWN_SYMBOL symbolId={} orderId={} clientRequestId={}",
          cmd.symbolId(),
          cmd.orderId(),
          cmd.clientRequestId());
      return List.of(
          CommandResult.Cancel.reject(
              cmd.clientRequestId(),
              now,
              "",
              cmd.orderId(),
              RejectReason.UNKNOWN_SYMBOL));
    }
    if (!info.canCancel()) {
      log.info(
          "cancel REJECT NOT_TRADABLE symbolId={} code={} status={} orderId={} clientRequestId={}",
          cmd.symbolId(),
          info.code(),
          info.status(),
          cmd.orderId(),
          cmd.clientRequestId());
      return List.of(
          CommandResult.Cancel.reject(
              cmd.clientRequestId(),
              now,
              info.code(),
              cmd.orderId(),
              RejectReason.SYMBOL_NOT_TRADABLE));
    }
    MatchingEngine engine = engines.get(cmd.symbolId());
    if (engine == null) {
      log.info(
          "cancel REJECT engine-missing symbolId={} code={} orderId={} clientRequestId={}",
          cmd.symbolId(),
          info.code(),
          cmd.orderId(),
          cmd.clientRequestId());
      return List.of(
          CommandResult.Cancel.reject(
              cmd.clientRequestId(),
              now,
              info.code(),
              cmd.orderId(),
              RejectReason.UNKNOWN_SYMBOL));
    }
    return engine.apply(cmd);
  }

  private void recordSymbolStatusDelta(int symbolId, SymbolStatus status) {
    if (bookDeltaSink != null && symbolId > 0) {
      bookDeltaSink.append(BookDelta.metaSymbolStatus(MatchDeltaContext.currentOffset(), symbolId, status));
    }
  }
}
