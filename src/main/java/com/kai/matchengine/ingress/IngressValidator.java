package com.kai.matchengine.ingress;

import com.kai.matchengine.event.inbound.CancelOrder;
import com.kai.matchengine.event.inbound.CreateSymbol;
import com.kai.matchengine.event.inbound.HaltSymbol;
import com.kai.matchengine.event.inbound.MatchEvent;
import com.kai.matchengine.event.inbound.OrderType;
import com.kai.matchengine.event.inbound.PlaceOrder;
import com.kai.matchengine.event.inbound.ResumeSymbol;
import com.kai.matchengine.event.inbound.UpdateSymbol;
import java.util.Objects;

/**
 * 入站结构校验。非法消息不得进入撮合。
 *
 * <p>交易对是否存在 / 是否可交易在 apply 侧以 {@code CommandResult} 拒绝（与订单业务拒单一致），
 * 避免 Create 与 Place 跨 Disruptor 线程的竞态。
 */
public final class IngressValidator {

  public void validate(MatchEvent event) throws IngressException {
    Objects.requireNonNull(event, "event");
    switch (event) {
      case PlaceOrder p -> validatePlace(p);
      case CancelOrder c -> validateCancel(c);
      case CreateSymbol c -> validateCreate(c);
      case UpdateSymbol u -> validateUpdate(u);
      case HaltSymbol h -> validateHalt(h);
      case ResumeSymbol r -> validateResume(r);
    }
  }

  private void validatePlace(PlaceOrder p) throws IngressException {
    requirePositiveSymbolId(p.symbolId());
    if (p.side() == null || p.orderType() == null) {
      throw new IngressException(IngressRejectReason.INVALID_ENUM, "side/orderType required");
    }
    if (p.qty() <= 0) {
      throw new IngressException(IngressRejectReason.INVALID_QTY, "qty must be > 0");
    }
    if (p.orderType() == OrderType.LIMIT && p.priceTick() <= 0) {
      throw new IngressException(IngressRejectReason.INVALID_PRICE, "limit priceTick must be > 0");
    }
  }

  private void validateCancel(CancelOrder c) throws IngressException {
    requirePositiveSymbolId(c.symbolId());
  }

  private void validateCreate(CreateSymbol c) throws IngressException {
    requirePositiveSymbolId(c.symbolId());
    requireValidCode(c.code());
  }

  private void validateUpdate(UpdateSymbol u) throws IngressException {
    requirePositiveSymbolId(u.symbolId());
    requireValidCode(u.code());
  }

  private void validateHalt(HaltSymbol h) throws IngressException {
    requirePositiveSymbolId(h.symbolId());
  }

  private void validateResume(ResumeSymbol r) throws IngressException {
    requirePositiveSymbolId(r.symbolId());
  }

  private void requireValidCode(String code) throws IngressException {
    if (code == null || code.isBlank()) {
      throw new IngressException(IngressRejectReason.INVALID_SYMBOL_CODE, "code required");
    }
    if (code.trim().length() > 16) {
      throw new IngressException(IngressRejectReason.INVALID_SYMBOL_CODE, "code length > 16");
    }
  }

  private void requirePositiveSymbolId(int symbolId) throws IngressException {
    if (symbolId <= 0) {
      throw new IngressException(IngressRejectReason.INVALID_SYMBOL, "symbolId must be > 0");
    }
  }
}
