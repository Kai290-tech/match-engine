package com.kai.matchengine.event.inbound;

/**
 * 撤单命令。
 *
 * @param symbolId 交易对 id
 * @param clientRequestId 上游请求关联 id（原样回写 event_result）
 * @param orderId 订单 id
 * @param userId 发起撤单的用户（须与挂单用户一致）
 */
public record CancelOrder(int symbolId, long clientRequestId, long orderId, long userId)
    implements MatchEvent {}
