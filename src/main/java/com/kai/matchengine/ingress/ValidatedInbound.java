package com.kai.matchengine.ingress;

import com.kai.matchengine.event.inbound.MatchEvent;

/**
 * 校验通过的入站命令（尚未进入撮合前的载体）。
 *
 * @param event 领域入站事件
 * @param symbolId 交易对 id
 * @param topic Kafka topic
 * @param partition 分区
 * @param offset 记录 offset
 */
public record ValidatedInbound(
    MatchEvent event, int symbolId, String topic, int partition, long offset) {}
