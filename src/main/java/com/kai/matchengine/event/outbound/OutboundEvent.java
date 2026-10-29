package com.kai.matchengine.event.outbound;

/**
 * 撮合对外输出事件的统一标记。
 *
 * <p>每种输出类型独立模型；撮合过程中一旦触发即追加一条，由上层按类型发到对应 Kafka topic。
 */
public sealed interface OutboundEvent
    permits Trade, OrderUpdate, BookUpdate, BookSnapshot, CommandResult {}
