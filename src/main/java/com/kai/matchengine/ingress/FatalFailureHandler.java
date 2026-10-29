package com.kai.matchengine.ingress;

/**
 * 不可恢复失败处理。生产默认 {@link #SYSTEM_EXIT}；单测可替换以免干掉 JVM。
 *
 * <p>{@link #SYSTEM_EXIT} 在 {@code System.exit} 之后仍会 {@code halt}，防止 exit 未真正终止进程时继续撮合。
 */
@FunctionalInterface
public interface FatalFailureHandler {

  FatalFailureHandler SYSTEM_EXIT =
      (message, cause) -> {
        System.exit(1);
        Runtime.getRuntime().halt(1);
      };

  void onFatal(String message, Throwable cause);
}
