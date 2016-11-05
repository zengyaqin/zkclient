package com.kingdee.architecture.zkclient;

import com.kingdee.architecture.zkclient.exception.ZkInterruptedException;

public class ExceptionUtil {
	public static RuntimeException convertToRuntimeException(Throwable e) {
		if ((e instanceof RuntimeException)) {
			return (RuntimeException) e;
		}
		retainInterruptFlag(e);
		return new RuntimeException(e);
	}

	public static void retainInterruptFlag(Throwable catchedException) {
		if ((catchedException instanceof InterruptedException)) {
			Thread.currentThread().interrupt();
		}
	}

	public static void rethrowInterruptedException(Throwable e) throws InterruptedException {
		if ((e instanceof InterruptedException)) {
			throw ((InterruptedException) e);
		}
		if ((e instanceof ZkInterruptedException)) {
			throw ((ZkInterruptedException) e);
		}
	}
}
