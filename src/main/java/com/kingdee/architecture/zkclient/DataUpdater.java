package com.kingdee.architecture.zkclient;

public abstract interface DataUpdater<T> {
	public abstract T update(T paramT);
}
