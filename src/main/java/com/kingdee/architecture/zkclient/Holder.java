package com.kingdee.architecture.zkclient;

public class Holder<T> {
	private T _value;

	public Holder() {
	}

	public Holder(T value) {
		this._value = value;
	}

	public T get() {
		return this._value;
	}

	public void set(T value) {
		this._value = value;
	}
}
