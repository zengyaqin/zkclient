package com.kingdee.architecture.zkclient;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ZkLock extends ReentrantLock {
	private static final long serialVersionUID = 1L;
	private Condition _dataChangedCondition = newCondition();
	private Condition _stateChangedCondition = newCondition();
	private Condition _zNodeEventCondition = newCondition();

	public Condition getDataChangedCondition() {
		return this._dataChangedCondition;
	}

	public Condition getStateChangedCondition() {
		return this._stateChangedCondition;
	}

	public Condition getZNodeEventCondition() {
		return this._zNodeEventCondition;
	}
}
