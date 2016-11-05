package com.kingdee.architecture.zkclient;

import org.apache.zookeeper.Watcher;

public abstract interface IZkStateListener {
	public abstract void handleStateChanged(Watcher.Event.KeeperState paramKeeperState) throws Exception;

	public abstract void handleNewSession() throws Exception;
}
