package com.kingdee.architecture.zkclient;

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public abstract interface IZkConnection {
	public abstract void connect(Watcher paramWatcher);

	public abstract void close() throws InterruptedException;

	public abstract String create(String paramString, byte[] paramArrayOfByte, CreateMode paramCreateMode)
			throws KeeperException, InterruptedException;

	public abstract void delete(String paramString) throws InterruptedException, KeeperException;

	public abstract boolean exists(String paramString, boolean paramBoolean)
			throws KeeperException, InterruptedException;

	public abstract List<String> getChildren(String paramString, boolean paramBoolean)
			throws KeeperException, InterruptedException;

	public abstract byte[] readData(String paramString, Stat paramStat, boolean paramBoolean)
			throws KeeperException, InterruptedException;

	public abstract Stat writeData(String paramString, byte[] paramArrayOfByte, int paramInt)
			throws KeeperException, InterruptedException;

	public abstract ZooKeeper.States getZookeeperState();

	public abstract long getCreateTime(String paramString) throws KeeperException, InterruptedException;

	public abstract String getServers();
}
