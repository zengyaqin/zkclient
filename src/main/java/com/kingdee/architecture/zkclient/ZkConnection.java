package com.kingdee.architecture.zkclient;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.kingdee.architecture.zkclient.exception.ZkException;

public class ZkConnection implements IZkConnection {
	private static final Logger LOG = Logger.getLogger(ZkConnection.class);
	private static final int DEFAULT_SESSION_TIMEOUT = 30000;
	private ZooKeeper _zk = null;
	private Lock _zookeeperLock = new ReentrantLock();
	private final String _servers;
	private final int _sessionTimeOut;

	public ZkConnection(String zkServers) {
		this(zkServers, 30000);
	}

	public ZkConnection(String zkServers, int sessionTimeOut) {
		this._servers = zkServers;
		this._sessionTimeOut = sessionTimeOut;
	}

	public void connect(Watcher watcher) {
		this._zookeeperLock.lock();
		try {
			if (this._zk != null) {
				throw new IllegalStateException("zk client has already been started");
			}
			try {
				LOG.debug("Creating new ZookKeeper instance to connect to " + this._servers + ".");
				this._zk = new ZooKeeper(this._servers, this._sessionTimeOut, watcher);
			} catch (IOException e) {
				throw new ZkException("Unable to connect to " + this._servers, e);
			}
		} finally {
			this._zookeeperLock.unlock();
		}
	}

	public void close() throws InterruptedException {
		this._zookeeperLock.lock();
		try {
			if (this._zk != null) {
				LOG.debug("Closing ZooKeeper connected to " + this._servers);
				this._zk.close();
				this._zk = null;
			}
		} finally {
			this._zookeeperLock.unlock();
		}
	}

	public String create(String path, byte[] data, CreateMode mode) throws KeeperException, InterruptedException {
		return this._zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
	}

	public void delete(String path) throws InterruptedException, KeeperException {
		this._zk.delete(path, -1);
	}

	public boolean exists(String path, boolean watch) throws KeeperException, InterruptedException {
		return this._zk.exists(path, watch) != null;
	}

	public List<String> getChildren(String path, boolean watch) throws KeeperException, InterruptedException {
		return this._zk.getChildren(path, watch);
	}

	public byte[] readData(String path, Stat stat, boolean watch) throws KeeperException, InterruptedException {
		return this._zk.getData(path, watch, stat);
	}

	public Stat writeData(String path, byte[] data) throws KeeperException, InterruptedException {
		return writeData(path, data, -1);
	}

	public Stat writeData(String path, byte[] data, int version) throws KeeperException, InterruptedException {
		return this._zk.setData(path, data, version);
	}

	public ZooKeeper.States getZookeeperState() {
		return this._zk != null ? this._zk.getState() : null;
	}

	public ZooKeeper getZookeeper() {
		return this._zk;
	}

	public long getCreateTime(String path) throws KeeperException, InterruptedException {
		Stat stat = this._zk.exists(path, false);
		if (stat != null) {
			return stat.getCtime();
		}
		return -1L;
	}

	public String getServers() {
		return this._servers;
	}
}
