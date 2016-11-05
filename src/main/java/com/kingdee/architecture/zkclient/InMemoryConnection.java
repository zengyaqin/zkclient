package com.kingdee.architecture.zkclient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.kingdee.architecture.zkclient.exception.ZkException;
import com.kingdee.architecture.zkclient.exception.ZkInterruptedException;
import com.kingdee.architecture.zkclient.exception.ZkNoNodeException;
import com.kingdee.architecture.zkclient.util.ZkPathUtil;

public class InMemoryConnection implements IZkConnection {
	public static class DataAndVersion {
		private byte[] _data;
		private int _version;

		public DataAndVersion(byte[] data, int version) {
			this._data = data;
			this._version = version;
		}

		public byte[] getData() {
			return this._data;
		}

		public int getVersion() {
			return this._version;
		}
	}

	private Lock _lock = new ReentrantLock(true);
	private Map<String, DataAndVersion> _data = new HashMap();
	private Map<String, Long> _creationTime = new HashMap();
	private final AtomicInteger sequence = new AtomicInteger(0);
	private Set<String> _dataWatches = new HashSet();
	private Set<String> _nodeWatches = new HashSet();
	private EventThread _eventThread;

	private class EventThread extends Thread {
		private Watcher _watcher;
		private BlockingQueue<WatchedEvent> _blockingQueue = new LinkedBlockingDeque();

		public EventThread(Watcher watcher) {
			this._watcher = watcher;
		}

		public void run() {
			try {
				for (;;) {
					this._watcher.process((WatchedEvent) this._blockingQueue.take());
				}
			} catch (InterruptedException e) {
			}
		}

		public void send(WatchedEvent event) {
			this._blockingQueue.add(event);
		}
	}

	public InMemoryConnection() {
		try {
			create("/", null, CreateMode.PERSISTENT);
		} catch (KeeperException e) {
			throw ZkException.create(e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new ZkInterruptedException(e);
		}
	}

	public void close() throws InterruptedException {
		this._lock.lockInterruptibly();
		try {
			if (this._eventThread != null) {
				this._eventThread.interrupt();
				this._eventThread.join();
				this._eventThread = null;
			}
		} finally {
			this._lock.unlock();
		}
	}

	public void connect(Watcher watcher) {
		this._lock.lock();
		try {
			if (this._eventThread != null) {
				throw new IllegalStateException("Already connected.");
			}
			this._eventThread = new EventThread(watcher);
			this._eventThread.start();
			this._eventThread.send(new WatchedEvent(null, Watcher.Event.KeeperState.SyncConnected, null));
		} finally {
			this._lock.unlock();
		}
	}

	public String create(String path, byte[] data, CreateMode mode) throws KeeperException, InterruptedException {
		this._lock.lock();
		try {
			if (mode.isSequential()) {
				int newSequence = this.sequence.getAndIncrement();
				path = path + ZkPathUtil.leadingZeros(newSequence, 10);
			}
			if (exists(path, false)) {
				throw new KeeperException.NodeExistsException();
			}
			this._data.put(path, new DataAndVersion(data, 0));
			this._creationTime.put(path, Long.valueOf(System.currentTimeMillis()));
			checkWatch(this._nodeWatches, path, Watcher.Event.EventType.NodeCreated);

			String parentPath = getParentPath(path);
			if (parentPath != null) {
				checkWatch(this._nodeWatches, parentPath, Watcher.Event.EventType.NodeChildrenChanged);
			}
			return path;
		} finally {
			this._lock.unlock();
		}
	}

	private String getParentPath(String path) {
		int lastIndexOf = path.lastIndexOf("/");
		if ((lastIndexOf == -1) || (lastIndexOf == 0)) {
			return null;
		}
		return path.substring(0, lastIndexOf);
	}

	public void delete(String path) throws InterruptedException, KeeperException {
		this._lock.lock();
		try {
			if (!exists(path, false)) {
				throw new KeeperException.NoNodeException();
			}
			this._data.remove(path);
			this._creationTime.remove(path);
			checkWatch(this._nodeWatches, path, Watcher.Event.EventType.NodeDeleted);
			String parentPath = getParentPath(path);
			if (parentPath != null) {
				checkWatch(this._nodeWatches, parentPath, Watcher.Event.EventType.NodeChildrenChanged);
			}
		} finally {
			this._lock.unlock();
		}
	}

	public boolean exists(String path, boolean watch) throws KeeperException, InterruptedException {
		this._lock.lock();
		try {
			if (watch) {
				installWatch(this._nodeWatches, path);
			}
			return this._data.containsKey(path);
		} finally {
			this._lock.unlock();
		}
	}

	private void installWatch(Set<String> watches, String path) {
		watches.add(path);
	}

	public List<String> getChildren(String path, boolean watch) throws KeeperException, InterruptedException {
		if (!exists(path, false)) {
			throw KeeperException.create(KeeperException.Code.NONODE, path);
		}
		if ((exists(path, false)) && (watch)) {
			installWatch(this._nodeWatches, path);
		}
		ArrayList<String> children = new ArrayList();
		String[] directoryStack = path.split("/");
		Set<String> keySet = this._data.keySet();
		for (String string : keySet) {
			if (string.startsWith(path)) {
				String[] stack = string.split("/");
				if (stack.length == directoryStack.length + 1) {
					children.add(stack[(stack.length - 1)]);
				}
			}
		}
		return children;
	}

	public ZooKeeper.States getZookeeperState() {
		this._lock.lock();
		try {
			ZooKeeper.States localStates;
			if (this._eventThread == null) {
				return ZooKeeper.States.CLOSED;
			}
			return ZooKeeper.States.CONNECTED;
		} finally {
			this._lock.unlock();
		}
	}

	public byte[] readData(String path, Stat stat, boolean watch) throws KeeperException, InterruptedException {
		if (watch) {
			installWatch(this._dataWatches, path);
		}
		this._lock.lock();
		try {
			DataAndVersion dataAndVersion = (DataAndVersion) this._data.get(path);
			if (dataAndVersion == null) {
				throw new ZkNoNodeException(new KeeperException.NoNodeException());
			}
			byte[] bs = dataAndVersion.getData();
			if (stat != null) {
				stat.setVersion(dataAndVersion.getVersion());
			}
			return bs;
		} finally {
			this._lock.unlock();
		}
	}

	public Stat writeData(String path, byte[] data, int expectedVersion) throws KeeperException, InterruptedException {
		int newVersion = -1;
		this._lock.lock();
		try {
			checkWatch(this._dataWatches, path, Watcher.Event.EventType.NodeDataChanged);
			if (!exists(path, false)) {
				throw new KeeperException.NoNodeException();
			}
			newVersion = ((DataAndVersion) this._data.get(path)).getVersion() + 1;
			this._data.put(path, new DataAndVersion(data, newVersion));
			String parentPath = getParentPath(path);
			if (parentPath != null) {
				checkWatch(this._nodeWatches, parentPath, Watcher.Event.EventType.NodeChildrenChanged);
			}
		} finally {
			this._lock.unlock();
		}
		Stat stat = new Stat();
		stat.setVersion(newVersion);
		return stat;
	}

	private void checkWatch(Set<String> watches, String path, Watcher.Event.EventType eventType) {
		if (watches.contains(path)) {
			watches.remove(path);
			this._eventThread.send(new WatchedEvent(eventType, Watcher.Event.KeeperState.SyncConnected, path));
		}
	}

	public long getCreateTime(String path) {
		Long time = (Long) this._creationTime.get(path);
		if (time == null) {
			return -1L;
		}
		return time.longValue();
	}

	public String getServers() {
		return "mem";
	}
}
