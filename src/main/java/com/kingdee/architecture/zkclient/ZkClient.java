package com.kingdee.architecture.zkclient;

import com.kingdee.architecture.zkclient.exception.*;
import com.kingdee.architecture.zkclient.serialize.SerializableSerializer;
import com.kingdee.architecture.zkclient.serialize.ZkSerializer;
import com.kingdee.architecture.zkclient.util.ZkPathUtil;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;

public class ZkClient implements Watcher {
	private static final Logger LOG = Logger.getLogger(ZkClient.class);
	protected IZkConnection _connection;
	private final Map<String, Set<IZkChildListener>> _childListener = new ConcurrentHashMap();
	private final ConcurrentHashMap<String, Set<IZkDataListener>> _dataListener = new ConcurrentHashMap();
	private final Set<IZkStateListener> _stateListener = new CopyOnWriteArraySet();
	private Watcher.Event.KeeperState _currentState = Watcher.Event.KeeperState.Unknown;
	private final ZkLock _zkEventLock = new ZkLock();
	private boolean _shutdownTriggered;
	private ZkEventThread _eventThread;
	private Thread _zookeeperEventThread;
	private ZkSerializer _zkSerializer;

	public ZkClient(String serverstring) {
		this(serverstring, 2147483647);
	}

	public ZkClient(String zkServers, int connectionTimeout) {
		this(new ZkConnection(zkServers), connectionTimeout);
	}

	public ZkClient(String zkServers, int sessionTimeout, int connectionTimeout) {
		this(new ZkConnection(zkServers, sessionTimeout), connectionTimeout);
	}

	public ZkClient(String zkServers, int sessionTimeout, int connectionTimeout, ZkSerializer zkSerializer) {
		this(new ZkConnection(zkServers, sessionTimeout), connectionTimeout, zkSerializer);
	}

	public ZkClient(IZkConnection connection) {
		this(connection, 2147483647);
	}

	public ZkClient(IZkConnection connection, int connectionTimeout) {
		this(connection, connectionTimeout, new SerializableSerializer());
	}

	public ZkClient(IZkConnection zkConnection, int connectionTimeout, ZkSerializer zkSerializer) {
		this._connection = zkConnection;
		this._zkSerializer = zkSerializer;
		connect(connectionTimeout, this);
	}

	public void setZkSerializer(ZkSerializer zkSerializer) {
		this._zkSerializer = zkSerializer;
	}

	public List<String> subscribeChildChanges(String path, IZkChildListener listener) {
		synchronized (this._childListener) {
			Set<IZkChildListener> listeners = (Set) this._childListener.get(path);
			if (listeners == null) {
				listeners = new CopyOnWriteArraySet();
				this._childListener.put(path, listeners);
			}
			listeners.add(listener);
		}
		return watchForChilds(path);
	}

	public void unsubscribeChildChanges(String path, IZkChildListener childListener) {
		synchronized (this._childListener) {
			Set<IZkChildListener> listeners = (Set) this._childListener.get(path);
			if (listeners != null) {
				listeners.remove(childListener);
			}
		}
	}

	public void subscribeDataChanges(String path, IZkDataListener listener) {
		synchronized (this._dataListener) {
			Set<IZkDataListener> listeners = (Set) this._dataListener.get(path);
			if (listeners == null) {
				listeners = new CopyOnWriteArraySet();
				this._dataListener.put(path, listeners);
			}
			listeners.add(listener);
		}
		watchForData(path);
		LOG.debug("Subscribed data changes for " + path);
	}

	public void unsubscribeDataChanges(String path, IZkDataListener dataListener) {
		synchronized (this._dataListener) {
			Set<IZkDataListener> listeners = (Set) this._dataListener.get(path);
			if (listeners != null) {
				listeners.remove(dataListener);
			}
			if ((listeners == null) || (listeners.isEmpty())) {
				this._dataListener.remove(path);
			}
		}
	}

	public void subscribeStateChanges(IZkStateListener listener) {
		synchronized (this._stateListener) {
			this._stateListener.add(listener);
		}
	}

	public void unsubscribeStateChanges(IZkStateListener stateListener) {
		synchronized (this._stateListener) {
			this._stateListener.remove(stateListener);
		}
	}

	public void unsubscribeAll() {
		synchronized (this._childListener) {
			this._childListener.clear();
		}
		synchronized (this._dataListener) {
			this._dataListener.clear();
		}
		synchronized (this._stateListener) {
			this._stateListener.clear();
		}
	}

	public void createPersistent(String path)
			throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
		createPersistent(path, false);
	}

	public void createPersistent(String path, boolean createParents)
			throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
		try {
			create(path, null, CreateMode.PERSISTENT);
		} catch (ZkNodeExistsException e) {
			if (!createParents) {
				throw e;
			}
		} catch (ZkNoNodeException e) {
			if (!createParents) {
				throw e;
			}
			String parentDir = path.substring(0, path.lastIndexOf('/'));
			createPersistent(parentDir, createParents);
			createPersistent(path, createParents);
		}
	}

	public void createPersistent(String path, Object data)
			throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
		create(path, data, CreateMode.PERSISTENT);
	}

	public String createPersistentSequential(String path, Object data)
			throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
		return create(path, data, CreateMode.PERSISTENT_SEQUENTIAL);
	}

	public void createEphemeral(String path)
			throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
		create(path, null, CreateMode.EPHEMERAL);
	}

	public String create(final String path, Object data, final CreateMode mode)
			throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
		if (path == null) {
			throw new NullPointerException("path must not be null.");
		}
		final byte[] bytes = data == null ? null : serialize(data);

		return (String) retryUntilConnected(new Callable() {
			public String call() throws Exception {
				return ZkClient.this._connection.create(path, bytes, mode);
			}
		});
	}

	public void createEphemeral(String path, Object data)
			throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
		create(path, data, CreateMode.EPHEMERAL);
	}

	public String createEphemeralSequential(String path, Object data)
			throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
		return create(path, data, CreateMode.EPHEMERAL_SEQUENTIAL);
	}

	public void process(WatchedEvent event) {
		LOG.debug("Received event: " + event);
		this._zookeeperEventThread = Thread.currentThread();

		boolean stateChanged = event.getPath() == null;
		boolean znodeChanged = event.getPath() != null;
		boolean dataChanged = (event.getType() == Watcher.Event.EventType.NodeDataChanged)
				|| (event.getType() == Watcher.Event.EventType.NodeDeleted)
				|| (event.getType() == Watcher.Event.EventType.NodeCreated)
				|| (event.getType() == Watcher.Event.EventType.NodeChildrenChanged);

		getEventLock().lock();
		try {
			if (getShutdownTrigger()) {
				LOG.debug("ignoring event '{" + event.getType() + " | " + event.getPath()
						+ "}' since shutdown triggered");
			} else {
				if (stateChanged) {
					processStateChanged(event);
				}
				if (dataChanged) {
					processDataOrChildChange(event);
				}
			}
		} finally {
			if (stateChanged) {
				getEventLock().getStateChangedCondition().signalAll();
				if (event.getState() == Watcher.Event.KeeperState.Expired) {
					getEventLock().getZNodeEventCondition().signalAll();
					getEventLock().getDataChangedCondition().signalAll();

					fireAllEvents();
				}
			}
			if (znodeChanged) {
				getEventLock().getZNodeEventCondition().signalAll();
			}
			if (dataChanged) {
				getEventLock().getDataChangedCondition().signalAll();
			}
			getEventLock().unlock();
			LOG.debug("Leaving process event");
		}
	}

	private void fireAllEvents() {
		for (Map.Entry<String, Set<IZkChildListener>> entry : this._childListener.entrySet()) {
			fireChildChangedEvents((String) entry.getKey(), (Set) entry.getValue());
		}
		for (Map.Entry<String, Set<IZkDataListener>> entry : this._dataListener.entrySet()) {
			fireDataChangedEvents((String) entry.getKey(), (Set) entry.getValue());
		}
	}

	public List<String> getChildren(String path) {
		return getChildren(path, hasListeners(path));
	}

	protected List<String> getChildren(final String path, final boolean watch) {
		return (List) retryUntilConnected(new Callable() {
			public List<String> call() throws Exception {
				return ZkClient.this._connection.getChildren(path, watch);
			}
		});
	}

	public int countChildren(String path) {
		try {
			return getChildren(path).size();
		} catch (ZkNoNodeException e) {
		}
		return 0;
	}

	protected boolean exists(final String path, final boolean watch) {
		return ((Boolean) retryUntilConnected(new Callable() {
			public Boolean call() throws Exception {
				return Boolean.valueOf(ZkClient.this._connection.exists(path, watch));
			}
		})).booleanValue();
	}

	public boolean exists(String path) {
		return exists(path, hasListeners(path));
	}

	private void processStateChanged(WatchedEvent event) {
		LOG.info("zookeeper state changed (" + event.getState() + ")");
		setCurrentState(event.getState());
		if (getShutdownTrigger()) {
			return;
		}
		try {
			fireStateChangedEvent(event.getState());
			if (event.getState() == Watcher.Event.KeeperState.Expired) {
				reconnect();
				fireNewSessionEvents();
			}
		} catch (Exception e) {
			throw new RuntimeException("Exception while restarting zk client", e);
		}
	}

	private void fireNewSessionEvents() {
		for (final IZkStateListener stateListener : this._stateListener) {
			this._eventThread.send(new ZkEventThread.ZkEvent("New session event sent to " + stateListener) {
				public void run() throws Exception {
					stateListener.handleNewSession();
				}
			});
		}
	}

	private void fireStateChangedEvent(final Watcher.Event.KeeperState state) {
		for (final IZkStateListener stateListener : this._stateListener) {
			this._eventThread
					.send(new ZkEventThread.ZkEvent("State changed to " + state + " sent to " + stateListener) {
						public void run() throws Exception {
							stateListener.handleStateChanged(state);
						}
					});
		}
	}

	private boolean hasListeners(String path) {
		Set<IZkDataListener> dataListeners = (Set) this._dataListener.get(path);
		if ((dataListeners != null) && (dataListeners.size() > 0)) {
			return true;
		}
		Set<IZkChildListener> childListeners = (Set) this._childListener.get(path);
		if ((childListeners != null) && (childListeners.size() > 0)) {
			return true;
		}
		return false;
	}

	public boolean deleteRecursive(String path) {
		List<String> children;
		try {
			children = getChildren(path, false);
		} catch (ZkNoNodeException e) {
			return true;
		}
		for (String subPath : children) {
			if (!deleteRecursive(path + "/" + subPath)) {
				return false;
			}
		}
		return delete(path);
	}

	private void processDataOrChildChange(WatchedEvent event) {
		String path = event.getPath();
		if ((event.getType() == Watcher.Event.EventType.NodeChildrenChanged)
				|| (event.getType() == Watcher.Event.EventType.NodeCreated)
				|| (event.getType() == Watcher.Event.EventType.NodeDeleted)) {
			Set<IZkChildListener> childListeners = (Set) this._childListener.get(path);
			if ((childListeners != null) && (!childListeners.isEmpty())) {
				fireChildChangedEvents(path, childListeners);
			}
		}
		if ((event.getType() == Watcher.Event.EventType.NodeDataChanged)
				|| (event.getType() == Watcher.Event.EventType.NodeDeleted)
				|| (event.getType() == Watcher.Event.EventType.NodeCreated)) {
			Set<IZkDataListener> listeners = (Set) this._dataListener.get(path);
			if ((listeners != null) && (!listeners.isEmpty())) {
				fireDataChangedEvents(event.getPath(), listeners);
			}
		}
	}

	private void fireDataChangedEvents(final String path, Set<IZkDataListener> listeners) {
		for (final IZkDataListener listener : listeners) {
			this._eventThread.send(new ZkEventThread.ZkEvent("Data of " + path + " changed sent to " + listener) {
				public void run() throws Exception {
					ZkClient.this.exists(path, true);
					try {
						Object data = ZkClient.this.readData(path, null, true);
						listener.handleDataChange(path, data);
					} catch (ZkMarshallingError error) {
						ZkClient.LOG.error("fire data changer events throws ZkMarshallingError", error);

						byte[] data = null;
						try {
							data = ZkClient.this.readRawData(path, true);
						} catch (Throwable t) {
							ZkClient.LOG.error("read raw data error within fireDataChangedEvents", t);
						}
						listener.handleDataChange(path, data);
					} catch (ZkNoNodeException e) {
						listener.handleDataDeleted(path);
						ZkClient.this.unsubscribeDataChanges(path, listener);
					}
				}
			});
		}
	}

	private void fireChildChangedEvents(final String path, Set<IZkChildListener> childListeners) {
		try {
			for (final IZkChildListener listener : childListeners) {
				this._eventThread
						.send(new ZkEventThread.ZkEvent("Children of " + path + " changed sent to " + listener) {
							public void run() throws Exception {
								try {
									ZkClient.this.exists(path);
									List<String> children = ZkClient.this.getChildren(path);
									listener.handleChildChange(path, children);
								} catch (ZkNoNodeException e) {
									listener.handleChildChange(path, null);
								}
							}
						});
			}
		} catch (Exception e) {
			LOG.error("Failed to fire child changed event. Unable to getChildren.  ", e);
		}
	}

	public boolean waitUntilExists(String path, TimeUnit timeUnit, long time) throws ZkInterruptedException {
		Date timeout = new Date(System.currentTimeMillis() + timeUnit.toMillis(time));
		LOG.debug("Waiting until znode '" + path + "' becomes available.");
		if (exists(path)) {
			return true;
		}
		acquireEventLock();
		try {
			boolean gotSignal;
			while (!exists(path, true)) {
				gotSignal = getEventLock().getZNodeEventCondition().awaitUntil(timeout);
				if (!gotSignal) {
					return false;
				}
			}
			return true;
		} catch (InterruptedException e) {
			throw new ZkInterruptedException(e);
		} finally {
			getEventLock().unlock();
		}
	}

	protected Set<IZkDataListener> getDataListener(String path) {
		return (Set) this._dataListener.get(path);
	}

	public void showFolders(OutputStream output) {
		try {
			output.write(ZkPathUtil.toString(this).getBytes());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void waitUntilConnected() throws ZkInterruptedException {
		waitUntilConnected(2147483647L, TimeUnit.MILLISECONDS);
	}

	public boolean waitUntilConnected(long time, TimeUnit timeUnit) throws ZkInterruptedException {
		return waitForKeeperState(Watcher.Event.KeeperState.SyncConnected, time, timeUnit);
	}

	public boolean waitForKeeperState(Watcher.Event.KeeperState keeperState, long time, TimeUnit timeUnit)
			throws ZkInterruptedException {
		if ((this._zookeeperEventThread != null) && (Thread.currentThread() == this._zookeeperEventThread)) {
			throw new IllegalArgumentException("Must not be done in the zookeeper event thread.");
		}
		Date timeout = new Date(System.currentTimeMillis() + timeUnit.toMillis(time));

		LOG.debug("Waiting for keeper state " + keeperState);
		acquireEventLock();
		try {
			boolean stillWaiting = true;
			boolean bool1;
			while (!this._currentState.equals(keeperState)) {
				if (!stillWaiting) {
					return false;
				}
				stillWaiting = getEventLock().getStateChangedCondition().awaitUntil(timeout);
			}
			LOG.debug("State is " + this._currentState);
			return true;
		} catch (InterruptedException e) {
			throw new ZkInterruptedException(e);
		} finally {
			getEventLock().unlock();
		}
	}

	private void acquireEventLock() {
		try {
			getEventLock().lockInterruptibly();
		} catch (InterruptedException e) {
			throw new ZkInterruptedException(e);
		}
	}

	public <T> T retryUntilConnected(Callable<T> callable)
			throws ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
		if ((this._zookeeperEventThread != null) && (Thread.currentThread() == this._zookeeperEventThread)) {
			throw new IllegalArgumentException("Must not be done in the zookeeper event thread.");
		}
		try {
			return callable.call();
		} catch (KeeperException.ConnectionLossException e) {
			for (;;) {
				Thread.yield();
				waitUntilConnected();
			}
		} catch (KeeperException.SessionExpiredException e) {
			for (;;) {
				Thread.yield();
				waitUntilConnected();
			}
		} catch (KeeperException e) {
			throw ZkException.create(e);
		} catch (InterruptedException e) {
			throw new ZkInterruptedException(e);
		} catch (Exception e) {
			throw ExceptionUtil.convertToRuntimeException(e);
		}
	}

	public void setCurrentState(Watcher.Event.KeeperState currentState) {
		getEventLock().lock();
		try {
			this._currentState = currentState;
		} finally {
			getEventLock().unlock();
		}
	}

	public ZkLock getEventLock() {
		return this._zkEventLock;
	}

	public boolean delete(final String path) {
		try {
			retryUntilConnected(new Callable() {
				public Object call() throws Exception {
					ZkClient.this._connection.delete(path);
					return null;
				}
			});
			return true;
		} catch (ZkNoNodeException e) {
		}
		return false;
	}

	private byte[] serialize(Object data) {
		return this._zkSerializer.serialize(data);
	}

	private Object derializable(byte[] data) {
		if (data == null) {
			return null;
		}
		return this._zkSerializer.deserialize(data);
	}

	public Object readData(String path) {
		return readData(path, false);
	}

	public Object readData(String path, boolean returnNullIfPathNotExists) {
		Object data = null;
		try {
			data = readData(path, null);
		} catch (ZkNoNodeException e) {
			if (!returnNullIfPathNotExists) {
				throw e;
			}
		}
		return data;
	}

	public Object readData(String path, Stat stat) {
		return readData(path, stat, hasListeners(path));
	}

	protected Object readData(final String path, final Stat stat, final boolean watch) {
		byte[] data = (byte[]) retryUntilConnected(new Callable() {
			public byte[] call() throws Exception {
				return ZkClient.this._connection.readData(path, stat, watch);
			}
		});
		return derializable(data);
	}

	public byte[] readRawData(final String path, boolean returnNullIfPathNotExists) {
		byte[] data = (byte[]) retryUntilConnected(new Callable() {
			public byte[] call() throws Exception {
				return ZkClient.this._connection.readData(path, null, ZkClient.this.hasListeners(path));
			}
		});
		return data;
	}

	public Stat writeData(String path, Object object) {
		return writeData(path, object, -1);
	}

	public void updateDataSerialized(String path, DataUpdater updater) {
		Stat stat = new Stat();
		boolean retry;
		do {
			retry = false;
			try {
				Object oldData = readData(path, stat);
				Object newData = updater.update(oldData);
				writeData(path, newData, stat.getVersion());
			} catch (ZkBadVersionException e) {
				retry = true;
			}
		} while (retry);
	}

	public Stat writeData(final String path, Object datat, final int expectedVersion) {
		final byte[] data = serialize(datat);
		return (Stat) retryUntilConnected(new Callable() {
			public Object call() throws Exception {
				Stat stat = ZkClient.this._connection.writeData(path, data, expectedVersion);
				return stat;
			}
		});
	}

	public Stat writeRawData(String path, String object) {
		return writeRawData(path, object, -1);
	}

	public Stat writeRawData(final String path, String str, final int expectedVersion) {
		final byte[] data = str.getBytes();
		return (Stat) retryUntilConnected(new Callable() {
			public Object call() throws Exception {
				Stat stat = ZkClient.this._connection.writeData(path, data, expectedVersion);
				return stat;
			}
		});
	}

	public void watchForData(final String path) {
		retryUntilConnected(new Callable() {
			public Object call() throws Exception {
				ZkClient.this._connection.exists(path, true);
				return null;
			}
		});
	}

	public List<String> watchForChilds(final String path) {
		if ((this._zookeeperEventThread != null) && (Thread.currentThread() == this._zookeeperEventThread)) {
			throw new IllegalArgumentException("Must not be done in the zookeeper event thread.");
		}
		return (List) retryUntilConnected(new Callable() {
			public List<String> call() throws Exception {
				ZkClient.this.exists(path, true);
				try {
					return ZkClient.this.getChildren(path, true);
				} catch (ZkNoNodeException e) {
				}
				return null;
			}
		});
	}

	public void connect(long maxMsToWaitUntilConnected, Watcher watcher)
			throws ZkInterruptedException, ZkTimeoutException, IllegalStateException {
		boolean started = false;
		try {
			getEventLock().lockInterruptibly();
			setShutdownTrigger(false);
			this._eventThread = new ZkEventThread(this._connection.getServers());
			this._eventThread.start();
			this._connection.connect(watcher);

			LOG.debug("Awaiting connection to Zookeeper server");
			if (!waitUntilConnected(maxMsToWaitUntilConnected, TimeUnit.MILLISECONDS)) {
				throw new ZkTimeoutException(
						"Unable to connect to zookeeper server within timeout: " + maxMsToWaitUntilConnected);
			}
			started = true;
		} catch (InterruptedException e) {
			ZooKeeper.States state = this._connection.getZookeeperState();
			throw new IllegalStateException("Not connected with zookeeper server yet. Current state is " + state);
		} finally {
			getEventLock().unlock();
			if (!started) {
				close();
			}
		}
	}

	public long getCreationTime(String path) {
		try {
			getEventLock().lockInterruptibly();
			return this._connection.getCreateTime(path);
		} catch (KeeperException e) {
			throw ZkException.create(e);
		} catch (InterruptedException e) {
			throw new ZkInterruptedException(e);
		} finally {
			getEventLock().unlock();
		}
	}

	public void close() throws ZkInterruptedException {
		if (this._connection == null) {
			return;
		}
		LOG.debug("Closing ZkClient...");
		getEventLock().lock();
		try {
			setShutdownTrigger(true);
			this._eventThread.interrupt();
			this._eventThread.join(2000L);
			this._connection.close();
			this._connection = null;
		} catch (InterruptedException e) {
			throw new ZkInterruptedException(e);
		} finally {
			getEventLock().unlock();
		}
		LOG.debug("Closing ZkClient...done");
	}

	private void reconnect() {
		getEventLock().lock();
		try {
			this._connection.close();
			this._connection.connect(this);
		} catch (InterruptedException e) {
			throw new ZkInterruptedException(e);
		} finally {
			getEventLock().unlock();
		}
	}

	public void setShutdownTrigger(boolean triggerState) {
		this._shutdownTriggered = triggerState;
	}

	public boolean getShutdownTrigger() {
		return this._shutdownTriggered;
	}

	public int numberOfListeners() {
		int listeners = 0;
		for (Set<IZkChildListener> childListeners : this._childListener.values()) {
			listeners += childListeners.size();
		}
		for (Set<IZkDataListener> dataListeners : this._dataListener.values()) {
			listeners += dataListeners.size();
		}
		listeners += this._stateListener.size();

		return listeners;
	}
}
