package com.kingdee.architecture.zkclient;

import com.kingdee.architecture.zkclient.exception.ZkNoNodeException;
import org.apache.log4j.Logger;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public final class ContentWatcher<T> implements IZkDataListener {
	private static final Logger LOG = Logger.getLogger(ContentWatcher.class);
	private Lock _contentLock = new ReentrantLock(true);
	private Condition _contentAvailable = this._contentLock.newCondition();
	private Holder<T> _content;
	private String _fileName;
	private ZkClient _zkClient;

	public ContentWatcher(ZkClient zkClient, String fileName) {
		this._fileName = fileName;
		this._zkClient = zkClient;
	}

	public void start() {
		this._zkClient.subscribeDataChanges(this._fileName, this);
		readData();
		LOG.debug("Started ContentWatcher");
	}

	private void readData() {
		try {
			setContent(this._zkClient.readData(this._fileName));
		} catch (ZkNoNodeException e) {
		}
	}

	public void stop() {
		this._zkClient.unsubscribeDataChanges(this._fileName, this);
	}

	public void setContent(Object object) {
		LOG.debug("Received new data: " + object);
		this._contentLock.lock();
		try {
			this._content = new Holder(object);
			this._contentAvailable.signalAll();
		} finally {
			this._contentLock.unlock();
		}
	}

	public void handleDataChange(String dataPath, Object data) {
		setContent(data);
	}

	public void handleDataDeleted(String dataPath) {
	}

	public T getContent() throws InterruptedException {
		this._contentLock.lock();
		try {
			while (this._content == null) {
				this._contentAvailable.await();
			}
			return this._content.get();
		} finally {
			this._contentLock.unlock();
		}
	}
}
