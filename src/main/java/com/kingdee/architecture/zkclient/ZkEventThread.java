package com.kingdee.architecture.zkclient;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.kingdee.architecture.zkclient.exception.ZkInterruptedException;

class ZkEventThread extends Thread {
	private static final Logger LOG = Logger.getLogger(ZkEventThread.class);
	private BlockingQueue<ZkEvent> _events = new LinkedBlockingQueue();
	private static AtomicInteger _eventId = new AtomicInteger(0);

	static abstract class ZkEvent {
		private String _description;

		public ZkEvent(String description) {
			this._description = description;
		}

		public abstract void run() throws Exception;

		public String toString() {
			return "ZkEvent[" + this._description + "]";
		}
	}

	ZkEventThread(String name) {
		setDaemon(true);
		setName("ZkClient-EventThread-" + getId() + "-" + name);
	}

	public void run() {
		LOG.info("Starting ZkClient event thread.");
		try {
			while (!isInterrupted()) {
				ZkEvent zkEvent = (ZkEvent) this._events.take();
				int eventId = _eventId.incrementAndGet();
				LOG.debug("Delivering event #" + eventId + " " + zkEvent);
				try {
					zkEvent.run();
				} catch (InterruptedException e) {
					interrupt();
				} catch (ZkInterruptedException e) {
					interrupt();
				} catch (Throwable e) {
					LOG.error("Error handling event " + zkEvent, e);
				}
				LOG.debug("Delivering event #" + eventId + " done");
			}
		} catch (InterruptedException e) {
			LOG.info("Terminate ZkClient event thread.");
		}
	}

	public void send(ZkEvent event) {
		if (!isInterrupted()) {
			LOG.debug("New event: " + event);
			this._events.add(event);
		}
	}
}
