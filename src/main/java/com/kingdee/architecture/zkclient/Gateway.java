package com.kingdee.architecture.zkclient;

public class Gateway {
	private GatewayThread _thread;
	private final int _port;
	private final int _destinationPort;

	public Gateway(int port, int destinationPort) {
		this._port = port;
		this._destinationPort = destinationPort;
	}

	public synchronized void start() {
		if (this._thread != null) {
			throw new IllegalStateException("Gateway already running");
		}
		this._thread = new GatewayThread(this._port, this._destinationPort);
		this._thread.start();
		this._thread.awaitUp();
	}

	public synchronized void stop() {
		if (this._thread != null) {
			try {
				this._thread.interruptAndJoin();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			this._thread = null;
		}
	}
}
