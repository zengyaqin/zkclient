package com.kingdee.architecture.zkclient;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

public class GatewayThread extends Thread {
	protected static final Logger LOG = Logger.getLogger(GatewayThread.class);
	private final int _port;
	private final int _destinationPort;
	private ServerSocket _serverSocket;
	private Lock _lock = new ReentrantLock();
	private Condition _runningCondition = this._lock.newCondition();
	private boolean _running = false;

	public GatewayThread(int port, int destinationPort) {
		this._port = port;
		this._destinationPort = destinationPort;
		setDaemon(true);
	}

	public void run() {
		final List<Thread> runningThreads = new Vector();
		try {
			LOG.info("Starting gateway on port " + this._port + " pointing to port " + this._destinationPort);
			this._serverSocket = new ServerSocket(this._port);
			this._lock.lock();
			try {
				this._running = true;
				this._runningCondition.signalAll();
			} finally {
				this._lock.unlock();
			}
			for (;;) {
				final Socket socket = this._serverSocket.accept();
				LOG.info("new client is connected " + socket.getInetAddress());
				final InputStream incomingInputStream = socket.getInputStream();
				final OutputStream incomingOutputStream = socket.getOutputStream();
				final Socket outgoingSocket;
				try {
					outgoingSocket = new Socket("localhost", this._destinationPort);
				} catch (Exception e) {
					LOG.warn("could not connect to " + this._destinationPort);
					continue;
				}

				final InputStream outgoingInputStream = outgoingSocket.getInputStream();
				final OutputStream outgoingOutputStream = outgoingSocket.getOutputStream();

				Thread writeThread = new Thread() {
					public void run() {
						runningThreads.add(this);
						try {
							int read = -1;
							while ((read = incomingInputStream.read()) != -1) {
								outgoingOutputStream.write(read);
							}
						} catch (IOException e) {
						} finally {
							GatewayThread.this.closeQuietly(outgoingOutputStream);
							runningThreads.remove(this);
						}
					}

					public void interrupt() {
						try {
							socket.close();
							outgoingSocket.close();
						} catch (IOException e) {
							GatewayThread.LOG.error("error on stopping closing sockets", e);
						}
						super.interrupt();
					}
				};
				Thread readThread = new Thread() {
					public void run() {
						runningThreads.add(this);
						try {
							int read = -1;
							while ((read = outgoingInputStream.read()) != -1) {
								incomingOutputStream.write(read);
							}
						} catch (IOException e) {
						} finally {
							GatewayThread.this.closeQuietly(incomingOutputStream);
							runningThreads.remove(this);
						}
					}
				};
				writeThread.setDaemon(true);
				readThread.setDaemon(true);

				writeThread.start();
				readThread.start();
			}
		} catch (SocketException e) {
			if (!this._running) {
				throw ExceptionUtil.convertToRuntimeException(e);
			}
			LOG.info("Stopping gateway");
		} catch (Exception e) {
			LOG.error("error on gateway execution", e);
		}
		for (Thread thread : runningThreads) {
			thread.interrupt();
			try {
				thread.join();
			} catch (InterruptedException e) {
			}
		}
	}

	protected void closeQuietly(Closeable closable) {
		try {
			closable.close();
		} catch (IOException e) {
		}
	}

	public void interrupt() {
		try {
			this._serverSocket.close();
		} catch (Exception cE) {
			LOG.error("error on stopping gateway", cE);
		}
		super.interrupt();
	}

	public void interruptAndJoin() throws InterruptedException {
		interrupt();
		join();
	}

	public void awaitUp() {
		this._lock.lock();
		try {
			while (!this._running) {
				this._runningCondition.await();
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		} finally {
			this._lock.unlock();
		}
	}
}
