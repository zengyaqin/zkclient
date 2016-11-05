package com.kingdee.architecture.zkclient;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.log4j.Logger;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ZooKeeperServer;

import com.kingdee.architecture.zkclient.exception.ZkException;
import com.kingdee.architecture.zkclient.exception.ZkInterruptedException;

public class ZkServer {
	private static final Logger LOG = Logger.getLogger(ZkServer.class);
	public static final int DEFAULT_PORT = 2181;
	public static final int DEFAULT_TICK_TIME = 5000;
	public static final int DEFAULT_MIN_SESSION_TIMEOUT = 10000;
	private String _dataDir;
	private String _logDir;
	private IDefaultNameSpace _defaultNameSpace;
	private ZooKeeperServer _zk;
	private NIOServerCnxn.Factory _nioFactory;
	private ZkClient _zkClient;
	private int _port;
	private int _tickTime;
	private int _minSessionTimeout;

	public ZkServer(String dataDir, String logDir, IDefaultNameSpace defaultNameSpace) {
		this(dataDir, logDir, defaultNameSpace, 2181);
	}

	public ZkServer(String dataDir, String logDir, IDefaultNameSpace defaultNameSpace, int port) {
		this(dataDir, logDir, defaultNameSpace, port, 5000);
	}

	public ZkServer(String dataDir, String logDir, IDefaultNameSpace defaultNameSpace, int port, int tickTime) {
		this(dataDir, logDir, defaultNameSpace, port, tickTime, 10000);
	}

	public ZkServer(String dataDir, String logDir, IDefaultNameSpace defaultNameSpace, int port, int tickTime,
			int minSessionTimeout) {
		this._dataDir = dataDir;
		this._logDir = logDir;
		this._defaultNameSpace = defaultNameSpace;
		this._port = port;
		this._tickTime = tickTime;
		this._minSessionTimeout = minSessionTimeout;
	}

	public int getPort() {
		return this._port;
	}

	@PostConstruct
	public void start() {
		String[] localHostNames = NetworkUtil.getLocalHostNames();
		String names = "";
		for (int i = 0; i < localHostNames.length; i++) {
			String name = localHostNames[i];
			names = names + " " + name;
			if (i + 1 != localHostNames.length) {
				names = names + ",";
			}
		}
		LOG.info("Starting ZkServer on: [" + names + "] port " + this._port + "...");
		startZooKeeperServer();
		this._zkClient = new ZkClient("localhost:" + this._port, 10000);
		this._defaultNameSpace.createDefaultNameSpace(this._zkClient);
	}

	private void startZooKeeperServer() {
		String[] localhostHostNames = NetworkUtil.getLocalHostNames();
		String servers = "localhost:" + this._port;

		int pos = -1;
		LOG.debug("check if hostNames " + servers + " is in list: " + Arrays.asList(localhostHostNames));
		if ((pos = NetworkUtil.hostNamesInList(servers, localhostHostNames)) != -1) {
			String[] hosts = servers.split(",");
			String[] hostSplitted = hosts[pos].split(":");
			int port = this._port;
			if (hostSplitted.length > 1) {
				port = Integer.parseInt(hostSplitted[1]);
			}
			if (NetworkUtil.isPortFree(port)) {
				File dataDir = new File(this._dataDir);
				File dataLogDir = new File(this._logDir);
				dataDir.mkdirs();
				dataLogDir.mkdirs();
				if (hosts.length > 1) {
					LOG.info("Start distributed zookeeper server...");
					throw new IllegalArgumentException("Unable to start distributed zookeeper server");
				}
				LOG.info("Start single zookeeper server...");
				LOG.info("data dir: " + dataDir.getAbsolutePath());
				LOG.info("data log dir: " + dataLogDir.getAbsolutePath());
				startSingleZkServer(this._tickTime, dataDir, dataLogDir, port);
			} else {
				throw new IllegalStateException(
						"Zookeeper port " + port + " was already in use. Running in single machine mode?");
			}
		}
	}

	private void startSingleZkServer(int tickTime, File dataDir, File dataLogDir, int port) {
		try {
			this._zk = new ZooKeeperServer(dataDir, dataLogDir, tickTime);
			this._zk.setMinSessionTimeout(this._minSessionTimeout);
			this._nioFactory = new NIOServerCnxn.Factory(new InetSocketAddress(port));
			this._nioFactory.startup(this._zk);
		} catch (IOException e) {
			throw new ZkException("Unable to start single ZooKeeper server.", e);
		} catch (InterruptedException e) {
			throw new ZkInterruptedException(e);
		}
	}

	@PreDestroy
	public void shutdown() {
		LOG.info("Shutting down ZkServer...");
		try {
			this._zkClient.close();
		} catch (ZkException e) {
			LOG.warn("Error on closing zkclient: " + e.getClass().getName());
		}
		if (this._nioFactory != null) {
			this._nioFactory.shutdown();
			try {
				this._nioFactory.join();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			this._nioFactory = null;
		}
		if (this._zk != null) {
			this._zk.shutdown();
			this._zk = null;
		}
		LOG.info("Shutting down ZkServer...done");
	}

	public ZkClient getZkClient() {
		return this._zkClient;
	}
}
