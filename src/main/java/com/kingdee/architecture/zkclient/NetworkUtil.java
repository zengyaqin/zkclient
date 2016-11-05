package com.kingdee.architecture.zkclient;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

public class NetworkUtil {
	public static final String OVERWRITE_HOSTNAME_SYSTEM_PROPERTY = "zkclient.hostname.overwritten";

	public static String[] getLocalHostNames() {
		Set<String> hostNames = new HashSet();

		hostNames.add("localhost");
		try {
			Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
			for (Enumeration ifaces = networkInterfaces; ifaces.hasMoreElements();) {
				NetworkInterface iface = (NetworkInterface) ifaces.nextElement();
				InetAddress ia = null;
				Enumeration ips = iface.getInetAddresses();
				while (ips.hasMoreElements()) {
					ia = (InetAddress) ips.nextElement();
					hostNames.add(ia.getCanonicalHostName());
					hostNames.add(ipToString(ia.getAddress()));
				}
			}
		} catch (SocketException e) {
			Enumeration<NetworkInterface> ifaces;
			InetAddress ia;
			Enumeration<InetAddress> ips;
			throw new RuntimeException("unable to retrieve host names of localhost");
		}
		return (String[]) hostNames.toArray(new String[hostNames.size()]);
	}

	private static String ipToString(byte[] bytes) {
		StringBuffer addrStr = new StringBuffer();
		for (int cnt = 0; cnt < bytes.length; cnt++) {
			int uByte = bytes[cnt] < 0 ? bytes[cnt] + 256 : bytes[cnt];
			addrStr.append(uByte);
			if (cnt < 3) {
				addrStr.append('.');
			}
		}
		return addrStr.toString();
	}

	public static int hostNamesInList(String serverList, String[] hostNames) {
		String[] serverNames = serverList.split(",");
		for (int i = 0; i < hostNames.length; i++) {
			String hostname = hostNames[i];
			for (int j = 0; j < serverNames.length; j++) {
				String serverNameAndPort = serverNames[j];
				String serverName = serverNameAndPort.split(":")[0];
				if (serverName.equalsIgnoreCase(hostname)) {
					return j;
				}
			}
		}
		return -1;
	}

	public static boolean hostNameInArray(String[] hostNames, String hostName) {
		for (String name : hostNames) {
			if (name.equalsIgnoreCase(hostName)) {
				return true;
			}
		}
		return false;
	}

	public static boolean isPortFree(int port) {
		try {
			Socket socket = new Socket("localhost", port);
			socket.close();
			return false;
		} catch (ConnectException e) {
			return true;
		} catch (SocketException e) {
			if (e.getMessage().equals("Connection reset by peer")) {
				return true;
			}
			throw new RuntimeException(e);
		} catch (UnknownHostException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static String getLocalhostName() {
		String property = System.getProperty("zkclient.hostname.overwritten");
		if ((property != null) && (property.trim().length() > 0)) {
			return property;
		}
		try {
			return InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			throw new RuntimeException("unable to retrieve localhost name");
		}
	}
}
