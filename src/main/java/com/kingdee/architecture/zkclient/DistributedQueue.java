package com.kingdee.architecture.zkclient;

import java.io.Serializable;
import java.util.List;

import com.kingdee.architecture.zkclient.exception.ZkNoNodeException;

public class DistributedQueue<T extends Serializable> {
	private ZkClient _zkClient;
	private String _root;
	private static final String ELEMENT_NAME = "element";

	private static class Element<T> {
		private String _name;
		private T _data;

		public Element(String name, T data) {
			this._name = name;
			this._data = data;
		}

		public String getName() {
			return this._name;
		}

		public T getData() {
			return this._data;
		}
	}

	public DistributedQueue(ZkClient zkClient, String root) {
		this._zkClient = zkClient;
		this._root = root;
	}

	public boolean offer(T element) {
		try {
			this._zkClient.createPersistentSequential(this._root + "/" + "element" + "-", element);
		} catch (Exception e) {
			throw ExceptionUtil.convertToRuntimeException(e);
		}
		return true;
	}

	public Serializable poll() {
		for (;;) {
			Element<T> element = getFirstElement();
			if (element == null) {
				return null;
			}
			try {
				this._zkClient.delete(element.getName());
				return (Serializable) element.getData();
			} catch (ZkNoNodeException e) {
			} catch (Exception e) {
				throw ExceptionUtil.convertToRuntimeException(e);
			}
		}
	}

	private String getSmallestElement(List<String> list) {
		String smallestElement = (String) list.get(0);
		for (String element : list) {
			if (element.compareTo(smallestElement) < 0) {
				smallestElement = element;
			}
		}
		return smallestElement;
	}

	public boolean isEmpty() {
		return this._zkClient.getChildren(this._root).size() == 0;
	}

	private Element getFirstElement() {
		List list;
		String elementName;
		try {
			list = _zkClient.getChildren(_root);
			if (list.size() == 0)
				return null;
		} catch (Exception e) {
			throw ExceptionUtil.convertToRuntimeException(e);
		}
		elementName = getSmallestElement(list);
		return new Element((new StringBuilder()).append(_root).append("/").append(elementName).toString(),
				(Serializable) _zkClient
						.readData((new StringBuilder()).append(_root).append("/").append(elementName).toString()));
	}

	public Serializable peek() {
		Element<T> element = getFirstElement();
		if (element == null) {
			return null;
		}
		return (Serializable) element.getData();
	}
}
