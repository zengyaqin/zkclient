package com.kingdee.architecture.zkclient;

public abstract interface IZkDataListener {
	public abstract void handleDataChange(String paramString, Object paramObject) throws Exception;

	public abstract void handleDataDeleted(String paramString) throws Exception;
}
