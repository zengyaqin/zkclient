package com.kingdee.architecture.zkclient;

import java.util.List;

public abstract interface IZkChildListener {
	public abstract void handleChildChange(String paramString, List<String> paramList) throws Exception;
}
