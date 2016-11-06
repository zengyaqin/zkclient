package com.kingdee.dto;

/**
 * Created by zyq on 16/11/6.
 */
public class RemoteConfig extends BaseDto{

    private String remoteUrl;
    private int  port;

    public void setRemoteUrl(String remoteUrl) {
        this.remoteUrl = remoteUrl;
    }

    public String getRemoteUrl() {
        return remoteUrl;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}
