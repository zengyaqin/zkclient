package com.kingdee.architecture.zkclient.exception;

import org.apache.zookeeper.KeeperException;

import com.kingdee.architecture.zkclient.exception.ZkException;

public class ZkBadVersionException
  extends ZkException
{
  private static final long serialVersionUID = 1L;
  
  public ZkBadVersionException() {}
  
  public ZkBadVersionException(KeeperException cause)
  {
    super(cause);
  }
  
  public ZkBadVersionException(String message, KeeperException cause)
  {
    super(message, cause);
  }
  
  public ZkBadVersionException(String message)
  {
    super(message);
  }
}
