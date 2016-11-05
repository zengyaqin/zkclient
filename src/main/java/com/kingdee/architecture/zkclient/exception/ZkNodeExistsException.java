package com.kingdee.architecture.zkclient.exception;

import org.apache.zookeeper.KeeperException;

import com.kingdee.architecture.zkclient.exception.ZkException;

public class ZkNodeExistsException
  extends ZkException
{
  private static final long serialVersionUID = 1L;
  
  public ZkNodeExistsException() {}
  
  public ZkNodeExistsException(KeeperException cause)
  {
    super(cause);
  }
  
  public ZkNodeExistsException(String message, KeeperException cause)
  {
    super(message, cause);
  }
  
  public ZkNodeExistsException(String message)
  {
    super(message);
  }
}
