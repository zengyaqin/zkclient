package com.kingdee.architecture.zkclient.exception;

import org.apache.zookeeper.KeeperException;

import com.kingdee.architecture.zkclient.exception.ZkException;

public class ZkNoNodeException
  extends ZkException
{
  private static final long serialVersionUID = 1L;
  
  public ZkNoNodeException() {}
  
  public ZkNoNodeException(KeeperException cause)
  {
    super(cause);
  }
  
  public ZkNoNodeException(String message, KeeperException cause)
  {
    super(message, cause);
  }
  
  public ZkNoNodeException(String message)
  {
    super(message);
  }
}
