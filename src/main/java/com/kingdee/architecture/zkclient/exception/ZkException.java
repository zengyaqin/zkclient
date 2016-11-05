package com.kingdee.architecture.zkclient.exception;

import org.apache.zookeeper.KeeperException;

import com.kingdee.architecture.zkclient.exception.ZkBadVersionException;
import com.kingdee.architecture.zkclient.exception.ZkException;
import com.kingdee.architecture.zkclient.exception.ZkNoNodeException;
import com.kingdee.architecture.zkclient.exception.ZkNodeExistsException;

public class ZkException
  extends RuntimeException
{
  private static final long serialVersionUID = 1L;
  
  public ZkException() {}
  
  public ZkException(String message, Throwable cause)
  {
    super(message, cause);
  }
  
  public ZkException(String message)
  {
    super(message);
  }
  
  public ZkException(Throwable cause)
  {
    super(cause);
  }
  
  public static ZkException create(KeeperException e)
  {
    switch (e.code().ordinal())
    {
    case 1: 
      return new ZkNoNodeException(e);
    case 2: 
      return new ZkBadVersionException(e);
    case 3: 
      return new ZkNodeExistsException(e);
    }
    return new ZkException(e);
  }
}
