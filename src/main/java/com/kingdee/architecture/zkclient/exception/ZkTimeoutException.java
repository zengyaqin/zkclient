package com.kingdee.architecture.zkclient.exception;

import com.kingdee.architecture.zkclient.exception.ZkException;

public class ZkTimeoutException
  extends ZkException
{
  private static final long serialVersionUID = 1L;
  
  public ZkTimeoutException() {}
  
  public ZkTimeoutException(String message, Throwable cause)
  {
    super(message, cause);
  }
  
  public ZkTimeoutException(String message)
  {
    super(message);
  }
  
  public ZkTimeoutException(Throwable cause)
  {
    super(cause);
  }
}
