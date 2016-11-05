package com.kingdee.architecture.zkclient.exception;

import com.kingdee.architecture.zkclient.exception.ZkException;

public class ZkMarshallingError
  extends ZkException
{
  private static final long serialVersionUID = 1L;
  
  public ZkMarshallingError() {}
  
  public ZkMarshallingError(Throwable cause)
  {
    super(cause);
  }
  
  public ZkMarshallingError(String message, Throwable cause)
  {
    super(message, cause);
  }
  
  public ZkMarshallingError(String message)
  {
    super(message);
  }
}
