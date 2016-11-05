package com.kingdee.architecture.zkclient.exception;

import com.kingdee.architecture.zkclient.exception.ZkException;

public class ZkInterruptedException
  extends ZkException
{
  private static final long serialVersionUID = 1L;
  
  public ZkInterruptedException(InterruptedException e)
  {
    super(e);
    Thread.currentThread().interrupt();
  }
}
