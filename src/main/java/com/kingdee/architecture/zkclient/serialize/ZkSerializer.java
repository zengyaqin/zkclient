package com.kingdee.architecture.zkclient.serialize;

import com.kingdee.architecture.zkclient.exception.ZkMarshallingError;

public abstract interface ZkSerializer
{
  public abstract byte[] serialize(Object paramObject)
    throws ZkMarshallingError;
  
  public abstract Object deserialize(byte[] paramArrayOfByte)
    throws ZkMarshallingError;
}
