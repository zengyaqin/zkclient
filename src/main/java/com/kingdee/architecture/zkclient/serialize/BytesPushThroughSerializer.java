package com.kingdee.architecture.zkclient.serialize;

import com.kingdee.architecture.zkclient.exception.ZkMarshallingError;
import com.kingdee.architecture.zkclient.serialize.ZkSerializer;

public class BytesPushThroughSerializer
  implements ZkSerializer
{
  public Object deserialize(byte[] bytes)
    throws ZkMarshallingError
  {
    return bytes;
  }
  
  public byte[] serialize(Object bytes)
    throws ZkMarshallingError
  {
    return (byte[])bytes;
  }
}
