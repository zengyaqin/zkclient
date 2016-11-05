package com.kingdee.architecture.zkclient.serialize;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import com.kingdee.architecture.zkclient.exception.ZkMarshallingError;
import com.kingdee.architecture.zkclient.serialize.ZkSerializer;

public class SerializableSerializer
  implements ZkSerializer
{
  public Object deserialize(byte[] bytes)
    throws ZkMarshallingError
  {
    try
    {
      ObjectInputStream inputStream = new ObjectInputStream(new ByteArrayInputStream(bytes));
      return inputStream.readObject();
    }
    catch (ClassNotFoundException e)
    {
      throw new ZkMarshallingError("Unable to find object class.", e);
    }
    catch (IOException e)
    {
      throw new ZkMarshallingError(e);
    }
  }
  
  public byte[] serialize(Object serializable)
    throws ZkMarshallingError
  {
    try
    {
      ByteArrayOutputStream byteArrayOS = new ByteArrayOutputStream();
      ObjectOutputStream stream = new ObjectOutputStream(byteArrayOS);
      stream.writeObject(serializable);
      stream.close();
      return byteArrayOS.toByteArray();
    }
    catch (IOException e)
    {
      throw new ZkMarshallingError(e);
    }
  }
}
