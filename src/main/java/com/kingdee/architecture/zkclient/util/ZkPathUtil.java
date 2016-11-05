package com.kingdee.architecture.zkclient.util;

import java.util.List;

import com.kingdee.architecture.zkclient.ZkClient;

public class ZkPathUtil
{
  public static String leadingZeros(long number, int numberOfLeadingZeros)
  {
    return String.format("%0" + numberOfLeadingZeros + "d", new Object[] { Long.valueOf(number) });
  }
  
  public static String toString(ZkClient zkClient)
  {
    return toString(zkClient, "/", PathFilter.ALL);
  }
  
  public static String toString(ZkClient zkClient, String startPath, PathFilter pathFilter)
  {
    int level = 1;
    StringBuilder builder = new StringBuilder("+ (" + startPath + ")");
    builder.append("\n");
    addChildrenToStringBuilder(zkClient, pathFilter, 1, builder, startPath);
    return builder.toString();
  }
  
  private static void addChildrenToStringBuilder(ZkClient zkClient, PathFilter pathFilter, int level, StringBuilder builder, String startPath)
  {
    List<String> children = zkClient.getChildren(startPath);
    for (String node : children)
    {
      String nestedPath;
      if (startPath.endsWith("/")) {
        nestedPath = startPath + node;
      } else {
        nestedPath = startPath + "/" + node;
      }
      if (pathFilter.showChilds(nestedPath))
      {
        builder.append(getSpaces(level - 1) + "'-" + "+" + node + "\n");
        addChildrenToStringBuilder(zkClient, pathFilter, level + 1, builder, nestedPath);
      }
      else
      {
        builder.append(getSpaces(level - 1) + "'-" + "-" + node + " (contents hidden)\n");
      }
    }
  }
  
  private static String getSpaces(int level)
  {
    String s = "";
    for (int i = 0; i < level; i++) {
      s = s + "  ";
    }
    return s;
  }
  
  public static abstract interface PathFilter
  {
    public static final PathFilter ALL = new PathFilter()
    {
      public boolean showChilds(String path)
      {
        return true;
      }
    };
    
    public abstract boolean showChilds(String paramString);
  }
}
