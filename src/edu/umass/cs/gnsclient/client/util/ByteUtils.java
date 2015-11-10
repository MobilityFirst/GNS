
package edu.umass.cs.gnsclient.client.util;

/**
 * @author westy
 */
public class ByteUtils
{

  public static String toHex(byte b)
  {
    return String.format("%02X", b);
  }

  public static String toHex(byte[] bytes)
  {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < bytes.length; i++)
    {
      sb.append(ByteUtils.toHex(bytes[i]));
    }
    return sb.toString();
  }

  public static byte[] hexStringToByteArray(String s)
  {
    int len = s.length();
    byte[] data = new byte[len / 2];
    for (int i = 0; i < len; i += 2)
    {
      data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character
          .digit(s.charAt(i + 1), 16));
    }
    return data;
  }

  public static long byteArrayToLong(byte[] bytes)
  {
    long value = 0;
    for (int i = 0; i < bytes.length; i++)
    {
      value = (value << 8) + (bytes[i] & 0xff);
    }
    return value;
  }

}
