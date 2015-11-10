/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Westy, Emmanuel Cecchet
 *
 */
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
