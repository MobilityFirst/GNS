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
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.acs.util;

/**
 *
 * @author westy
 */
public class ByteUtils {

  public static String convertToHex(byte[] data) {
    StringBuilder buf = new StringBuilder();
    for (int i = 0; i < data.length; i++) {
      int halfbyte = (data[i] >>> 4) & 15;
      int two_halfs = 0;
      do {
        if ((0 <= halfbyte) && (halfbyte <= 9)) {
          buf.append((char) ('0' + halfbyte));
        } else {
          buf.append((char) ('a' + (halfbyte - 10)));
        }
        halfbyte = data[i] & 15;
      } while (two_halfs++ < 1);
    }
    return buf.toString();
  }

  //---------------------------------------------------------
  // short / byte[] utilities
  //---------------------------------------------------------
  /**

   * Write low-order 16-bits of val into buf[offset] buf[offset+1]
   */
  public static void ShortToByteArray(int val, byte[] buf, int offset) {
    buf[0 + offset] = (byte) ((val >> 8) & 255);
    buf[1 + offset] = (byte) (val & 255);
  }

  /**
   * Write low order 16-bits of val into buf[0] buf[1]
   */
  public static void ShortToByteArray(int val, byte[] buf) {
    ShortToByteArray(val, buf, 0);
  }

  /**
   * Convert byte[offset]...byte[offset+3] to integer.
   *
   * @param buf Byte array
   * @param offset Offset into the byte array
   */
  public static int BAToInt(byte[] buf, int offset) {
    int val = ((buf[offset] & 255) << 24) | ((buf[offset + 1] & 255) << 16) | ((buf[offset + 2] & 255) << 8) | (buf[offset + 3] & 255);
    return val;
  }

  /**
   * Convert byte[0]...buf[3] to integer.
   */
  public static int BAToInt(byte[] buf) {
    return BAToInt(buf, 0);
  }

  /**

   * Convert byte[offset]...byte[offset+3] to integer.
   *
   * @param buf Byte array
   * @param offset Offset into the byte array
   */
  public static int ByteArrayToInt(byte[] buf, int offset) {
    int val = ((buf[offset] & 255) << 24) | ((buf[offset + 1] & 255) << 16) | ((buf[offset + 2] & 255) << 8) | (buf[offset + 3] & 255);
    return val;
  }

  /**
   * Convert byte[0]...buf[3] to integer.
   */
  public static int ByteArrayToInt(byte[] buf) {
    return ByteArrayToInt(buf, 0);
  }

  //---------------------------------------------------------
  // Integer / byte[] utilities
  //---------------------------------------------------------
  /**
   * Write integer into buf[offset]...buf[offset+3]
   *
   * @param val Integer value
   * @param buf Byte array
   * @param offset Offset into the byte array
   */
  public static void IntToByteArray(int val, byte[] buf, int offset) {
    buf[0 + offset] = (byte) ((val >> 24) & 255);
    buf[1 + offset] = (byte) ((val >> 16) & 255);
    buf[2 + offset] = (byte) ((val >> 8) & 255);
    buf[3 + offset] = (byte) (val & 255);
  }

  /**
   * Write integer into buf[0]...buf[3]
   *
   * @param val Integer value
   * @param buf Byte array
   */
  public static void IntToByteArray(int val, byte[] buf) {
    IntToByteArray(val, buf, 0);
  }

  /**
   * Convert buf[offset] buf[offset+1] to int in range 0..65535
   */
  public static int ByteArrayToShort(byte[] buf, int offset) {
    int val = ((buf[offset] & 255) << 8) | (buf[offset + 1] & 255);
    return val;
  }

  /**
   * Convert buf[0] buf[1] to int in range 0..65535 
   */
  public static int ByteArrayToShort(byte[] buf) {
    return ByteArrayToShort(buf, 0);
  }

  public static long byteArrayToLong(byte[] bytes) {
    long value = 0;
    for (int i = 0; i < bytes.length; i++) {
      value = (value << 8) + (bytes[i] & 255);
    }
    return value;
  }

  public static byte[] hexStringToByteArray(String s) {
    int len = s.length();
    byte[] data = new byte[len / 2];
    for (int i = 0; i < len; i += 2) {
      data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i + 1), 16));
    }
    return data;
  }

  public static String toHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < bytes.length; i++) {
      sb.append(ByteUtils.toHex(bytes[i]));
    }
    return sb.toString();
  }

  public static String toHex(byte b) {
    return String.format("%02X", b);
  }
  
}
