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
package edu.umass.cs.gnscommon.utils;

/**
 *
 * @author westy
 */
public class ByteUtils {

  //---------------------------------------------------------
  // short / byte[] utilities
  //---------------------------------------------------------
  /**
   * Write low-order 16-bits of val into buf[offset] buf[offset+1].
   * 
   * @param val
   * @param buf
   * @param offset
   */
  public static void ShortToByteArray(int val, byte[] buf, int offset) {
    buf[0 + offset] = (byte) ((val >> 8) & 255);
    buf[1 + offset] = (byte) (val & 255);
  }

  /**
   * Write low order 16-bits of val into buf[0] buf[1].
   * 
   * @param val
   * @param buf
   */
  public static void ShortToByteArray(int val, byte[] buf) {
    ShortToByteArray(val, buf, 0);
  }

  /**
   * Convert byte[offset]...byte[offset+3] to integer.
   *
   * @param buf Byte array
   * @param offset Offset into the byte array
   * @return the integer
   */
  public static int BAToInt(byte[] buf, int offset) {
    int val = ((buf[offset] & 255) << 24) | ((buf[offset + 1] & 255) << 16) | ((buf[offset + 2] & 255) << 8) | (buf[offset + 3] & 255);
    return val;
  }

  /**
   * Convert byte[0]...buf[3] to integer.
   * @param buf
   * @return the integer
   */
  public static int BAToInt(byte[] buf) {
    return BAToInt(buf, 0);
  }

  /**
   * Convert byte[offset]...byte[offset+3] to integer.
   *
   * @param buf Byte array
   * @param offset Offset into the byte array
   * @return the integer
   */
  public static int ByteArrayToInt(byte[] buf, int offset) {
    int val = ((buf[offset] & 255) << 24) | ((buf[offset + 1] & 255) << 16) | ((buf[offset + 2] & 255) << 8) | (buf[offset + 3] & 255);
    return val;
  }

  /**
   * Convert byte[0]...buf[3] to integer.
   * 
   * @param buf
   * @return the integer
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
   * @param buf
   * @param offset
   * @return the integer
   */
  public static int ByteArrayToShort(byte[] buf, int offset) {
    int val = ((buf[offset] & 255) << 8) | (buf[offset + 1] & 255);
    return val;
  }

  /**
   * Convert buf[0] buf[1] to int in range 0..65535 
   * @param buf
   * @return the integer
   */
  public static int ByteArrayToShort(byte[] buf) {
    return ByteArrayToShort(buf, 0);
  }

  /**
   * Convert a byte array to a long value.
   * 
   * @param bytes
   * @return the long
   */
  public static long byteArrayToLong(byte[] bytes) {
    long value = 0;
    for (int i = 0; i < bytes.length; i++) {
      value = (value << 8) + (bytes[i] & 255);
    }
    return value;
  }

  /**
   * Converts a hexidecimal string to a byte array.
   * 
   * @param s
   * @return a byte array
   */
  public static byte[] hexStringToByteArray(String s) {
    int len = s.length();
    byte[] data = new byte[len / 2];
    for (int i = 0; i < len; i += 2) {
      data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i + 1), 16));
    }
    return data;
  }

  /**
   * Converts a byte array to a hex string.
   * 
   * @param bytes
   * @return the string
   */
  public static String toHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < bytes.length; i++) {
      sb.append(ByteUtils.toHex(bytes[i]));
    }
    return sb.toString();
  }

  /**
   * Converts a byte to a two digit hex string.
   * 
   * @param b
   * @return the string
   */
  public static String toHex(byte b) {
    return String.format("%02X", b);
  }
  
}
