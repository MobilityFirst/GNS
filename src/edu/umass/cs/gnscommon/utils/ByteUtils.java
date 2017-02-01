
package edu.umass.cs.gnscommon.utils;


public class ByteUtils {

  //---------------------------------------------------------
  // short / byte[] utilities
  //---------------------------------------------------------

  public static void ShortToByteArray(int val, byte[] buf, int offset) {
    buf[0 + offset] = (byte) ((val >> 8) & 255);
    buf[1 + offset] = (byte) (val & 255);
  }


  public static void ShortToByteArray(int val, byte[] buf) {
    ShortToByteArray(val, buf, 0);
  }


  public static int BAToInt(byte[] buf, int offset) {
    int val = ((buf[offset] & 255) << 24) | ((buf[offset + 1] & 255) << 16) | ((buf[offset + 2] & 255) << 8) | (buf[offset + 3] & 255);
    return val;
  }


  public static int BAToInt(byte[] buf) {
    return BAToInt(buf, 0);
  }


  public static int ByteArrayToInt(byte[] buf, int offset) {
    int val = ((buf[offset] & 255) << 24) | ((buf[offset + 1] & 255) << 16) | ((buf[offset + 2] & 255) << 8) | (buf[offset + 3] & 255);
    return val;
  }


  public static int ByteArrayToInt(byte[] buf) {
    return ByteArrayToInt(buf, 0);
  }

  //---------------------------------------------------------
  // Integer / byte[] utilities
  //---------------------------------------------------------

  public static void IntToByteArray(int val, byte[] buf, int offset) {
    buf[0 + offset] = (byte) ((val >> 24) & 255);
    buf[1 + offset] = (byte) ((val >> 16) & 255);
    buf[2 + offset] = (byte) ((val >> 8) & 255);
    buf[3 + offset] = (byte) (val & 255);
  }


  public static void IntToByteArray(int val, byte[] buf) {
    IntToByteArray(val, buf, 0);
  }


  public static int ByteArrayToShort(byte[] buf, int offset) {
    int val = ((buf[offset] & 255) << 8) | (buf[offset + 1] & 255);
    return val;
  }


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

//
  public static byte[] hexStringToByteArray(String s) {
    int len = s.length();
    byte[] data = new byte[len / 2];
    for (int i = 0; i < len; i += 2) {
      data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i + 1), 16));
    }
    return data;
  }

//
  public static String toHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < bytes.length; i++) {
      sb.append(toHex(bytes[i]));
    }
    return sb.toString();
  }
//
//
  public static String toHex(byte b) {
    return String.format("%02X", b);
  }
  
  
  
//  public static void main(String[] args) {
//    String random = RandomString.randomString(24);
//    System.out.println(random + " " + new String(hexStringToByteArray(random)));
//  }
//  
}
