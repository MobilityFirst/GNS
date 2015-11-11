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

import java.util.Arrays;

/**
 *
 * @author westy 
 */
public class Base64 {

  private static final char[] CharacterArry = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".toCharArray();
  private static final int[] IntegerArray = new int[256];

  static {
    Arrays.fill(IntegerArray, -1);
    for (int i = 0, iS = CharacterArry.length; i < iS; i++) {
      IntegerArray[CharacterArry[i]] = i;
    }
    IntegerArray['='] = 0;
  }

  // ****************************************************************************************
  // *  char[] version
  // ****************************************************************************************
  /**
   * Encodes a raw byte array into a BASE64 <code>char[]</code> representation in accordance with RFC 2045.
   *
   * @param sourceArray The bytes to convert. If <code>null</code> or length 0 an empty array will be returned.
   * @param lineSep Optional "\r\n" after 76 characters, unless end of file.<br>
   * No line separator will be in breach of RFC 2045 which specifies max 76 per line but will be a
   * little faster.
   * @return A BASE64 encoded array. Never <code>null</code>.
   */
  public final static char[] encodeToChar(byte[] sourceArray, boolean lineSep) {
    // Check special case
    int sourceLength = sourceArray != null ? sourceArray.length : 0;
    if (sourceLength == 0) {
      return new char[0];
    }

    int eLen = (sourceLength / 3) * 3;              // Length of even 24-bits.
    int cCnt = ((sourceLength - 1) / 3 + 1) << 2;   // Returned character count
    int destLen = cCnt + (lineSep ? (cCnt - 1) / 76 << 1 : 0); // Length of returned array
    char[] destArray = new char[destLen];

    // Encode even 24-bits
    for (int s = 0, d = 0, cc = 0; s < eLen;) {
      // Copy next three bytes into lower 24 bits of int, paying attension to sign.
      int i = (sourceArray[s++] & 0xff) << 16 | (sourceArray[s++] & 0xff) << 8 | (sourceArray[s++] & 0xff);

      // Encode the int into four chars
      destArray[d++] = CharacterArry[(i >>> 18) & 0x3f];
      destArray[d++] = CharacterArry[(i >>> 12) & 0x3f];
      destArray[d++] = CharacterArry[(i >>> 6) & 0x3f];
      destArray[d++] = CharacterArry[i & 0x3f];

      // Add optional line separator
      if (lineSep && ++cc == 19 && d < destLen - 2) {
        destArray[d++] = '\r';
        destArray[d++] = '\n';
        cc = 0;
      }
    }

    // Pad and encode last bits if source isn't even 24 bits.
    int left = sourceLength - eLen; // 0 - 2.
    if (left > 0) {
      // Prepare the int
      int i = ((sourceArray[eLen] & 0xff) << 10) | (left == 2 ? ((sourceArray[sourceLength - 1] & 0xff) << 2) : 0);

      // Set last four chars
      destArray[destLen - 4] = CharacterArry[i >> 12];
      destArray[destLen - 3] = CharacterArry[(i >>> 6) & 0x3f];
      destArray[destLen - 2] = left == 2 ? CharacterArry[i & 0x3f] : '=';
      destArray[destLen - 1] = '=';
    }
    return destArray;
  }

  /**
   * Decodes a BASE64 encoded char array. All illegal characters will be ignored and can handle both arrays with
   * and without line separators.
   *
   * @param sourceArray The source array. <code>null</code> or length 0 will return an empty array.
   * @return The decoded array of bytes. May be of length 0. Will be <code>null</code> if the legal characters
   * (including '=') isn't divideable by 4. (I.e. definitely corrupted).
   */
  public final static byte[] decode(char[] sourceArray) {
    // Check special case
    int sourceLength = sourceArray != null ? sourceArray.length : 0;
    if (sourceLength == 0) {
      return new byte[0];
    }

    // Count illegal characters (including '\r', '\n') to know what size the returned array will be,
    // so we don't have to reallocate & copy it later.
    int sepCnt = 0; // Number of separator characters. (Actually illegal characters, but that's a bonus...)
    for (int i = 0; i < sourceLength; i++) // If input is "pure" (I.e. no line separators or illegal chars) base64 this loop can be commented out.
    {
      if (IntegerArray[sourceArray[i]] < 0) {
        sepCnt++;
      }
    }

    // Check so that legal chars (including '=') are evenly divideable by 4 as specified in RFC 2045.
    if ((sourceLength - sepCnt) % 4 != 0) {
      return null;
    }

    int pad = 0;
    for (int i = sourceLength; i > 1 && IntegerArray[sourceArray[--i]] <= 0;) {
      if (sourceArray[i] == '=') {
        pad++;
      }
    }

    int len = ((sourceLength - sepCnt) * 6 >> 3) - pad;

    byte[] destArray = new byte[len];       // Preallocate byte[] of exact length

    for (int s = 0, d = 0; d < len;) {
      // Assemble three bytes into an int from four "valid" characters.
      int i = 0;
      for (int j = 0; j < 4; j++) {   // j only increased if a valid char was found.
        int c = IntegerArray[sourceArray[s++]];
        if (c >= 0) {
          i |= c << (18 - j * 6);
        } else {
          j--;
        }
      }
      // Add the bytes
      destArray[d++] = (byte) (i >> 16);
      if (d < len) {
        destArray[d++] = (byte) (i >> 8);
        if (d < len) {
          destArray[d++] = (byte) i;
        }
      }
    }
    return destArray;
  }

  /**
   * Encodes a raw byte array into a BASE64 <code>byte[]</code> representation i accordance with RFC 2045.
   *
   * @param sourceArray The bytes to convert. If <code>null</code> or length 0 an empty array will be returned.
   * @param lineSep Optional "\r\n" after 76 characters, unless end of file.<br>
   * No line separator will be in breach of RFC 2045 which specifies max 76 per line but will be a
   * little faster.
   * @return A BASE64 encoded array. Never <code>null</code>.
   */
  public final static byte[] encodeToByte(byte[] sourceArray, boolean lineSep) {
    // Check special case
    int sourceLength = sourceArray != null ? sourceArray.length : 0;
    if (sourceLength == 0) {
      return new byte[0];
    }

    int eLen = (sourceLength / 3) * 3;                              // Length of even 24-bits.
    int cCnt = ((sourceLength - 1) / 3 + 1) << 2;                   // Returned character count
    int destLen = cCnt + (lineSep ? (cCnt - 1) / 76 << 1 : 0); // Length of returned array
    byte[] destArray = new byte[destLen];

    // Encode even 24-bits
    for (int s = 0, d = 0, cc = 0; s < eLen;) {
      // Copy next three bytes into lower 24 bits of int, paying attension to sign.
      int i = (sourceArray[s++] & 0xff) << 16 | (sourceArray[s++] & 0xff) << 8 | (sourceArray[s++] & 0xff);

      // Encode the int into four chars
      destArray[d++] = (byte) CharacterArry[(i >>> 18) & 0x3f];
      destArray[d++] = (byte) CharacterArry[(i >>> 12) & 0x3f];
      destArray[d++] = (byte) CharacterArry[(i >>> 6) & 0x3f];
      destArray[d++] = (byte) CharacterArry[i & 0x3f];

      // Add optional line separator
      if (lineSep && ++cc == 19 && d < destLen - 2) {
        destArray[d++] = '\r';
        destArray[d++] = '\n';
        cc = 0;
      }
    }

    // Pad and encode last bits if source isn't an even 24 bits.
    int left = sourceLength - eLen; // 0 - 2.
    if (left > 0) {
      // Prepare the int
      int i = ((sourceArray[eLen] & 0xff) << 10) | (left == 2 ? ((sourceArray[sourceLength - 1] & 0xff) << 2) : 0);

      // Set last four chars
      destArray[destLen - 4] = (byte) CharacterArry[i >> 12];
      destArray[destLen - 3] = (byte) CharacterArry[(i >>> 6) & 0x3f];
      destArray[destLen - 2] = left == 2 ? (byte) CharacterArry[i & 0x3f] : (byte) '=';
      destArray[destLen - 1] = '=';
    }
    return destArray;
  }

  /**
   * Decodes a BASE64 encoded byte array. All illegal characters will be ignored and can handle both arrays with
   * and without line separators.
   *
   * @param sourceArray The source array. Length 0 will return an empty array. <code>null</code> will throw an exception.
   * @return The decoded array of bytes. May be of length 0. Will be <code>null</code> if the legal characters
   * (including '=') isn't dividable by 4. (I.e. definitely corrupted).
   */
  public final static byte[] decode(byte[] sourceArray) {
    // Check special case
    int sourceLength = sourceArray.length;

		// Count illegal characters (including '\r', '\n') to know what size the returned array will be,
    // so we don't have to reallocate & copy it later.
    int sepCnt = 0; // Number of separator characters. (Actually illegal characters, but that's a bonus...)
    for (int i = 0; i < sourceLength; i++) // If input is "pure" (I.e. no line separators or illegal chars) base64 this loop can be commented out.
    {
      if (IntegerArray[sourceArray[i] & 0xff] < 0) {
        sepCnt++;
      }
    }

    // Check so that legal chars (including '=') are evenly divideable by 4 as specified in RFC 2045.
    if ((sourceLength - sepCnt) % 4 != 0) {
      return null;
    }

    int pad = 0;
    for (int i = sourceLength; i > 1 && IntegerArray[sourceArray[--i] & 0xff] <= 0;) {
      if (sourceArray[i] == '=') {
        pad++;
      }
    }

    int len = ((sourceLength - sepCnt) * 6 >> 3) - pad;

    byte[] destArray = new byte[len];       // Preallocate byte[] of exact length

    for (int s = 0, d = 0; d < len;) {
      // Assemble three bytes into an int from four "valid" characters.
      int i = 0;
      for (int j = 0; j < 4; j++) {   // j only increased if a valid char was found.
        int c = IntegerArray[sourceArray[s++] & 0xff];
        if (c >= 0) {
          i |= c << (18 - j * 6);
        } else {
          j--;
        }
      }

      // Add the bytes
      destArray[d++] = (byte) (i >> 16);
      if (d < len) {
        destArray[d++] = (byte) (i >> 8);
        if (d < len) {
          destArray[d++] = (byte) i;
        }
      }
    }

    return destArray;
  }

  /**
   * Encodes a raw byte array into a BASE64 <code>String</code> representation i accordance with RFC 2045.
   *
   * @param sourceArray The bytes to convert. If <code>null</code> or length 0 an empty array will be returned.
   * @param lineSep Optional "\r\n" after 76 characters, unless end of file.<br>
   * No line separator will be in breach of RFC 2045 which specifies max 76 per line but will be a
   * little faster.
   * @return A BASE64 encoded array. Never <code>null</code>.
   */
  public final static String encodeToString(byte[] sourceArray, boolean lineSep) {
    // Reuse char[] since we can't create a String incrementally anyway and StringBuffer/Builder would be slower.
    return new String(encodeToChar(sourceArray, lineSep));
  }

  /**
   * Decodes a BASE64 encoded <code>String</code>. All illegal characters will be ignored and can handle both strings with
   * and without line separators.<br>
   * <b>Note!</b> It can be up to about 2x the speed to call <code>decode(str.toCharArray())</code> instead. That
   * will create a temporary array though. This version will use <code>str.charAt(i)</code> to iterate the string.
   *
   * @param str The source string. <code>null</code> or length 0 will return an empty array.
   * @return The decoded array of bytes. May be of length 0. Will be <code>null</code> if the legal characters
   * (including '=') isn't divideable by 4. (I.e. definitely corrupted).
   */
  public final static byte[] decode(String str) {
    // Check special case
    int sourceLength = str != null ? str.length() : 0;
    if (sourceLength == 0) {
      return new byte[0];
    }
 
    // Count illegal characters (including '\r', '\n') to know what size the returned array will be,
    // so we don't have to reallocate & copy it later.
    int sepCnt = 0; // Number of separator characters. (Actually illegal characters, but that's a bonus...)
    for (int i = 0; i < sourceLength; i++) // If input is "pure" (I.e. no line separators or illegal chars) base64 this loop can be commented out.
    {
      if (IntegerArray[str.charAt(i)] < 0) {
        sepCnt++;
      }
    }

    // Check so that legal chars (including '=') are evenly divideable by 4 as specified in RFC 2045.
    if ((sourceLength - sepCnt) % 4 != 0) {
      return null;
    }

    // Count '=' at end
    int pad = 0;
    for (int i = sourceLength; i > 1 && IntegerArray[str.charAt(--i)] <= 0;) {
      if (str.charAt(i) == '=') {
        pad++;
      }
    }

    int len = ((sourceLength - sepCnt) * 6 >> 3) - pad;

    byte[] destArray = new byte[len];       // Preallocate byte[] of exact length

    for (int s = 0, d = 0; d < len;) {
      // Assemble three bytes into an int from four "valid" characters.
      int i = 0;
      for (int j = 0; j < 4; j++) {   // j only increased if a valid char was found.
        int c = IntegerArray[str.charAt(s++)];
        if (c >= 0) {
          i |= c << (18 - j * 6);
        } else {
          j--;
        }
      }
      // Add the bytes
      destArray[d++] = (byte) (i >> 16);
      if (d < len) {
        destArray[d++] = (byte) (i >> 8);
        if (d < len) {
          destArray[d++] = (byte) i;
        }
      }
    }
    return destArray;
  }
}
