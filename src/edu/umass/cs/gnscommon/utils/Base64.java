
package edu.umass.cs.gnscommon.utils;

import java.util.Arrays;


public class Base64 {

  private static final char[] CHARACTER_ARRAY = 
          "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".toCharArray();
  private static final int[] INTEGER_ARRAY = new int[256];

  static {
    Arrays.fill(INTEGER_ARRAY, -1);
    for (int i = 0, iS = CHARACTER_ARRAY.length; i < iS; i++) {
      INTEGER_ARRAY[CHARACTER_ARRAY[i]] = i;
    }
    INTEGER_ARRAY['='] = 0;
  }


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
      destArray[d++] = CHARACTER_ARRAY[(i >>> 18) & 0x3f];
      destArray[d++] = CHARACTER_ARRAY[(i >>> 12) & 0x3f];
      destArray[d++] = CHARACTER_ARRAY[(i >>> 6) & 0x3f];
      destArray[d++] = CHARACTER_ARRAY[i & 0x3f];

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
      destArray[destLen - 4] = CHARACTER_ARRAY[i >> 12];
      destArray[destLen - 3] = CHARACTER_ARRAY[(i >>> 6) & 0x3f];
      destArray[destLen - 2] = left == 2 ? CHARACTER_ARRAY[i & 0x3f] : '=';
      destArray[destLen - 1] = '=';
    }
    return destArray;
  }


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
      if (INTEGER_ARRAY[sourceArray[i]] < 0) {
        sepCnt++;
      }
    }

    // Check so that legal chars (including '=') are evenly divideable by 4 as specified in RFC 2045.
    if ((sourceLength - sepCnt) % 4 != 0) {
      return null;
    }

    int pad = 0;
    for (int i = sourceLength; i > 1 && INTEGER_ARRAY[sourceArray[--i]] <= 0;) {
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
        int c = INTEGER_ARRAY[sourceArray[s++]];
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
      destArray[d++] = (byte) CHARACTER_ARRAY[(i >>> 18) & 0x3f];
      destArray[d++] = (byte) CHARACTER_ARRAY[(i >>> 12) & 0x3f];
      destArray[d++] = (byte) CHARACTER_ARRAY[(i >>> 6) & 0x3f];
      destArray[d++] = (byte) CHARACTER_ARRAY[i & 0x3f];

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
      destArray[destLen - 4] = (byte) CHARACTER_ARRAY[i >> 12];
      destArray[destLen - 3] = (byte) CHARACTER_ARRAY[(i >>> 6) & 0x3f];
      destArray[destLen - 2] = left == 2 ? (byte) CHARACTER_ARRAY[i & 0x3f] : (byte) '=';
      destArray[destLen - 1] = '=';
    }
    return destArray;
  }


  public final static byte[] decode(byte[] sourceArray) {
    // Check special case
    int sourceLength = sourceArray.length;

    // Count illegal characters (including '\r', '\n') to know what size the returned array will be,
    // so we don't have to reallocate & copy it later.
    int sepCnt = 0; // Number of separator characters. (Actually illegal characters, but that's a bonus...)
    for (int i = 0; i < sourceLength; i++) // If input is "pure" (I.e. no line separators or illegal chars) base64 this loop can be commented out.
    {
      if (INTEGER_ARRAY[sourceArray[i] & 0xff] < 0) {
        sepCnt++;
      }
    }

    // Check so that legal chars (including '=') are evenly divideable by 4 as specified in RFC 2045.
    if ((sourceLength - sepCnt) % 4 != 0) {
      return null;
    }

    int pad = 0;
    for (int i = sourceLength; i > 1 && INTEGER_ARRAY[sourceArray[--i] & 0xff] <= 0;) {
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
        int c = INTEGER_ARRAY[sourceArray[s++] & 0xff];
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


  public final static String encodeToString(byte[] sourceArray, boolean lineSep) {
    // Reuse char[] since we can't create a String incrementally anyway and StringBuffer/Builder would be slower.
    return new String(encodeToChar(sourceArray, lineSep));
  }


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
      if (INTEGER_ARRAY[str.charAt(i)] < 0) {
        sepCnt++;
      }
    }

    // Check so that legal chars (including '=') are evenly divideable by 4 as specified in RFC 2045.
    if ((sourceLength - sepCnt) % 4 != 0) {
      return null;
    }

    // Count '=' at end
    int pad = 0;
    for (int i = sourceLength; i > 1 && INTEGER_ARRAY[str.charAt(--i)] <= 0;) {
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
        int c = INTEGER_ARRAY[str.charAt(s++)];
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
