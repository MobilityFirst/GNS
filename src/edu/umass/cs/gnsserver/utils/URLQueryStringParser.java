
package edu.umass.cs.gnsserver.utils;

import edu.umass.cs.gnscommon.utils.URIEncoderDecoder;

import java.io.UnsupportedEncodingException;
import java.util.Collection;


public class URLQueryStringParser {

  private final String queryString;


  private int paramBegin;
  private int paramEnd = -1;
  private int paramNameEnd;
  private String paramName;
  private String paramValue;


  public URLQueryStringParser(String queryString) {
    this.queryString = queryString;
  }


  public boolean next() {
    int len = queryString.length();
    while (true) {
      if (paramEnd == len) {
        return false;
      }
      paramBegin = paramEnd == -1 ? 0 : paramEnd + 1;
      int idx = queryString.indexOf('&', paramBegin);
      paramEnd = idx == -1 ? len : idx;
      if (paramEnd > paramBegin) {
        idx = queryString.indexOf('=', paramBegin);
        paramNameEnd = idx == -1 || idx > paramEnd ? paramEnd : idx;
        paramName = null;
        paramValue = null;
        return true;
      }
    }
  }


  public boolean search(Collection<String> names) {
    while (next()) {
      if (names.contains(getName())) {
        return true;
      }
    }
    return false;
  }


  public String getName() {
    if (paramName == null) {
      paramName = queryString.substring(paramBegin, paramNameEnd);
    }
    return paramName;
  }


  public String getValue() {
    if (paramValue == null) {
      if (paramNameEnd == paramEnd) {
        return null;
      }
      try {
        paramValue = URIEncoderDecoder.decode(queryString.substring(paramNameEnd + 1, paramEnd));
      } catch (UnsupportedEncodingException ex) {
        // TODO: URIEncoderDecoder.decode should be changed to not throw
        // UnsupportedEncodingException since this actually never happens (the charset
        // being looked up is UTF-8 which always exists).
        throw new Error(ex);
      }
    }
    return paramValue;
  }
}
