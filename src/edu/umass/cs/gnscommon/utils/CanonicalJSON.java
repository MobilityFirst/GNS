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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONString;

/**
 * Generates canonical JSON strings.
 * Identical to the standard code to render a JSON object,
 * except it forces the keys for maps to be listed in sorted order.
 *
 * @author westy
 */
public class CanonicalJSON {

  /**
   * Main function to generate canonical strings for JSON objects.
   *
   * @param json
   * @return a string
   */
  public static String getCanonicalForm(JSONObject json) {
    return renderSimpleCanonicalJSON(json);
  }

  /**
   * Helper function to generate canonical strings for JSON strings.
   *
   * @param json
   * @return a string
   */
  public static String getCanonicalForm(String json) {
    try {
      return getCanonicalForm(new JSONObject(json));
    } catch (JSONException e) {
      return null;
    }
  }

  /* This should be identical to the standard code to render the JSON object,
   * except it forces the keys for maps to be listed in sorted order. */
  private static String renderSimpleCanonicalJSON(Object x) {
    try {
      if (x instanceof JSONObject) {
        JSONObject theObject = (JSONObject) x;

        // Sort the keys
        TreeSet<String> t = new TreeSet<>();
        Iterator<?> i = theObject.keys();
        while (i.hasNext()) {
          t.add((String) i.next());
        }
        Iterator<String> keys = t.iterator();

        StringBuilder sb = new StringBuilder("{");
        while (keys.hasNext()) {
          if (sb.length() > 1) {
            sb.append(',');
          }
          Object o = keys.next();
          sb.append(canonicalQuote(o.toString()));
          sb.append(':');
          sb.append(renderSimpleCanonicalJSON(theObject.get(o.toString())));
        }
        sb.append('}');
        return sb.toString();
      } else if (x instanceof JSONArray) {
        JSONArray theArray = (JSONArray) x;
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        int len = theArray.length();
        for (int i = 0; i < len; i += 1) {
          if (i > 0) {
            sb.append(",");
          }
          sb.append(renderSimpleCanonicalJSON(theArray.get(i)));
        }
        sb.append("]");
        return sb.toString();
      } else {
        if (x == null || x.equals(null)) {
          return "null";
        }
        if (x instanceof JSONString) {
          Object object;
          try {
            object = ((JSONString) x).toJSONString();
          } catch (Exception e) {
            throw new JSONException(e.getMessage());
          }
          if (object instanceof String) {
            return (String) object;
          }
          throw new JSONException("Bad value from toJSONString: " + object);
        }
        if (x instanceof Number) {
          return JSONObject.numberToString((Number) x);
        }
        if (x instanceof Boolean || x instanceof JSONObject
                || x instanceof JSONArray) {
          return x.toString();
        }
        if (x instanceof Map) {
          return renderSimpleCanonicalJSON(new JSONObject((Map<?, ?>) x));
        }
        if (x instanceof Collection) {
          return renderSimpleCanonicalJSON(new JSONArray((Collection<?>) x));
        }
        if (x.getClass().isArray()) {
          return renderSimpleCanonicalJSON(new JSONArray(x));
        }
        return canonicalQuote(x.toString());
      }
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Produce a string in double quotes with backslash sequences in all the
   * right places. A backslash will be inserted within &lt;/, allowing JSON
   * text to be delivered in HTML. In JSON text, a string cannot contain a
   * control character or an unescaped quote or backslash.
   * @param string A String
   * @return A String correctly formatted for insertion in a JSON text.
   */
//   This is an exact copy of JSONObject.quote() method from the org.json package.
//   This method was added to fix the Android behavior of escaping forward slashes
//   always in contrast with the open source org.json that escapes forward slashes
//   only if the preceding character is an angular bracket ('<'). 
//     
//   Also see MOB-886.
  private static String canonicalQuote(String string) {
    if (string == null || string.length() == 0) {
      return "\"\"";
    }

    char b;
    char c = 0;
    String hhhh;
    int i;
    int len = string.length();
    StringBuilder sb = new StringBuilder(len + 4);

    sb.append('"');
    for (i = 0; i < len; i += 1) {
      b = c;
      c = string.charAt(i);
      switch (c) {
        case '\\':
        case '"':
          sb.append('\\');
          sb.append(c);
          break;
        // This implements the more conserative behavior 
        case '/':
          if (b == '<') {
            sb.append('\\');
          }
          sb.append(c);
          break;
        case '\b':
          sb.append("\\b");
          break;
        case '\t':
          sb.append("\\t");
          break;
        case '\n':
          sb.append("\\n");
          break;
        case '\f':
          sb.append("\\f");
          break;
        case '\r':
          sb.append("\\r");
          break;
        default:
          if (c < ' ' || (c >= '\u0080' && c < '\u00a0')
                  || (c >= '\u2000' && c < '\u2100')) {
            hhhh = "000" + Integer.toHexString(c);
            sb.append("\\u" + hhhh.substring(hhhh.length() - 4));
          } else {
            sb.append(c);
          }
      }
    }
    sb.append('"');
    return sb.toString();
  }
}
