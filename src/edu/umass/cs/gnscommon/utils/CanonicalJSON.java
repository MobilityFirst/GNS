
package edu.umass.cs.gnscommon.utils;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONString;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;


public class CanonicalJSON {


  public static String getCanonicalForm(JSONObject json) {
    return renderSimpleCanonicalJSON(json);
  }


  public static String getCanonicalForm(String json) {
    try {
      return getCanonicalForm(new JSONObject(json));
    } catch (JSONException e) {
      return null;
    }
  }


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
            throw new Exception(e);
          }
          if (object instanceof String) {
            return (String) object;
          }
          throw new Exception("Bad value from toJSONString: " + object);
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

  public static String[] getNames(JSONObject jo) {
    int length = jo.length();
    if (length == 0) {
      return null;
    }
    Iterator iterator = jo.keys();
    String[] names = new String[length];
    int i = 0;
    while (iterator.hasNext()) {
      names[i] = (String) iterator.next();
      i += 1;
    }
    return names;
  }

  public static Object stringToValue(String string) {
    if (string.equals("")) {
      return string;
    }
    if (string.equalsIgnoreCase("true")) {
      return Boolean.TRUE;
    }
    if (string.equalsIgnoreCase("false")) {
      return Boolean.FALSE;
    }
    if (string.equalsIgnoreCase("null")) {
      return JSONObject.NULL;
    }

    /*
     * If it might be a number, try converting it.
     * We support the non-standard 0x- convention.
     * If a number cannot be produced, then the value will just
     * be a string. Note that the 0x-, plus, and implied string
     * conventions are non-standard. A JSON parser may accept
     * non-JSON forms as long as it accepts all correct JSON forms.
     */
    char b = string.charAt(0);
    if ((b >= '0' && b <= '9') || b == '.' || b == '-' || b == '+') {
      if (b == '0' && string.length() > 2
              && (string.charAt(1) == 'x' || string.charAt(1) == 'X')) {
        try {
          return new Integer(Integer.parseInt(string.substring(2), 16));
        } catch (Exception ignore) {
        }
      }
      try {
        if (string.indexOf('.') > -1
                || string.indexOf('e') > -1 || string.indexOf('E') > -1) {
          return Double.valueOf(string);
        } else {
          Long myLong = new Long(string);
          if (myLong.longValue() == myLong.intValue()) {
            return new Integer(myLong.intValue());
          } else {
            return myLong;
          }
        }
      } catch (Exception ignore) {
      }
    }
    return string;
  }

}
