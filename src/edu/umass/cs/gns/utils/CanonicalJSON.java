/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.utils;

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
        TreeSet<String> t = new TreeSet<String>();
        Iterator<?> i = theObject.keys();
        while (i.hasNext()) {
          t.add((String) i.next());
        }
        Iterator<String> keys = t.iterator();

        StringBuffer sb = new StringBuffer("{");
        while (keys.hasNext()) {
          if (sb.length() > 1) {
            sb.append(',');
          }
          Object o = keys.next();
          sb.append(JSONObject.quote(o.toString()));
          sb.append(':');
          sb.append(renderSimpleCanonicalJSON(theObject.get(o.toString())));
        }
        sb.append('}');
        return sb.toString();
      } else if (x instanceof JSONArray) {
        JSONArray theArray = (JSONArray) x;
        StringBuffer sb = new StringBuffer();
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
            throw new JSONException(e);
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
          return renderSimpleCanonicalJSON(new JSONObject((Map<?, ?>) x)).toString();
        }
        if (x instanceof Collection) {
          return renderSimpleCanonicalJSON(new JSONArray((Collection<?>) x)).toString();
        }
        if (x.getClass().isArray()) {
          return renderSimpleCanonicalJSON(new JSONArray(x)).toString();
        }
        return JSONObject.quote(x.toString());
      }
    } catch (Exception e) {
      return null;
    }
  }
}
