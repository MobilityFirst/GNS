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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONString;

public class CanonicalJSON {

  /**
   * Main function to generate canonical strings for JSON objects.
   *
   * @param x
   * @return
   */
  public static String getCanonicalForm(JSONObject x) {
    long startTime = System.currentTimeMillis();
    String result = renderSimpleCanonicalJSON(x);
    DelayProfiler.updateDelay("getCanonicalForm", startTime);
    return result;
  }

  /**
   * Helper function to generate canonical strings for JSON strings.
   *
   * @param x
   * @return
   */
  public static String getCanonicalForm(String x) {
    try {
      return getCanonicalForm(new JSONObject(x));
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
          t.add((String)i.next());
        }
        Iterator<String> keys = t.iterator();

        StringBuilder sb = new StringBuilder("{");
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
          return renderSimpleCanonicalJSON(new JSONObject((Map<?, ?>) x));
        }
        if (x instanceof Collection) {
          return renderSimpleCanonicalJSON(new JSONArray((Collection<?>) x));
        }
        if (x.getClass().isArray()) {
          return renderSimpleCanonicalJSON(new JSONArray(x));
        }
        return JSONObject.quote(x.toString());
      }
    } catch (Exception e) {
      return null;
    }
  }
}
