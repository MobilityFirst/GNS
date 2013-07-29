/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gns.nameserver;

import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class StatsInfo {

  public int read;
  public int write;

  public StatsInfo(int read, int write) {
    this.read = read;
    this.write = write;
  }

  public StatsInfo(JSONObject json) throws JSONException {
    this.read = json.getInt("read");
    this.write = json.getInt("write");
  }

  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    json.put("read", read);
    json.put("write", write);
    return json;
  }

  @Override
  public String toString() {
    return "Read:" + read + " Write:" + write;
  }
}