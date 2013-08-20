/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gns.nameserver;

import java.util.HashMap;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class StatsInfo extends HashMap<String,Integer> 
// implements Serializable 
{

  //private static final long serialVersionUID = 2326392043474125897L;

//  public StatsInfo() {
//  }
// 
  public StatsInfo(int read, int write) {
    put("read", read);
    put("write", write);
  }

  public StatsInfo(JSONObject json) throws JSONException {
    initFromJSONObject(json);
  }

  private void initFromJSONObject(JSONObject json) throws JSONException {
    put("read", json.getInt("read"));
    put("write", json.getInt("write"));
  }

  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    json.put("read", get("read"));
    json.put("write", get("write"));
    return json;
  }

//  private void writeObject(ObjectOutputStream s) throws IOException {
//    try {
//      s.writeUTF(toJSONObject().toString());
//    } catch (JSONException e) {
//      throw new IOException(e);
//    }
//  }
//
//  private void readObject(ObjectInputStream s) throws IOException {
//    try {
//      initFromJSONObject(new JSONObject(s.readUTF()));
//    } catch (JSONException e) {
//      throw new IOException(e);
//    }
//  }
  
  public int getRead() {
    return get("read");
  }
  
   public int getWrite() {
    return get("write");
  }
   
}