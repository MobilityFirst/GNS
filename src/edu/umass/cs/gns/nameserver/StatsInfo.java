/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gns.nameserver;

import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashMap;

/**
 *
 * @author westy
 */
public class StatsInfo extends HashMap<String,Integer>
// implements Serializable 
{
  public static final String READ = "r";
  public static final String WRITE = "w";
  //private static final long serialVersionUID = 2326392043474125897L;

//  public StatsInfo() {
//  }
// 
  public StatsInfo(int read, int write) {
    put(READ, read);
    put(WRITE, write);
  }

  public StatsInfo(JSONObject json) throws JSONException {
    initFromJSONObject(json);
  }

  private void initFromJSONObject(JSONObject json) throws JSONException {
    put(READ, json.getInt(READ));
    put(WRITE, json.getInt(WRITE));
  }

  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    json.put(READ, get(READ));
    json.put(WRITE, get(WRITE));
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
    return get(READ);
  }
  
   public int getWrite() {
    return get(WRITE);
  }


}