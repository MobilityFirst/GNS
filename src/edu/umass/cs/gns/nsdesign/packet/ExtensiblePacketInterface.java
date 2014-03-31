/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gns.nsdesign.packet;

import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public interface ExtensiblePacketInterface {
  
  /**
   * Add the fields from this packet to the JSON object.
   * 
   * @param json
   * @throws org.json.JSONException
   */
  public void addToJSONObject(JSONObject json) throws JSONException;
  
}
