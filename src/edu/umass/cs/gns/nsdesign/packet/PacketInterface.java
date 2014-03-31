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
public interface PacketInterface {
  
  public JSONObject toJSONObject() throws JSONException;
  
}
