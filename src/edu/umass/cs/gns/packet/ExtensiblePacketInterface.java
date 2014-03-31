/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gns.packet;

import org.json.JSONException;
import org.json.JSONObject;
/**************** FIXME Package deprecated by nsdesign/packet. this will soon be deleted. **/
/**
 *
 * @author westy
 * @deprecated
 */
public interface ExtensiblePacketInterface {
  
  /**
   * Add the fields from this packet to the JSON object.
   * 
   * @param json
   * @throws JSONException
   */
  public void addToJSONObject(JSONObject json) throws JSONException;
  
}
