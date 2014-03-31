/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gns.packet;

import org.json.JSONException;
import org.json.JSONObject;
/**************** FIXME Package deprecated by nsdesign/packet. this will soon be deleted. **/
/** @deprecated
 *
 * @author westy
 */
public interface PacketInterface {
  
  public JSONObject toJSONObject() throws JSONException;
  
}
