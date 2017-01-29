
package edu.umass.cs.gnsserver.gnsapp.packet;

import org.json.JSONException;
import org.json.JSONObject;


public interface PacketInterface {
  

  public JSONObject toJSONObject() throws JSONException;
  
}
