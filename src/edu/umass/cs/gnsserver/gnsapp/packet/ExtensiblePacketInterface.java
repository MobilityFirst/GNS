
package edu.umass.cs.gnsserver.gnsapp.packet;

import org.json.JSONException;
import org.json.JSONObject;


public interface ExtensiblePacketInterface {
  

  public void addToJSONObject(JSONObject json) throws JSONException;
  
}
