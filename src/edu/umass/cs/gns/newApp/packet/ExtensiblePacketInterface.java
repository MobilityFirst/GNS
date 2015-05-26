/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.newApp.packet;

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
