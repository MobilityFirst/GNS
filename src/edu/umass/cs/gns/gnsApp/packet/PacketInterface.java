/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.gnsApp.packet;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * The basic packet interface.
 * 
 * @author westy
 */
public interface PacketInterface {
  
  /**
   * Convert this to a JSON Object.
   * 
   * @return JSONObject
   * @throws JSONException
   */
  public JSONObject toJSONObject() throws JSONException;
  
}
