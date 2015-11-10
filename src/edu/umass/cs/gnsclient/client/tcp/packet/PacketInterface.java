/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gnsclient.client.tcp.packet;

import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public interface PacketInterface {
  
  public JSONObject toJSONObject() throws JSONException;
  
}
