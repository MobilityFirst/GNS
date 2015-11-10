/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gnsserver.gnsApp;

import edu.umass.cs.gnsserver.utils.ValuesMap;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Represents the state for a name record that GNS will transfer to (or received from) coordinator,
 * and other replicas.
 * This exists because we need the valuesMap from the NameRecord but 
 * we also want the TTL as well.
 *
 *
 * Created by abhigyan on 3/29/14.
 */
// FIXME: This could probably go away if the NameRecord class was rewritten.
public class NRState {
  
  final static String SEPARATOR = ":::"; // Made this something that probably won't appear in JSON. - Westy

  /**
   *
   */
  public final ValuesMap valuesMap;

  /**
   *
   */
  public final int ttl;

  /**
   *
   * @param valuesMap
   * @param ttl
   */
  public NRState(ValuesMap valuesMap, int ttl) {
    this.valuesMap = valuesMap;
    this.ttl = ttl;
  }

  /**
   *
   * @param state
   * @throws JSONException
   */
  public NRState(String state) throws JSONException {
    int ttlIndex;
    if (state != null &&  (ttlIndex = state.indexOf(SEPARATOR)) != -1) {
      this.ttl = Integer.parseInt(state.substring(0, ttlIndex));
      this.valuesMap = new ValuesMap(new JSONObject(state.substring(ttlIndex + SEPARATOR.length())));
    } else if (state != null) {
      this.ttl = 0;
      this.valuesMap = new ValuesMap(new JSONObject(state));
    } else {
      this.ttl = 0;
      this.valuesMap = new ValuesMap();
    } 
  }

  @Override
  public String toString() {
    return ttl + ":::" + valuesMap; // need to convert to json as it will be reinserted into database.
  }

}
