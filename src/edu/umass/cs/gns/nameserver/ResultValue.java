/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.nameserver;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.util.JSONUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONException;

// PLEASE DON'T DELETE THIS CLASS WITHOUT TALKING TO WESTY FIRST!
/**
 * This class is the underlying representation used to transmit the value in the key / value store from
 * the client to the server.
 * 
 * One of the ideas here is to make it easy to mess with the implementation of this.
 * The other idea is that we want to support Strings AND Numbers in this list at some point.
 * 
 * @author westy
 */
public class ResultValue extends ArrayList<Object> {

  public ResultValue() {
  }

  public ResultValue(Collection<? extends Object> clctn) {
    super(clctn);
  }

  /**
   * String should be a string representation of a JSON Array, that is, [thing, thing, thing... thing]
   * 
   * @param string 
   */
  public ResultValue(String string) throws JSONException {
    super(JSONUtils.JSONArrayToArrayList(new JSONArray(string)));
  }

  /**
   * Converts this ResultValue to a ResultValueString which insures that they're all strings.
   * 
   * @return 
   */
  public ResultValueString toResultValueString() {
    ResultValueString result = new ResultValueString();
    for (Object element : this) {
      result.add((String) element);
    }
    return result;
  }

  /**
   * Converts this ResultValue to a Set<String> which insures that they're all strings.
   * 
   * @return 
   */
  public Set<String> toStringSet() {
    Set<String> result = new HashSet<String>();
    for (Object element : this) {
      result.add((String) element);
    }
    return result;
  }
}
