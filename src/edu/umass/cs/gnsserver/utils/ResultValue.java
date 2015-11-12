/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsserver.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONException;

/**
 * This class is one representation used to transmit the value in the key / value store from
 * the client to the server. 
 * 
 * This extends ArrayList (like a JSONArray), but we made a class for this (as opposed to just using a
 * ArrayList or a JSONArray) so we can dispatch off it in methods and also more easily instrument it.
 * 
 * One of the ideas here is to make it easy to mess with the implementation of this.
 * The other idea is that we also support Strings AND Numbers in this list at some point.
 * 
 * It originally this was the only representation used. The database stored keys and values where
 * the value was an array. This is no longer the case. A guid is now a full JSON Object where each
 * value can be anything including a nested object.
 * 
 * Some internal fields still use this as their base representation in the database. User fields do not.
 * 
 * See also ValuesMap.
 * 
 * @author westy
 */
public class ResultValue extends ArrayList<Object> {

  /**
   * Create an empty ResultValue instance.
   */
  public ResultValue() {
  }

  /**
   * Create a ResultValue instance from a collection.
   * 
   * @param collection
   */
  public ResultValue(Collection<? extends Object> collection) {
    super(collection);
  }

  /**
   * String should be a string representation of a JSON Array, that is, [thing, thing, thing... thing]
   * 
   * @param string 
   * @throws org.json.JSONException 
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
    this.stream().forEach((element) -> {
      result.add((String) element);
    });
    return result;
  }

  /**
   * Converts this ResultValue to a Set which insures that they're all strings.
   * 
   * @return 
   */
  public Set<String> toStringSet() {
    Set<String> result = new HashSet<>();
    this.stream().forEach((element) -> {
      result.add((String) element);
    });
    return result;
  }
  
  // FIXME: CALLING THIS SOMETIMES HANGS THINGS!
//  /**
//   * Limits the size of fields printed by toReasonableString.
//   */
//  public static int reasonableFieldSize = 1500;
//
//  public String toReasonableString() {
//    return Util.ellipsize(this.toString(), reasonableFieldSize);
//  }
  
  
}
