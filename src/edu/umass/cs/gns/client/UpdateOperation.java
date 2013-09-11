/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.client;

import edu.umass.cs.gns.nameserver.ValuesMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * REPLACE_ALL - replaces the current values with this list;<br>
 * REMOVE - deletes the values from the current values;<br>
 * REPLACESINGLETON - treats the value as a singleton and replaces the current value with this one<br>
 * CLEAR - deletes all the values from the field<br>
 * APPEND - appends the given value onto the current values TREATING THE LIST AS A SET - meaning no duplicates<br>
 * APPEND_OR_CREATE - an upsert operation similar to APPEND that creates the record if it does not exist<br>
 * REPLACE_ALL_OR_CREATE - an upsert operation similar to REPLACE_ALL that creates the record if it does not exist<br>
 * APPEND_WITH_DUPLICATION - appends the given value onto the current values TREATING THE LIST AS A LIST - duplicates are not removed<br>
 * SUBSTITUTE - replaces all instances of the old value with the new value - if old and new are lists does a pairwise replacement<br>
 */
public enum UpdateOperation {

  CREATE(false),
  CLEAR(false),
  REPLACE_ALL(false),
  REMOVE(false),
  REPLACESINGLETON(false),
  APPEND(false),
  APPEND_OR_CREATE(true, APPEND),
  REPLACE_ALL_OR_CREATE(true, REPLACE_ALL),
  APPEND_WITH_DUPLICATION(false),
  SUBSTITUTE(false);
  //
  boolean upsert;
  UpdateOperation nonUpsertEquivalent = null;

  public boolean isUpsert() {
    return upsert;
  }

  public UpdateOperation getNonUpsertEquivalent() {
    return nonUpsertEquivalent;
  }
 
  private UpdateOperation(boolean upsert) {
    this.upsert = upsert;
  }

  private UpdateOperation(boolean upsert, UpdateOperation nonUpsertEquivalent) {
    this.upsert = upsert;
    this.nonUpsertEquivalent = nonUpsertEquivalent;
  }
  
  

  /**
   * Updates a valuesMap object based on the parameters given.
   *
   * If the key doesn't exist this will create it, but NameRecord code that calls this should still check for the key existing
   * in the name record so that the client can depend on that behavior for certain operations.
   *
   * @param valuesMap
   * @param key
   * @param newValues
   * @param oldValues
   * @param operation
   * @return false if the value was not updated true otherwise
   */
  public static boolean updateValuesMap(ValuesMap valuesMap, String key, ArrayList<String> newValues, ArrayList<String> oldValues,
          UpdateOperation operation) {
    ArrayList<String> valuesList = valuesMap.get(key);
    if (valuesList == null) {
      valuesList = new ArrayList<String>();
    }
    if (updateValuesList(valuesList, key, newValues, oldValues, operation)) {
      valuesMap.put(key, valuesList);
      return true;
    } else {
      return false;
    }
  }

  private static boolean updateValuesList(ArrayList<String> valuesList, String key, ArrayList<String> newValues, ArrayList<String> oldValues,
          UpdateOperation operation) {
    switch (operation) {
      case CLEAR:
        valuesList.clear();
        return true;
      case CREATE:
      case REPLACE_ALL_OR_CREATE:
      case REPLACE_ALL:
        valuesList.clear();
        valuesList.addAll(newValues);
        return true;
      case APPEND_WITH_DUPLICATION:
        if (valuesList.addAll(newValues)) {
          return true;
        } else {
          return false;
        }
      case APPEND_OR_CREATE:
      case APPEND:
        // this is ugly
        // make it a hash which removes duplicates
        Set singles = new HashSet(valuesList);
        singles.addAll(newValues);
        // clear the old values and
        valuesList.clear();
        // and the new ones
        valuesList.addAll(singles);
        return true;
      case REMOVE:
        if (valuesList.removeAll(newValues)) {
          return true;
        } else {
          return false;
        }
      case REPLACESINGLETON:
        valuesList.clear();
        if (!newValues.isEmpty()) {
          valuesList.add(newValues.get(0));
        }
        return true;
      case SUBSTITUTE:
        boolean changed = false;
        if (oldValues != null) {
          for (Iterator<String> oldIter = oldValues.iterator(), newIter = newValues.iterator();
                  oldIter.hasNext() && newIter.hasNext();) {
            String oldValue = oldIter.next();
            String newValue = newIter.next();
            if (Collections.replaceAll(valuesList, oldValue, newValue)) {
              changed = true;
            }
          }
        }
        return changed;
      default:
        return false;
    }
  }
}
