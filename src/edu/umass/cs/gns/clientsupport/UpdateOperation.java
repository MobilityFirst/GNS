/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.clientsupport;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.util.ValuesMap;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * CREATE - creates the field with the given value;<br>
 * REPLACE_ALL - replaces the current values with this list;<br>
 * REMOVE - deletes the values from the current values;<br>
 * REPLACESINGLETON - treats the value as a singleton and replaces the current value with this one<br>
 * CLEAR - deletes all the values from the field<br>
 * APPEND - appends the given value onto the current values TREATING THE LIST AS A SET - meaning no duplicates<br>
 * APPEND_OR_CREATE - an upsert operation similar to APPEND that creates the field if it does not exist<br>
 * REPLACE_ALL_OR_CREATE - an upsert operation similar to REPLACE_ALL that creates the field if it does not exist<br>
 * APPEND_WITH_DUPLICATION - appends the given value onto the current values TREATING THE LIST AS A LIST - duplicates are not removed<br>
 * SUBSTITUTE - replaces all instances of the old value with the new value - if old and new are lists does a pairwise replacement<br>
 *
 * REMOVE_FIELD - slightly different than the rest in that it actually removes the field
 * SET - sets the element of the list specified by the argument index
 */
public enum UpdateOperation {

  CREATE(false),
  REMOVE_FIELD(false),
  CLEAR(false),
  REPLACE_ALL(false),
  REMOVE(false),
  REPLACESINGLETON(false),
  APPEND(false),
  APPEND_OR_CREATE(true, APPEND),
  REPLACE_ALL_OR_CREATE(true, REPLACE_ALL),
  APPEND_WITH_DUPLICATION(false),
  SUBSTITUTE(false),
  SET(false),
  SETFIELDNULL(false);
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
  public static boolean updateValuesMap(ValuesMap valuesMap, String key,
          ResultValue newValues, ResultValue oldValues, int argument,
          UpdateOperation operation) {
    ResultValue valuesList = valuesMap.get(key);
    if (valuesList == null) {
      valuesList = new ResultValue();
    }
    if (updateValuesList(valuesList, key, newValues, oldValues, argument, operation)) {
      valuesMap.put(key, valuesList);
      return true;
    } else {
      return false;
    }
  }

  private static boolean valuesListHasNullFirstElement(ResultValue valuesList) {
    return !valuesList.isEmpty() && valuesList.get(0).equals(Defs.NULLRESPONSE);
  }

  private static boolean updateValuesList(ResultValue valuesList, String key,
          ResultValue newValues, ResultValue oldValues, int argument,
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
        // check for a null list and clear it if it is
        if (valuesListHasNullFirstElement(valuesList)) {
          valuesList.clear();
        }
        if (valuesList.addAll(newValues)) {
          return true;
        } else {
          return false;
        }
      case APPEND_OR_CREATE:
      case APPEND:
        Set singles; // use a hash to remove duplicates
        // check for a null list don't use the current values if it is
        if (valuesListHasNullFirstElement(valuesList)) {
          singles = new HashSet();
        } else {
          singles = new HashSet(valuesList);
        }
        singles.addAll(newValues);
        // clear the old values and
        valuesList.clear();
        // and the new ones
        valuesList.addAll(singles);
        return true;
      case REMOVE:
        GNS.getLogger().fine("Remove " + newValues + "\tValues list = " + valuesList);
        // check for a null list reset it to empty and return false if it is
        if (valuesListHasNullFirstElement(valuesList)) {
          valuesList.clear();
          return false;
        }
        // otherwise remove all the values if they exists
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
        // check for a null list reset it to empty and return false if it is
        if (valuesListHasNullFirstElement(valuesList)) {
          valuesList.clear();
          return false;
        }
        // otherwise do the substitue thing
        boolean changed = false;
        if (oldValues != null) {
          for (Iterator<Object> oldIter = oldValues.iterator(), newIter = newValues.iterator();
                  oldIter.hasNext() && newIter.hasNext();) {
            Object oldValue = oldIter.next();
            Object newValue = newIter.next();
            if (Collections.replaceAll(valuesList, oldValue, newValue)) {
              changed = true;
            }
          }
        }
        return changed;
      case SET:
        // check for a null list reset it to empty and return false if it is
        if (valuesListHasNullFirstElement(valuesList)) {
          valuesList.clear();
          return false;
        }
        if (!newValues.isEmpty() && argument < valuesList.size()) {
          // ignoring anything more than the first element of the new values
          valuesList.set(argument, newValues.get(0));
        }
        return true;
      case SETFIELDNULL:
        // already null return false
        if (valuesListHasNullFirstElement(valuesList)) {
          return false;
        }
        valuesList.clear();
        valuesList.add(Defs.NULLRESPONSE);
        return true;
      default:
        return false;
    }
  }
}
