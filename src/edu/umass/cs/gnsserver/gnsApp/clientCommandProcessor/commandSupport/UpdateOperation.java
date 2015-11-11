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
 *  Initial developer(s): Abhigyan Sharma, Westy
 *
 */
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport;

import edu.umass.cs.gnscommon.GnsProtocol;
import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.utils.ResultValue;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Defines a variety of update operations that can be performed on a field.
 * There are two formats supported for fields. A JSONObject based format and
 * the older JSONArray format. In general you don't want to mix the two, but
 * in some cases you can use both together.
 */
public enum UpdateOperation {
  //  Args:         singleField, skipread, upsert
  /**
   * Updates the value of field using the JSON Object. 
   * Fields already in the record not in the JSON Object are not touched.
   */
  USER_JSON_REPLACE(false, true, false), // doesn't require a read
  /**
   * Updates the value of field using the JSON Object creating the field if it does not exist.
   * Fields already in the record not in the JSON Object are not touched.
   */
  USER_JSON_REPLACE_OR_CREATE(false, true, true, USER_JSON_REPLACE), // doesn't require a read
  //
  // NOTE: The following all user the "older" JSONArray format. 
  //
  /**
   * Creates the field with the given value.
   */
  SINGLE_FIELD_CREATE(true, false, false),
  /**
   * Slightly different than the rest in that it actually removes the field.
   *
   */
  SINGLE_FIELD_REMOVE_FIELD(true, true, false), // doesn't require a read

  /**
   * Deletes all the values from the field.
   */
  SINGLE_FIELD_CLEAR(true, false, false),
  /**
   * Replaces the current values with this list.
   */
  SINGLE_FIELD_REPLACE_ALL(true, true, false), // doesn't require a read

  /**
   * Deletes the values from the current values.
   */
  SINGLE_FIELD_REMOVE(true, false, false),
  /**
   * Treats the value as a singleton and replaces the current value with this one.
   */
  SINGLE_FIELD_REPLACE_SINGLETON(true, false, false),
  /**
   * Appends the given value onto the current values TREATING THE LIST AS A SET - meaning no duplicates.
   */
  SINGLE_FIELD_APPEND(true, false, false),
  /**
   * An upsert operation similar to <code>SINGLE_FIELD_APPEND</code> that creates the field if it does not exist.
   */
  SINGLE_FIELD_APPEND_OR_CREATE(true, false, true, SINGLE_FIELD_APPEND),
  /**
   * An upsert operation similar to <code>SINGLE_FIELD_REPLACE_ALL</code> that creates the field if it does not exist.
   */
  SINGLE_FIELD_REPLACE_ALL_OR_CREATE(true, false, true, SINGLE_FIELD_REPLACE_ALL),
  /**
   * Appends the given value onto the current values TREATING THE LIST AS A LIST - duplicates are not removed.
   */
  SINGLE_FIELD_APPEND_WITH_DUPLICATION(true, false, false),
  /**
   * Replaces all instances of the old value with the new value - if old and new are lists does a pairwise replacement.
   */
  SINGLE_FIELD_SUBSTITUTE(true, false, false),
  /**
   * Sets the element of the list specified by the argument index.
   */
  SINGLE_FIELD_SET(true, false, false),
  /**
   * Sets the field to null (a singleton).
   */
  SINGLE_FIELD_SET_FIELD_NULL(true, false, false),;
  //
  boolean singleFieldOperation;
  boolean ableToSkipRead;
  boolean upsert;
  UpdateOperation nonUpsertEquivalent = null;

  private UpdateOperation(boolean singleFieldOperation, boolean ableToSkipRead, boolean upsert) {
    this.singleFieldOperation = singleFieldOperation;
    this.ableToSkipRead = ableToSkipRead;
    this.upsert = upsert;
  }

  private UpdateOperation(boolean singleFieldOperation, boolean ableToSkipRead, boolean upsert, UpdateOperation nonUpsertEquivalent) {
    this.singleFieldOperation = singleFieldOperation;
    this.ableToSkipRead = ableToSkipRead;
    this.upsert = upsert;
    this.nonUpsertEquivalent = nonUpsertEquivalent;
  }

  /**
   * Indicates that this operation only operates on a single field (key/value) in the given record.
   *
   * @return a boolean
   */
  public boolean isSingleFieldOperation() {
    return singleFieldOperation;
  }

  /**
   * Indicates that this operation can proceed without having to first read the record from the database.
   *
   * In other words, whatever this operation does it ignores the current contents of the database - say
   * maybe something like am "overwrite all the contents of the given field" operation.
   *
   * @return a boolean
   */
  public boolean isAbleToSkipRead() {
    return ableToSkipRead;
  }

  /**
   * Indicates that this operation will attempt to create a field that does not already exist.
   *
   * @return a boolean if this is an upsert operation
   */
  public boolean isUpsert() {
    return upsert;
  }

  /**
   * Returns the operation similar to an upsert operation that is not an upsert operation.
   *
   * @return the equivalent non-upsert operation
   */
  public UpdateOperation getNonUpsertEquivalent() {
    return nonUpsertEquivalent;
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
   * @param argument
   * @param userJSON - mutually exclusive of newValues, oldValues and argument (and only for non-SingleFieldOperations)
   * @param operation
   * @return false if the value was not updated true otherwise
   */
  public static boolean updateValuesMap(ValuesMap valuesMap, String key,
          ResultValue newValues, ResultValue oldValues, int argument,
          ValuesMap userJSON, UpdateOperation operation) {
    if (operation.isSingleFieldOperation()) {
      ResultValue valuesList = valuesMap.getAsArray(key);
      if (valuesList == null) {
        valuesList = new ResultValue();
      }
      if (UpdateSingleField(valuesList, newValues, oldValues, argument, operation)) {
        valuesMap.putAsArray(key, valuesList);
        return true;
      } else {
        return false;
      }
    } else {
      assert userJSON != null;
      // the valuesMap read from the database has user fields and hidden "systems" fields so we
      // have to update it field-by-field using writeToValuesMap so as not to clobber 
      // any of the systems fields
      // This also supports dot notation.
      return userJSON.writeToValuesMap(valuesMap);
    }
  }

  private static boolean valuesListHasNullFirstElement(ResultValue valuesList) {
    return !valuesList.isEmpty() && valuesList.get(0).equals(GnsProtocol.NULL_RESPONSE);
  }

  private static boolean UpdateSingleField(ResultValue valuesList, ResultValue newValues, ResultValue oldValues,
          int argument, UpdateOperation operation) {
    switch (operation) {
      case SINGLE_FIELD_CLEAR:
        valuesList.clear();
        return true;
      case SINGLE_FIELD_CREATE:
      case SINGLE_FIELD_REPLACE_ALL_OR_CREATE:
      case SINGLE_FIELD_REPLACE_ALL:
        valuesList.clear();
        valuesList.addAll(newValues);
        return true;
      case SINGLE_FIELD_APPEND_WITH_DUPLICATION:
        // check for a null list and clear it if it is
        if (valuesListHasNullFirstElement(valuesList)) {
          valuesList.clear();
        }
        if (valuesList.addAll(newValues)) {
          return true;
        } else {
          return false;
        }
      case SINGLE_FIELD_APPEND_OR_CREATE:
      case SINGLE_FIELD_APPEND:
        Set<Object> singles; // use a hash to remove duplicates
        // check for a null list don't use the current values if it is
        if (valuesListHasNullFirstElement(valuesList)) {
          singles = new HashSet<>();
        } else {
          singles = new HashSet<>(valuesList);
        }
        singles.addAll(newValues);
        // clear the old values and
        valuesList.clear();
        // and the new ones
        valuesList.addAll(singles);
        return true;
      case SINGLE_FIELD_REMOVE:
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
      case SINGLE_FIELD_REPLACE_SINGLETON:
        valuesList.clear();
        if (!newValues.isEmpty()) {
          valuesList.add(newValues.get(0));
        }
        return true;
      case SINGLE_FIELD_SUBSTITUTE:
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
      case SINGLE_FIELD_SET:
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
      case SINGLE_FIELD_SET_FIELD_NULL:
        // already null return false
        if (valuesListHasNullFirstElement(valuesList)) {
          return false;
        }
        valuesList.clear();
        valuesList.add(GnsProtocol.NULL_RESPONSE);
        return true;
      default:
        return false;
    }
  }
}
