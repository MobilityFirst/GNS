
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport;

import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnsserver.utils.ResultValue;
import edu.umass.cs.gnsserver.utils.ValuesMap;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;


public enum UpdateOperation {
  //  Args:         singleField, skipread, upsert

  USER_JSON_REPLACE(false, true, false), // doesn't require a read

  USER_JSON_REPLACE_OR_CREATE(false, true, true, USER_JSON_REPLACE), // doesn't require a read

  CREATE_INDEX(false, true, false), // doesn't require a read
  //
  // NOTE: The following all user the "older" JSONArray format. 
  //

  SINGLE_FIELD_CREATE(true, false, false),

  SINGLE_FIELD_REMOVE_FIELD(true, true, false), // doesn't require a read


  SINGLE_FIELD_CLEAR(true, false, false),

  SINGLE_FIELD_REPLACE_ALL(true, true, false), // doesn't require a read


  SINGLE_FIELD_REMOVE(true, false, false),

  SINGLE_FIELD_REPLACE_SINGLETON(true, false, false),

  SINGLE_FIELD_APPEND(true, false, false),

  SINGLE_FIELD_APPEND_OR_CREATE(true, false, true, SINGLE_FIELD_APPEND),

  SINGLE_FIELD_REPLACE_ALL_OR_CREATE(true, false, true, SINGLE_FIELD_REPLACE_ALL),

  SINGLE_FIELD_APPEND_WITH_DUPLICATION(true, false, false),

  SINGLE_FIELD_SUBSTITUTE(true, false, false),

  SINGLE_FIELD_SET(true, false, false),

  SINGLE_FIELD_GET(true, false, false),

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


  public boolean isSingleFieldOperation() {
    return singleFieldOperation;
  }


  public boolean isAbleToSkipRead() {
    return ableToSkipRead;
  }


  public boolean isUpsert() {
    return upsert;
  }


  public UpdateOperation getNonUpsertEquivalent() {
    return nonUpsertEquivalent;
  }


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
    return !valuesList.isEmpty() && valuesList.get(0).equals(GNSProtocol.NULL_RESPONSE.toString());
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
        GNSConfig.getLogger().fine("Remove " + newValues + "\tValues list = " + valuesList);
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
        valuesList.add(GNSProtocol.NULL_RESPONSE.toString());
        return true;
      default:
        return false;
    }
  }
}
