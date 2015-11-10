/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gnsserver.database;

import edu.umass.cs.gnsserver.exceptions.FailedDBOperationException;
import org.json.JSONObject;

import java.util.HashMap;
/**
 * A cursor that can be used to iterate through a collection of GNS served records as 
 * JSONObjects or HashMaps.
 * 
 * @author westy
 */
public interface RecordCursorInterface {
 
  /**
   * Returns the next row as a JSONObject.
   * 
   * @return the next row as a JSONObject
   * @throws edu.umass.cs.gnsserver.exceptions.FailedDBOperationException 
   */
  public JSONObject nextJSONObject() throws FailedDBOperationException;
  
  /**
   * Returns the next row as a HashMap.
   * 
   * @return the next row as a HashMap
   * @throws edu.umass.cs.gnsserver.exceptions.FailedDBOperationException
   */
  public HashMap<ColumnField, Object> nextHashMap() throws FailedDBOperationException;

  /**
   * Returns true if the collection has more records. 
   * @return true if the collection has more records
   * @throws edu.umass.cs.gnsserver.exceptions.FailedDBOperationException
   */
  public boolean hasNext() throws FailedDBOperationException;
  
}
