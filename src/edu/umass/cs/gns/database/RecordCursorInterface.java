/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.database;

import edu.umass.cs.gns.exceptions.FailedDBOperationException;
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
   * @throws FailedDBOperationException 
   */
  public JSONObject nextJSONObject() throws FailedDBOperationException;
  
  /**
   * Returns the next row as a HashMap.
   * 
   * @return the next row as a HashMap
   * @throws FailedDBOperationException 
   */
  public HashMap<ColumnField, Object> nextHashMap() throws FailedDBOperationException;

  /**
   * Returns true if the collection has more records. 
   * @return
   * @throws FailedDBOperationException 
   */
  public boolean hasNext() throws FailedDBOperationException;
  
}
