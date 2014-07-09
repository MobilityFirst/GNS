/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.database;

import edu.umass.cs.gns.exceptions.FailedDBOperationException;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Iterator;
/**
 * A cursor that can be used to iterate through a collection of GNS served records as JSONObjects.
 * 
 * @author westy
 */
public interface RecordCursorInterface {
 
  public JSONObject nextJSONObject() throws FailedDBOperationException;
  
  public HashMap<ColumnField, Object> nextHashMap() throws FailedDBOperationException;

  public boolean hasNext() throws FailedDBOperationException;
  
}
