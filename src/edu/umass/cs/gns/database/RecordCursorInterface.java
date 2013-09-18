/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.database;

import java.util.HashMap;
import java.util.Iterator;
import org.json.JSONObject;

/**
 * A cursor that can be used to iterate through a collection of GNS served records as JSONObjects.
 * 
 * @author westy
 */
public interface RecordCursorInterface extends Iterator<JSONObject> {
 
  public JSONObject nextJSONObject();
  
  public HashMap<Field, Object> nextHashMap();
  
}
