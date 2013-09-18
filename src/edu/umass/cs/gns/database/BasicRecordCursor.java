/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.database;

import java.util.HashMap;
import org.json.JSONObject;

/**
 * A cursor that can be used to iterate through a collection of GNS served records as JSONObjects.
 * 
 * @author westy
 */
public abstract class BasicRecordCursor implements RecordCursorInterface {
  
  @Override
  public JSONObject nextJSONObject() {
    throw new UnsupportedOperationException("Not supported yet.");
  }
  
  @Override
  public HashMap<Field, Object> nextHashMap() {
    throw new UnsupportedOperationException("Not supported yet.");
  }
  
}
