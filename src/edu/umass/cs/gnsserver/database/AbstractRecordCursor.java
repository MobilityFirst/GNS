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
package edu.umass.cs.gnsserver.database;

import edu.umass.cs.gnsserver.exceptions.FailedDBOperationException;
import org.json.JSONObject;

import java.util.HashMap;


/**
 * A cursor that can be used to iterate through a collection of GNS served records as JSONObjects.
 * 
 * @author westy
 */
public abstract class AbstractRecordCursor implements RecordCursorInterface {
  
  @Override
  public JSONObject nextJSONObject() throws FailedDBOperationException {
    throw new UnsupportedOperationException("Not supported yet.");
  }
  
  @Override
  public HashMap<ColumnField, Object> nextHashMap() throws FailedDBOperationException {
    throw new UnsupportedOperationException("Not supported yet.");
  }
  
}
