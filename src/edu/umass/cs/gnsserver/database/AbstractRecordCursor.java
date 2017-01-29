
package edu.umass.cs.gnsserver.database;

import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import org.json.JSONObject;


public abstract class AbstractRecordCursor implements RecordCursorInterface {
  
  @Override
  public JSONObject nextJSONObject() throws FailedDBOperationException {
    throw new UnsupportedOperationException("Not supported yet.");
  }
  
}
