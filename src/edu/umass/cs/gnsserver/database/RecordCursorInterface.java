
package edu.umass.cs.gnsserver.database;

import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import org.json.JSONObject;


public interface RecordCursorInterface {
 

  public JSONObject nextJSONObject() throws FailedDBOperationException;
  

  public boolean hasNext() throws FailedDBOperationException;
  
}
