package edu.umass.cs.gnrs.client;

import edu.umass.cs.gnrs.packet.QueryResultValue;
import edu.umass.cs.gnrs.packet.UpdateOperation;
import edu.umass.cs.gnrs.util.JSONUtils;
import java.security.acl.AclEntry;
import java.util.ArrayList;
import java.util.Arrays;
import org.json.JSONArray;
import org.json.JSONException;

/**
 * A frontend to the the table which stores the fields and values. Provides conversion between the database to java
 * objects
 *
 * See {@link AclEntry)
 *
 * @author westy
 */
public class RecordAccess {

  public static String Version = "$Revision: 645 $";

  // make it a singleton class
  public static RecordAccess getInstance() {
    return RecordAccessHolder.INSTANCE;
  }

  private static class RecordAccessHolder {

    private static final RecordAccess INSTANCE = new RecordAccess();
  }

  public String lookup(String guid, String key) {
    Intercessor client = Intercessor.getInstance();
    QueryResultValue result = client.sendQuery(guid, key);
    if (result != null) {
      return new JSONArray(new ArrayList<String>(result)).toString();
    } else {
      return new String();
    }
  }
  
  public String lookupOne(String guid, String key) {
    Intercessor client = Intercessor.getInstance();
    QueryResultValue result = client.sendQuery(guid, key);
    if (result != null && !result.isEmpty()) {
      return result.get(0);
    } else {
      return new String();
    }
  }

  public boolean update(String guid, String key, ArrayList<String> value, ArrayList<String> oldValue, UpdateOperation operation) {
    Intercessor client = Intercessor.getInstance();
    return client.sendUpdateRecordWithConfirmation(guid, key, value, oldValue, operation);
  }

  public boolean create(String guid, String key, ArrayList<String> value) {
    Intercessor client = Intercessor.getInstance();
    return client.sendUpdateRecordWithConfirmation(guid, key, value, null, UpdateOperation.CREATE);
  }
 
}
