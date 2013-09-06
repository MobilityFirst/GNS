package edu.umass.cs.gns.client;

//import edu.umass.cs.gns.packet.QueryResultValue;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nameserver.ValuesMap;
import edu.umass.cs.gns.packet.UpdateOperation;
import org.json.JSONArray;

import java.security.acl.AclEntry;
import java.util.ArrayList;
import org.json.JSONException;

/**
 * A frontend to the the table which stores the fields and values. Provides conversion between the database to java objects
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
    ArrayList<String> result = client.sendQuery(guid, key);
    if (result != null) {
      return new JSONArray(new ArrayList<String>(result)).toString();
    } else {
      return new String();
    }
  }

  public String lookupMultipleValues(String guid, String key) {
    Intercessor client = Intercessor.getInstance();
    ValuesMap result = client.sendMultipleReturnValueQuery(guid, key);
    try {
      if (result != null) {
        return result.toJSONObject().toString();
      }
    } catch (JSONException e) {
       GNS.getLogger().severe("Problem parsing multiple value return:" + e);
    }
    return new String();
  }

  public String lookupOne(String guid, String key) {
    Intercessor client = Intercessor.getInstance();
    ArrayList<String> result = client.sendQuery(guid, key);
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
