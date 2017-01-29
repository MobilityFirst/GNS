
package edu.umass.cs.gnsserver.gnsapp.clientSupport;

import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnscommon.exceptions.server.FieldNotFoundException;
import edu.umass.cs.gnscommon.exceptions.server.RecordNotFoundException;
import edu.umass.cs.gnsserver.database.ColumnFieldType;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.FieldMetaData;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.GuidInfo;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.MetaDataTypeName;
import edu.umass.cs.gnsserver.gnsapp.recordmap.BasicRecordMap;
import edu.umass.cs.gnsserver.gnsapp.recordmap.NameRecord;
import edu.umass.cs.gnsserver.utils.ResultValue;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;


public class NSFieldMetaData {


  public static Set<Object> lookupLocally(MetaDataTypeName type, GuidInfo guidInfo, String key,
          BasicRecordMap database) 
          throws FailedDBOperationException, FieldNotFoundException, RecordNotFoundException {
    return lookupLocally(type, guidInfo.getGuid(), key, database);
  }


  public static Set<Object> lookupLocally(MetaDataTypeName type, String guid, String key,
          BasicRecordMap database) 
          throws FailedDBOperationException, FieldNotFoundException, RecordNotFoundException {
    ResultValue result = NSFieldAccess.lookupListFieldLocallyNoAuth(guid, FieldMetaData.makeFieldMetaDataKey(type, key), database);
    if (result != null) {
      ClientSupportConfig.getLogger().log(Level.FINE, "lookupOnThisNameServer returning {0}", result);
      return new HashSet<Object>(result);
    } else {
      return new HashSet<Object>();
    }
  }


  public static boolean fieldExists(MetaDataTypeName type, String guid, String key, BasicRecordMap database)
          throws RecordNotFoundException, FieldNotFoundException, FailedDBOperationException {
    String field = FieldMetaData.makeFieldMetaDataKey(type, key);
    NameRecord nameRecord = NameRecord.getNameRecordMultiUserFields(database, guid,
            ColumnFieldType.LIST_STRING, field);
    return nameRecord.containsUserKey(field);
  }
}
