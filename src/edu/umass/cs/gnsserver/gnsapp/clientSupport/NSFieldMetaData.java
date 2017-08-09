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
 *  Initial developer(s): Abhigyan Sharma, Westy
 *
 */
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

/**
 * Implements metadata on fields.
 *
 * @author westy
 */
public class NSFieldMetaData {

  /**
   * Grabs the metadata indexed by type from the field from the guid.
   *
   * @param type
   * @param guidInfo
   * @param key
   * @param database
   * @return a set of objects
 * @throws FailedDBOperationException 
 * @throws FieldNotFoundException 
 * @throws RecordNotFoundException 
   */
  public static Set<Object> lookupLocally(MetaDataTypeName type, GuidInfo guidInfo, String key,
          BasicRecordMap database) 
          throws FailedDBOperationException, FieldNotFoundException, RecordNotFoundException {
    return lookupLocally(type, guidInfo.getGuid(), key, database);
  }

  /**
   * Grabs the metadata indexed by type from the field from the guid.
   *
   * @param type
   * @param guid
   * @param key
   * @param database
   * @return a set of objects
 * @throws FailedDBOperationException 
 * @throws FieldNotFoundException 
 * @throws RecordNotFoundException 
   */
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

  /**
   * Returns true if the ACL field exists.
   * 
   * @param type
   * @param guid
   * @param key
   * @param database
   * @return true if the ACL field exists
   * @throws RecordNotFoundException
   * @throws FieldNotFoundException
   * @throws FailedDBOperationException 
   */
  public static boolean fieldExists(MetaDataTypeName type, String guid, String key, BasicRecordMap database)
          throws RecordNotFoundException, FieldNotFoundException, FailedDBOperationException {
    String field = FieldMetaData.makeFieldMetaDataKey(type, key);
    NameRecord nameRecord = NameRecord.getNameRecordMultiUserFields(database, guid,
            ColumnFieldType.LIST_STRING, field);
    return nameRecord.containsUserKey(field);
  }
}
