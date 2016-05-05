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
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport;

import edu.umass.cs.gnsserver.gnsapp.NSResponseCode;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientSupport.NSFieldAccess;
import edu.umass.cs.gnsserver.utils.ResultValue;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

/**
 * Implements metadata on fields.
 *
 * @author westy
 */
public class FieldMetaData {

  /**
   * Creates a key for looking up metadata in a guid record.
   *
   * @param metaDataType
   * @param key
   * @return a string
   */
  public static String makeFieldMetaDataKey(MetaDataTypeName metaDataType, String key) {
    return metaDataType.getFieldPath() + "." + key + ".MD";
  }

  /**
   * Adds a value to the metadata of the field in the guid.
   *
   * @param type
   * @param guid
   * @param key
   * @param value
   * @param writer
   * @param signature
   * @param message
   * @param timestamp
   * @param handler
   * @return a {@link NSResponseCode}
   */
  public static NSResponseCode add(MetaDataTypeName type, String guid, 
          String key, String value, String writer, String signature,
          String message, Date timestamp, ClientRequestHandlerInterface handler) {
    return FieldAccess.update(guid, makeFieldMetaDataKey(type, key), value, null, -1,
            UpdateOperation.SINGLE_FIELD_APPEND_OR_CREATE, writer, signature, message, 
            timestamp, handler);
    
//    return handler.getIntercessor().sendUpdateRecord(guid, 
//            makeFieldMetaDataKey(type, key), value, null, -1,
//            UpdateOperation.SINGLE_FIELD_APPEND_OR_CREATE,
//            writer, signature, message);
  }

  /**
   * Grabs the metadata indexed by type from the field from the guid.
   *
   * @param type
   * @param guid
   * @param key
   * @param reader
   * @param signature
   * @param message
   * @param timestamp
   * @param handler
   * @return a set of strings
   */
  public static Set<String> lookup(MetaDataTypeName type, String guid, String key, 
          String reader, String signature, 
          String message, Date timestamp,
          ClientRequestHandlerInterface handler) {
    String field = makeFieldMetaDataKey(type, key);
    NSResponseCode errorCode = FieldAccess.signatureAndACLCheckForRead(guid, field, null, 
            reader, signature, message, timestamp, handler.getApp());
    if (errorCode.isAnError()) {
      return new HashSet<>();
    }
    ResultValue result = NSFieldAccess.lookupListFieldLocallyNoAuth(guid, field,
            handler.getApp().getDB());
    return new HashSet<>(result.toStringSet());
//    QueryResult<String> result = handler.getIntercessor().sendSingleFieldQuery(guid, makeFieldMetaDataKey(type, key), reader, signature, message,
//            ColumnFieldType.LIST_STRING);
//    if (!result.isError()) {
//      return new HashSet<>(result.getArray(makeFieldMetaDataKey(type, key)).toStringSet());
//    } else {
//      return new HashSet<>();
//    }
  }

  /**
   *
   * @param type
   * @param guid
   * @param key
   * @param value
   * @param timestamp
   * @param handler
   */
  public static void add(MetaDataTypeName type, String guid, 
          String key, String value, Date timestamp, ClientRequestHandlerInterface handler) {
    FieldAccess.update(guid, makeFieldMetaDataKey(type, key), value, null, -1,
            UpdateOperation.SINGLE_FIELD_APPEND_OR_CREATE, null, null, null, timestamp, handler);

//    handler.getIntercessor().sendUpdateRecordBypassingAuthentication(guid,
//            makeFieldMetaDataKey(type, key), value, null,
//            UpdateOperation.SINGLE_FIELD_APPEND_OR_CREATE);
  }

  /**
   *
   * @param type
   * @param guid
   * @param key
   * @param value
   * @param writer
   * @param signature
   * @param message
   * @param timestamp
   * @param handler
   * @return a {@link NSResponseCode}
   */
  public static NSResponseCode remove(MetaDataTypeName type, String guid, String key, String value, String writer, String signature,
          String message, Date timestamp, ClientRequestHandlerInterface handler) {
     return FieldAccess.update(guid, makeFieldMetaDataKey(type, key), value, null, -1,
            UpdateOperation.SINGLE_FIELD_REMOVE, writer, signature, message, timestamp, handler);
     
//    return handler.getIntercessor().sendUpdateRecord(guid, makeFieldMetaDataKey(type, key), value, null, -1,
//            UpdateOperation.SINGLE_FIELD_REMOVE, writer, signature, message);
  }

}
