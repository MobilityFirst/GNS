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
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport;

import edu.umass.cs.gnscommon.GnsProtocol;
import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnsserver.database.ColumnFieldType;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsApp.NSResponseCode;
import edu.umass.cs.gnsserver.gnsApp.clientSupport.NSFieldAccess;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Contains static fields and methods that implement activecode.
 *
 */
public class ActiveCode {

  /**
   * Active code fields
   */
  /**
   * ON_READ - the string key for the field that stores the read information.
   */
  public static final String ON_READ = InternalField.makeInternalFieldString("on_read");
  /**
   * ON_WRITE - the string key for the field that stores the write information.
   */
  public static final String ON_WRITE = InternalField.makeInternalFieldString("on_write");

  /**
   * Returns the internal field corresponding to the given action.
   *
   * @param action
   * @return a string
   */
  public static String codeField(String action) {
    if (action.equals("read")) {
      return ON_READ;
    } else if (action.equals("write")) {
      return ON_WRITE;
    } else {
      return null;
    }
  }

  /**
   * Sets active code for the guid and action.
   *
   * @param guid
   * @param action
   * @param code
   * @param writer
   * @param signature
   * @param message
   * @param handler
   * @return a {@link NSResponseCode}
   */
  public static NSResponseCode setCode(String guid, String action, String code, String writer,
          String signature, String message,
          ClientRequestHandlerInterface handler) {
    JSONObject json;
    try {
      json = new JSONObject();
      json.put(codeField(action), code);
    } catch (JSONException e) {
      return NSResponseCode.ERROR;
    }
    NSResponseCode response = FieldAccess.updateUserJSON(guid, json,
            writer, signature, message, handler);
//    NSResponseCode response = handler.getIntercessor().sendUpdateUserJSON(guid,
//            new ValuesMap(json), UpdateOperation.USER_JSON_REPLACE,
//            writer, signature, message);
    return response;
  }

  /**
   * Clears the active code for the guid and action.
   *
   * @param guid
   * @param action
   * @param writer
   * @param signature
   * @param message
   * @param handler
   * @return a {@link NSResponseCode}
   */
  public static NSResponseCode clearCode(String guid, String action, String writer, String signature, String message,
          ClientRequestHandlerInterface handler) {
    String field = codeField(action);

    NSResponseCode response = FieldAccess.update(guid, field, "", null, -1,
            UpdateOperation.SINGLE_FIELD_REMOVE, writer, signature, message, handler);
//    NSResponseCode response = handler.getIntercessor().sendUpdateRecord(guid, field, "", null, 0,
//            UpdateOperation.SINGLE_FIELD_REMOVE_FIELD, writer, signature, message);
    return response;
  }

  /**
   * Gets the currently set active code for the guid and action.
   *
   * @param guid
   * @param action
   * @param reader
   * @param signature
   * @param message
   * @param handler
   * @return a string
   */
  public static String getCode(String guid, String action, String reader, String signature, String message,
          ClientRequestHandlerInterface handler) {
    String field = codeField(action);
    NSResponseCode errorCode = FieldAccess.signatureAndACLCheckForRead(guid, field,
            reader, signature, message, handler.getApp());
    if (errorCode.isAnError()) {
      return GnsProtocol.NULL_RESPONSE;
    }
    try {
      ValuesMap result = NSFieldAccess.lookupFieldLocalNoAuth(guid, field,
              ColumnFieldType.USER_JSON,
              handler.getApp().getDB());
      return result.getString(field);
    } catch (FailedDBOperationException | JSONException e) {
      return GnsProtocol.NULL_RESPONSE;
    }
//    QueryResult<String> guidResult = handler.getIntercessor().sendSingleFieldQuery(guid, field, reader,
//            signature, message, ColumnFieldType.USER_JSON);
//    try {
//      if (!guidResult.isError()) {
//        return guidResult.getValuesMap().getString(field);
//      }
//    } catch (JSONException e) {
//    }
//    return GnsProtocol.NULL_RESPONSE;
  }
}
