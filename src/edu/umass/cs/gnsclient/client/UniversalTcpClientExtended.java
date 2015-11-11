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
 *  Initial developer(s): Westy, Emmanuel Cecchet
 *
 */
package edu.umass.cs.gnsclient.client;

import edu.umass.cs.gnscommon.GnsProtocol;
import java.io.IOException;
import org.json.JSONArray;
import edu.umass.cs.gnsclient.exceptions.GnsException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import org.json.JSONObject;

/**
 * This class defines an extension to {@link UniversalTcpClient} to communicate with a GNS instance
 * over HTTP. This extension contains more methods with more convenient to use invocation signatures.
 * It also has admin commands.
 *
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class UniversalTcpClientExtended extends UniversalTcpClient {

  /**
   * Creates a new <code>UniversalTcpClientExtended</code> object
   *
   * @param remoteHost Host name of the remote GNS server.
   * @param remotePort Port number of the remote GNS server.
   */
  public UniversalTcpClientExtended(String remoteHost, int remotePort) {
    this(remoteHost, remotePort, false);
  }
  
  /**
   * Creates a new <code>UniversalTcpClientExtended</code> object.
   * Optionally disables SSL if disableSSL is true.
   *
   * @param remoteHost Host name of the remote GNS server.
   * @param remotePort Port number of the remote GNS server.
   * @param disableSSL
   */
  public UniversalTcpClientExtended(String remoteHost, int remotePort, boolean disableSSL) {
    super(remoteHost, remotePort, disableSSL);
  }
  
  /**
   * Creates a new field in the target guid with value being the list.
   *
   * @param target
   * @param field
   * @param value
   * @throws IOException
   * @throws GnsException
   */
  public void fieldCreateList(GuidEntry target, String field, JSONArray value) throws IOException,
          GnsException {
    fieldCreateList(target.getGuid(), field, value, target);
  }

  /**
   * Creates a new one element field with single element value being the string. Allows a a different guid as
   * the writer. If the writer is different use addToACL first to allow other
   * the guid to write this field.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws IOException
   * @throws GnsException
   */
  public void fieldCreateOneElementList(String targetGuid, String field, String value, GuidEntry writer) throws IOException,
          GnsException {
    JSONObject command = createAndSignCommand(writer.getPrivateKey(), GnsProtocol.CREATE, GnsProtocol.GUID, targetGuid,
            GnsProtocol.FIELD, field, GnsProtocol.VALUE, value, GnsProtocol.WRITER, writer.getGuid());
    String response = sendCommand(command);

    checkResponse(command, response);
  }

  /**
   * Creates a new one element field in the target guid with single element value being the string.
   *
   * @param target
   * @param field
   * @param value
   * @throws IOException
   * @throws GnsException
   */
  public void fieldCreateOneElementList(GuidEntry target, String field, String value) throws IOException,
          GnsException {
    UniversalTcpClientExtended.this.fieldCreateOneElementList(target.getGuid(), field, value, target);
  }

  /**
   * Appends the single value of the field onto list of values or creates a new field
   * with a single value list if it does not exist.
   * If the writer is different use addToACL first to allow other
   * the guid to write this field.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws IOException
   * @throws GnsException
   */
  public void fieldAppendOrCreate(String targetGuid, String field, String value, GuidEntry writer)
          throws IOException, GnsException {
    JSONObject command = createAndSignCommand(writer.getPrivateKey(), GnsProtocol.APPEND_OR_CREATE, GnsProtocol.GUID, targetGuid,
            GnsProtocol.FIELD, field, GnsProtocol.VALUE, value, GnsProtocol.WRITER, writer.getGuid());
    String response = sendCommand(command);
    checkResponse(command, response);
  }

  /**
   * Replaces the values of the field in targetGuid with the single value or creates a new
   * field with a single value list if it does not exist.
   * If the writer is different use addToACL first to allow other
   * the guid to write this field.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws IOException
   * @throws GnsException
   */
  public void fieldReplaceOrCreate(String targetGuid, String field, String value, GuidEntry writer)
          throws IOException, GnsException {
    JSONObject command = createAndSignCommand(writer.getPrivateKey(), GnsProtocol.REPLACE_OR_CREATE, GnsProtocol.GUID, targetGuid,
            GnsProtocol.FIELD, field, GnsProtocol.VALUE, value, GnsProtocol.WRITER, writer.getGuid());
    String response = sendCommand(command);
    checkResponse(command, response);
  }
  
  /**
   * Replaces the values of the field with the list of values or creates a new
   * field with values in the list if it does not exist.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @throws IOException
   * @throws GnsException
   */
  public void fieldReplaceOrCreateList(GuidEntry targetGuid, String field, JSONArray value)
          throws IOException, GnsException {
    JSONObject command = createAndSignCommand(targetGuid.getPrivateKey(), GnsProtocol.REPLACE_OR_CREATE_LIST, 
            GnsProtocol.GUID, targetGuid.getGuid(),
            GnsProtocol.FIELD, field, GnsProtocol.VALUE, value.toString(), GnsProtocol.WRITER, targetGuid.getGuid());
    String response = sendCommand(command);
    checkResponse(command, response);
  }

  /**
   * Replaces the values of the field in the target guid with the single value or creates a new
   * field with a single value list if it does not exist.
   *
   * @param target
   * @param field
   * @param value
   * @throws IOException
   * @throws GnsException
   */
  public void fieldReplaceOrCreateList(GuidEntry target, String field, String value)
          throws IOException, GnsException {
    fieldReplaceOrCreate(target.getGuid(), field, value, target);
  }

  /**
   * Replaces all the values of field with the single value.
   * If the writer is different use addToACL first to allow other
   * the guid to write this field.
   *
   * @param targetGuid GUID where the field is stored
   * @param field field name
   * @param value the new value
   * @param writer GUID entry of the writer
   * @throws IOException
   * @throws GnsException
   */
  public void fieldReplace(String targetGuid, String field, String value, GuidEntry writer) throws IOException,
          GnsException {
    JSONObject command = createAndSignCommand(writer.getPrivateKey(), GnsProtocol.REPLACE, GnsProtocol.GUID, targetGuid,
            GnsProtocol.FIELD, field, GnsProtocol.VALUE, value, GnsProtocol.WRITER, writer.getGuid());
    String response = sendCommand(command);

    checkResponse(command, response);
  }

  /**
   * Replaces all the values of field in target with with the single value.
   *
   * @param target
   * @param field
   * @param value
   * @throws IOException
   * @throws GnsException
   */
  public void fieldReplace(GuidEntry target, String field, String value) throws IOException,
          GnsException {
    fieldReplace(target.getGuid(), field, value, target);
  }

  /**
   * Replaces all the values of field in target with the list of values.
   *
   * @param target
   * @param field
   * @param value
   * @throws IOException
   * @throws GnsException
   */
  public void fieldReplace(GuidEntry target, String field, JSONArray value) throws IOException,
          GnsException {
    fieldReplaceList(target.getGuid(), field, value, target);
  }

  /**
   * Appends a single value onto a field.
   * If the writer is different use addToACL first to allow other
   * the guid to write this field.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws IOException
   * @throws GnsException
   */
  public void fieldAppend(String targetGuid, String field, String value, GuidEntry writer) throws IOException,
          GnsException {
    JSONObject command = createAndSignCommand(writer.getPrivateKey(), GnsProtocol.APPEND_WITH_DUPLICATION, GnsProtocol.GUID, targetGuid,
            GnsProtocol.FIELD, field, GnsProtocol.VALUE, value, GnsProtocol.WRITER, writer.getGuid());
    String response = sendCommand(command);

    checkResponse(command, response);
  }

  /**
   * Appends a single value onto a field in the target guid.
   *
   * @param target
   * @param field
   * @param value
   * @throws IOException
   * @throws GnsException
   */
  public void fieldAppend(GuidEntry target, String field, String value) throws IOException,
          GnsException {
    fieldAppend(target.getGuid(), field, value, target);
  }

  /**
   * Appends a list of values onto a field but converts the list to set removing duplicates.
   * If the writer is different use addToACL first to allow other
   * the guid to write this field.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws IOException
   * @throws GnsException
   */
  public void fieldAppendWithSetSemantics(String targetGuid, String field, JSONArray value, GuidEntry writer) throws IOException,
          GnsException {
    JSONObject command = createAndSignCommand(writer.getPrivateKey(), GnsProtocol.APPEND_LIST, GnsProtocol.GUID, targetGuid,
            GnsProtocol.FIELD, field, GnsProtocol.VALUE, value.toString(), GnsProtocol.WRITER, writer.getGuid());
    String response = sendCommand(command);

    checkResponse(command, response);
  }

  /**
   * Appends a list of values onto a field in target but converts the list to set removing duplicates.
   *
   * @param target
   * @param field
   * @param value
   * @throws IOException
   * @throws GnsException
   */
  public void fieldAppendWithSetSemantics(GuidEntry target, String field, JSONArray value) throws IOException,
          GnsException {
    fieldAppendWithSetSemantics(target.getGuid(), field, value, target);
  }

  /**
   * Appends a single value onto a field but converts the list to set removing duplicates.
   * If the writer is different use addToACL first to allow other
   * the guid to write this field.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws IOException
   * @throws GnsException
   */
  public void fieldAppendWithSetSemantics(String targetGuid, String field, String value, GuidEntry writer) throws IOException,
          GnsException {
    JSONObject command = createAndSignCommand(writer.getPrivateKey(), GnsProtocol.APPEND, GnsProtocol.GUID, targetGuid,
            GnsProtocol.FIELD, field, GnsProtocol.VALUE, value.toString(), GnsProtocol.WRITER, writer.getGuid());
    String response = sendCommand(command);

    checkResponse(command, response);
  }

  /**
   * Appends a single value onto a field in target but converts the list to set removing duplicates.
   *
   * @param target
   * @param field
   * @param value
   * @throws IOException
   * @throws GnsException
   */
  public void fieldAppendWithSetSemantics(GuidEntry target, String field, String value) throws IOException,
          GnsException {
    fieldAppendWithSetSemantics(target.getGuid(), field, value, target);
  }

  /**
   * Replaces all the first element of field with the value.
   * If the writer is different use addToACL first to allow other
   * the guid to write this field. If writer is null the command
   * is sent unsigned.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @param writer
   * @throws IOException
   * @throws GnsException
   */
  public void fieldReplaceFirstElement(String targetGuid, String field, String value, GuidEntry writer) throws IOException,
          GnsException {
    JSONObject command;
    if (writer == null) {
      command = createCommand(GnsProtocol.REPLACE, GnsProtocol.GUID, targetGuid,
              GnsProtocol.FIELD, field, GnsProtocol.VALUE, value);
    } else {
      command = createAndSignCommand(writer.getPrivateKey(), GnsProtocol.REPLACE, GnsProtocol.GUID, targetGuid,
              GnsProtocol.FIELD, field, GnsProtocol.VALUE, value, GnsProtocol.WRITER, writer.getGuid());
    }
    String response = sendCommand(command);

    checkResponse(command, response);
  }

  /**
   * Replaces the first element of field in target with the value.
   *
   * @param target
   * @param field
   * @param value
   * @throws IOException
   * @throws GnsException
   */
  public void fieldReplaceFirstElement(GuidEntry target, String field, String value) throws IOException,
          GnsException {
    fieldReplaceFirstElement(target.getGuid(), field, value, target);
  }

  /**
   * Substitutes the value for oldValue in the list of values of a field.
   * If the writer is different use addToACL first to allow other
   * the guid to write this field.
   *
   * @param targetGuid GUID where the field is stored
   * @param field field name
   * @param newValue
   * @param oldValue
   * @param writer GUID entry of the writer
   * @throws IOException
   * @throws GnsException
   */
  public void fieldSubstitute(String targetGuid, String field, String newValue,
          String oldValue, GuidEntry writer) throws IOException, GnsException {
    JSONObject command = createAndSignCommand(writer.getPrivateKey(), GnsProtocol.SUBSTITUTE, GnsProtocol.GUID,
            targetGuid, GnsProtocol.FIELD, field, GnsProtocol.VALUE, newValue,
            GnsProtocol.OLD_VALUE, oldValue, GnsProtocol.WRITER, writer.getGuid());
    String response = sendCommand(command);

    checkResponse(command, response);
  }

  /**
   * Substitutes the value for oldValue in the list of values of a field in the target.
   *
   * @param target
   * @param field
   * @param newValue
   * @param oldValue
   * @throws IOException
   * @throws GnsException
   */
  public void fieldSubstitute(GuidEntry target, String field, String newValue,
          String oldValue) throws IOException, GnsException {
    fieldSubstitute(target.getGuid(), field, newValue, oldValue, target);
  }

  /**
   * Pairwise substitutes all the values for the oldValues in the list of values of a field.
   * If the writer is different use addToACL first to allow other
   * the guid to write this field.
   *
   *
   * @param targetGuid GUID where the field is stored
   * @param field
   * @param newValue list of new values
   * @param oldValue list of old values
   * @param writer GUID entry of the writer
   * @throws IOException
   * @throws GnsException
   */
  public void fieldSubstitute(String targetGuid, String field,
          JSONArray newValue, JSONArray oldValue, GuidEntry writer) throws IOException, GnsException {
    JSONObject command = createAndSignCommand(writer.getPrivateKey(), GnsProtocol.SUBSTITUTE_LIST, GnsProtocol.GUID,
            targetGuid, GnsProtocol.FIELD, field, GnsProtocol.VALUE, newValue.toString(),
            GnsProtocol.OLD_VALUE, oldValue.toString(), GnsProtocol.WRITER, writer.getGuid());
    String response = sendCommand(command);

    checkResponse(command, response);
  }

  /**
   * Pairwise substitutes all the values for the oldValues in the list of values of a field in the target.
   *
   * @param target
   * @param field
   * @param newValue
   * @param oldValue
   * @throws IOException
   * @throws GnsException
   */
  public void fieldSubstitute(GuidEntry target, String field,
          JSONArray newValue, JSONArray oldValue) throws IOException, GnsException {
    fieldSubstitute(target.getGuid(), field, newValue, oldValue, target);
  }

  /**
   * Reads the first value for a key from the GNS server for the given
   * guid. The guid of the user attempting access is also needed. Signs the
   * query using the private key of the user associated with the reader guid
   * (unsigned if reader is null).
   *
   * @param guid
   * @param field
   * @param reader
   * @return
   * @throws Exception
   */
  public String fieldReadArrayFirstElement(String guid, String field, GuidEntry reader) throws Exception {
    JSONObject command;
    if (reader == null) {
      command = createCommand(GnsProtocol.READ_ARRAY_ONE, GnsProtocol.GUID, guid, GnsProtocol.FIELD, field);
    } else {
      command = createAndSignCommand(reader.getPrivateKey(), GnsProtocol.READ_ARRAY_ONE, GnsProtocol.GUID, guid, GnsProtocol.FIELD, field,
              GnsProtocol.READER, reader.getGuid());
    }

    String response = sendCommand(command);

    return checkResponse(command, response);
  }

  /**
   * Reads the first value for a key in the guid.
   *
   * @param guid
   * @param field
   * @return
   * @throws Exception
   */
  public String fieldReadArrayFirstElement(GuidEntry guid, String field) throws Exception {
    return fieldReadArrayFirstElement(guid.getGuid(), field, guid);
  }
  
  /**
   * Removes a field in the JSONObject record of the given guid.
   * Signs the query using the private key of the guid.
   * A convenience method™.
   *
   * @param guid
   * @param field
   * @throws IOException
   * @throws InvalidKeyException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   * @throws GnsException
   */
  public void fieldRemove(GuidEntry guid, String field) throws IOException, InvalidKeyException,
          NoSuchAlgorithmException, SignatureException, GnsException {
    fieldRemove(guid.getGuid(), field, guid);
  }
  
  public String adminEnable(String passkey) throws Exception {
    JSONObject command = createCommand(GnsProtocol.ADMIN, GnsProtocol.PASSKEY, passkey);

    String response = sendCommand(command);

    return checkResponse(command, response);
  }
  
  public void parameterSet(String name, Object value) throws Exception {
    JSONObject command = createCommand(GnsProtocol.SET_PARAMETER, GnsProtocol.NAME, name, GnsProtocol.VALUE, value);

    String response = sendCommand(command);

    checkResponse(command, response);
  }
  
  public String parameterGet(String name) throws Exception {
    JSONObject command = createCommand(GnsProtocol.GET_PARAMETER, GnsProtocol.NAME, name);

    String response = sendCommand(command);

    return checkResponse(command, response);
  }

}
