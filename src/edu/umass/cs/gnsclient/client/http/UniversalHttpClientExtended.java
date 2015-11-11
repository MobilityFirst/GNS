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
package edu.umass.cs.gnsclient.client.http;

import edu.umass.cs.gnscommon.GnsProtocol;
import edu.umass.cs.gnsclient.client.GuidEntry;
import java.io.IOException;
import org.json.JSONArray;
import edu.umass.cs.gnsclient.exceptions.GnsException;

/**
 * This class defines an extension to AbstractGnrsClient to communicate with a GNS instance
 * over HTTP. This extension contains more methods.
 *
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class UniversalHttpClientExtended extends UniversalHttpClient {

  /**
   * Creates a new <code>AbstractGnrsClient</code> object
   *
   * @param host Hostname of the GNS instance
   * @param port Port number of the GNS instance
   */
  public UniversalHttpClientExtended(String host, int port) {
    super(host, port);
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
  public void fieldCreateSingleElementArray(String targetGuid, String field, String value, GuidEntry writer) throws IOException,
          GnsException {
    String command = createAndSignQuery(writer, GnsProtocol.CREATE, GnsProtocol.GUID, targetGuid,
            GnsProtocol.FIELD, field, GnsProtocol.VALUE, value, GnsProtocol.WRITER, writer.getGuid());
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }
  
  /**
   * Creates a new one element field in the given guid with single element value being the string.
   *
   * @param targetGuid
   * @param field
   * @param value
   * @throws IOException
   * @throws GnsException
   */
  public void fieldCreateSingleElementArray(GuidEntry targetGuid, String field, String value) throws IOException,
          GnsException {
    fieldCreateSingleElementArray(targetGuid.getGuid(), field, value, targetGuid);
  }

  /**
   * Appends the single value of the field onto list of values or creates a new field
   * with a single value list if it does not exist.
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
    String command = createAndSignQuery(writer, GnsProtocol.APPEND_OR_CREATE, GnsProtocol.GUID, targetGuid,
            GnsProtocol.FIELD, field, GnsProtocol.VALUE, value, GnsProtocol.WRITER, writer.getGuid());
    String response = sendGetCommand(command);
    checkResponse(command, response);
  }

  /**
   * Replaces the values of the field with the single value or creates a new
   * field with a single value list if it does not exist.
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
    String command = createAndSignQuery(writer, GnsProtocol.REPLACE_OR_CREATE, GnsProtocol.GUID, targetGuid,
            GnsProtocol.FIELD, field, GnsProtocol.VALUE, value, GnsProtocol.WRITER, writer.getGuid());
    String response = sendGetCommand(command);
    checkResponse(command, response);
  }

  /**
   * * Appends a single value onto a field.
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
    String command = createAndSignQuery(writer, GnsProtocol.APPEND_WITH_DUPLICATION, GnsProtocol.GUID, targetGuid,
            GnsProtocol.FIELD, field, GnsProtocol.VALUE, value, GnsProtocol.WRITER, writer.getGuid());
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * Appends a list of values onto a field but converts the list to set removing duplicates.
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
    String command = createAndSignQuery(writer, GnsProtocol.APPEND_LIST, GnsProtocol.GUID, targetGuid,
            GnsProtocol.FIELD, field, GnsProtocol.VALUE, value.toString(), GnsProtocol.WRITER, writer.getGuid());
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * Appends a single value onto a field but converts the list to set removing duplicates.
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
    String command = createAndSignQuery(writer, GnsProtocol.APPEND, GnsProtocol.GUID, targetGuid,
            GnsProtocol.FIELD, field, GnsProtocol.VALUE, value.toString(), GnsProtocol.WRITER, writer.getGuid());
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * Replaces all the first element of field with the value.
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
    String command = createAndSignQuery(writer, GnsProtocol.REPLACE, GnsProtocol.GUID, targetGuid,
            GnsProtocol.FIELD, field, GnsProtocol.VALUE, value, GnsProtocol.WRITER, writer.getGuid());
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * Substitutes the value for oldValue in the list of values of a field.
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
    String command = createAndSignQuery(writer, GnsProtocol.SUBSTITUTE, GnsProtocol.GUID,
            targetGuid, GnsProtocol.FIELD, field, GnsProtocol.VALUE, newValue,
            GnsProtocol.OLD_VALUE, oldValue);
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * Pairwise substitutes all the values for the oldValues in the list of values of a field.
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
    String command = createAndSignQuery(writer, GnsProtocol.SUBSTITUTE_LIST, GnsProtocol.GUID,
            targetGuid, GnsProtocol.FIELD, field, GnsProtocol.VALUE, newValue.toString(),
            GnsProtocol.OLD_VALUE, oldValue.toString());
    String response = sendGetCommand(command);

    checkResponse(command, response);
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
  public String fieldReadFirstElement(String guid, String field, GuidEntry reader) throws Exception {
    String command;
    if (reader == null) {
      command = createQuery(GnsProtocol.READ_ARRAY_ONE, GnsProtocol.GUID, guid, GnsProtocol.FIELD, field);
    } else {
      command = createAndSignQuery(reader, GnsProtocol.READ_ARRAY_ONE, GnsProtocol.GUID, guid, GnsProtocol.FIELD, field,
              GnsProtocol.READER, reader.getGuid());
    }

    String response = sendGetCommand(command);

    return checkResponse(command, response);
  }

  /**
   * Removes a tag from the tags of the guid.
   *
   * @param guid
   * @param tag
   * @throws Exception
   */
  public void removeTag(GuidEntry guid, String tag) throws Exception {
    String command = createAndSignQuery(guid, GnsProtocol.REMOVE_TAG,
            GnsProtocol.GUID, guid.getGuid(), GnsProtocol.NAME, tag);
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

  /**
   * Retrieves GUIDs that have been tagged with tag
   *
   * @param tag
   * @return
   * @throws Exception
   */
  public JSONArray retrieveTagged(String tag) throws Exception {
    String command = createQuery(GnsProtocol.DUMP, GnsProtocol.NAME, tag);
    String response = sendGetCommand(command);

    return new JSONArray(checkResponse(command, response));
  }

  /**
   * Removes all guids that have the corresponding tag. Removes the reverse
   * fields for the entity name and aliases. Note: doesn't remove all the
   * associated fields yet, though, so still a work in progress.
   *
   * @param tag
   * @throws Exception
   */
  public void clearTagged(String tag) throws Exception {
    String command = createQuery(GnsProtocol.CLEAR_TAGGED, GnsProtocol.NAME,
            tag);
    String response = sendGetCommand(command);

    checkResponse(command, response);
  }

}
