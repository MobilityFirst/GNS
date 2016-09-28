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

import edu.umass.cs.gnscommon.GNSCommandProtocol;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnscommon.CommandType;

import java.io.IOException;

import org.json.JSONArray;

import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.utils.DisabledClasses;

/**
 * This class defines an extension to AbstractGnrsClient to communicate with a GNS instance
 * over HTTP. This extension contains more methods.
 *
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
@Deprecated
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
   * @throws ClientException
   */
  public void fieldCreateSingleElementArray(String targetGuid, String field, String value, GuidEntry writer) throws IOException,
          ClientException {
    String command = createAndSignQuery(writer,
            CommandType.CreateList, GNSCommandProtocol.GUID, targetGuid,
            GNSCommandProtocol.FIELD, field, GNSCommandProtocol.VALUE, value, GNSCommandProtocol.WRITER, writer.getGuid());
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
   * @throws ClientException
   */
  public void fieldCreateSingleElementArray(GuidEntry targetGuid, String field, String value) throws IOException,
          ClientException {
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
   * @throws ClientException
   */
  public void fieldAppendOrCreate(String targetGuid, String field, String value, GuidEntry writer)
          throws IOException, ClientException {
    String command = createAndSignQuery(writer,
            CommandType.AppendOrCreateList, GNSCommandProtocol.GUID, targetGuid,
            GNSCommandProtocol.FIELD, field, GNSCommandProtocol.VALUE, value,
            GNSCommandProtocol.WRITER, writer.getGuid());
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
   * @throws ClientException
   */
  public void fieldReplaceOrCreate(String targetGuid, String field, String value, GuidEntry writer)
          throws IOException, ClientException {
    String command = createAndSignQuery(writer,
            CommandType.ReplaceOrCreateList, GNSCommandProtocol.GUID, targetGuid,
            GNSCommandProtocol.FIELD, field, GNSCommandProtocol.VALUE, value,
            GNSCommandProtocol.WRITER, writer.getGuid());
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
   * @throws ClientException
   */
  public void fieldAppend(String targetGuid, String field, String value, GuidEntry writer) throws IOException,
          ClientException {
    String command = createAndSignQuery(writer,
            CommandType.AppendListWithDuplication, GNSCommandProtocol.GUID, targetGuid,
            GNSCommandProtocol.FIELD, field, GNSCommandProtocol.VALUE, value, GNSCommandProtocol.WRITER, writer.getGuid());
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
   * @throws ClientException
   */
  public void fieldAppendWithSetSemantics(String targetGuid, String field, JSONArray value, GuidEntry writer) throws IOException,
          ClientException {
    String command = createAndSignQuery(writer,
            CommandType.AppendList, GNSCommandProtocol.GUID, targetGuid,
            GNSCommandProtocol.FIELD, field, GNSCommandProtocol.VALUE, value.toString(), GNSCommandProtocol.WRITER, writer.getGuid());
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
   * @throws ClientException
   */
  public void fieldAppendWithSetSemantics(String targetGuid, String field, String value, GuidEntry writer) throws IOException,
          ClientException {
    String command = createAndSignQuery(writer,
            CommandType.AppendList, GNSCommandProtocol.GUID, targetGuid,
            GNSCommandProtocol.FIELD, field, GNSCommandProtocol.VALUE, value.toString(), GNSCommandProtocol.WRITER, writer.getGuid());
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
   * @throws ClientException
   */
  public void fieldReplaceFirstElement(String targetGuid, String field, String value, GuidEntry writer) throws IOException,
          ClientException {
    String command = createAndSignQuery(writer,
            CommandType.ReplaceList, GNSCommandProtocol.GUID, targetGuid,
            GNSCommandProtocol.FIELD, field, GNSCommandProtocol.VALUE, value, GNSCommandProtocol.WRITER, writer.getGuid());
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
   * @throws ClientException
   */
  public void fieldSubstitute(String targetGuid, String field, String newValue,
          String oldValue, GuidEntry writer) throws IOException, ClientException {
    String command = createAndSignQuery(writer,
            CommandType.SubstituteList,
            GNSCommandProtocol.GUID, targetGuid,
            GNSCommandProtocol.FIELD, field, GNSCommandProtocol.VALUE, newValue,
            GNSCommandProtocol.OLD_VALUE, oldValue);
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
   * @throws ClientException
   */
  public void fieldSubstitute(String targetGuid, String field,
          JSONArray newValue, JSONArray oldValue, GuidEntry writer) throws IOException, ClientException {
    String command = createAndSignQuery(writer,
            CommandType.SubstituteList, GNSCommandProtocol.GUID,
            targetGuid, GNSCommandProtocol.FIELD, field, GNSCommandProtocol.VALUE, newValue.toString(),
            GNSCommandProtocol.OLD_VALUE, oldValue.toString());
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
      command = createQuery(
              CommandType.ReadArrayOneUnsigned,
              GNSCommandProtocol.GUID, guid, GNSCommandProtocol.FIELD, field);
    } else {
      command = createAndSignQuery(reader,
              CommandType.ReadArrayOne,
              GNSCommandProtocol.GUID, guid, GNSCommandProtocol.FIELD, field,
              GNSCommandProtocol.READER, reader.getGuid());
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
  @Deprecated
  public void removeTag(GuidEntry guid, String tag) throws Exception {
    throw new UnsupportedOperationException();
  }

  /**
   * Retrieves GUIDs that have been tagged with tag
   *
   * @param tag
   * @return
   * @throws Exception
   */
  @Deprecated
  public JSONArray retrieveTagged(String tag) throws Exception {
    throw new UnsupportedOperationException();
  }

  /**
   * Removes all guids that have the corresponding tag. Removes the reverse
   * fields for the entity name and aliases. Note: doesn't remove all the
   * associated fields yet, though, so still a work in progress.
   *
   * @param tag
   * @throws Exception
   */
  @Deprecated
  public void clearTagged(String tag) throws Exception {
    throw new UnsupportedOperationException();
  }

}
