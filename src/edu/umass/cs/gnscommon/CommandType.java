/* Copyright (1c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (1the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Initial developer(s): Westy */
package edu.umass.cs.gnscommon;

import edu.umass.cs.gnsclient.client.CommandResultType;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.utils.DefaultTest;
import edu.umass.cs.utils.Util;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import org.junit.Test;

/**
 * All the commands supported by the GNS server are listed here.
 *
 * Each one of these has a corresponding method in an array defined in
 * edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandDefs
 */
public enum CommandType {
//
  // Data Commands
  //
//
  // Data Commands
  //

  /**
   *
   */
  Append(110, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Append.class,
          CommandResultType.NULL, true, false,
          "Appends the value onto the key value pair for the given GUID. "
          + "Treats the list as a set, removing duplicates. "
          + "Field must be writeable by the WRITER guid.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.VALUE,
            GNSCommandProtocol.WRITER,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  AppendList(111, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendList.class,
          CommandResultType.NULL, true, false,
          "Appends the value onto of this key value pair for the given GUID. "
          + "Value is a list of items formated as a JSON list. "
          + "Field must be writeable by the WRITER guid.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.VALUE,
            GNSCommandProtocol.WRITER,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  AppendListUnsigned(113, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendListUnsigned.class,
          CommandResultType.NULL, true, false,
          "Appends the value onto of this key value pair for the given GUID. "
          + "Value is a list of items formated as a JSON list. "
          + "Field must be world writeable as this command does not specify "
          + "the writer and is not signed.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.VALUE}),

  /**
   *
   */
  AppendListWithDuplication(114, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendListWithDuplication.class,
          CommandResultType.NULL, true, false,
          "Appends the values onto this key value pair for the given GUID. "
          + "Treats the list as a list, allows dupicates. "
          + "Value is a list of items formated as a JSON list. "
          + "Field must be writeable by the WRITER guid.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.VALUE,
            GNSCommandProtocol.WRITER,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  AppendListWithDuplicationUnsigned(116, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendListWithDuplicationUnsigned.class,
          CommandResultType.NULL, true, false,
          "Appends the values onto of this key value pair for the given GUID. "
          + "Treats the list as a list, allows dupicate. "
          + "Value is a list of items formated as a JSON list. "
          + "Field must be world writeable as this command does not specify the writer and is not signed.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.VALUE}),

  /**
   *
   */
  AppendOrCreate(120, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendOrCreate.class,
          CommandResultType.NULL, true, false,
          "Adds a key value pair to the GNS for the given GUID if it doesn't not exist "
          + "otherwise append value onto existing value. "
          + "Field must be writeable by the WRITER guid.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.VALUE,
            GNSCommandProtocol.WRITER,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  AppendOrCreateList(121, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendOrCreateList.class,
          CommandResultType.NULL, true, false,
          "Adds a key value pair to the GNS for the given GUID if it doesn not exist "
          + "otherwise appends values onto existing value. "
          + "Value is a list of items formated as a JSON list. Field must be writeable by the WRITER guid.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.VALUE,
            GNSCommandProtocol.WRITER,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  AppendOrCreateListUnsigned(123, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendOrCreateListUnsigned.class,
          CommandResultType.NULL, true, false,
          "Adds a key value pair to the GNS for the given GUID if it doesn not exist "
          + "otherwise appends values onto existing value. "
          + "Value is a list of items formated as a JSON list. "
          + "Field must be world writeable as this command does not specify the "
          + "writer and is not signed.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.VALUE}),

  /**
   *
   */
  AppendOrCreateUnsigned(125, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendOrCreateUnsigned.class,
          CommandResultType.NULL, true, false,
          "Adds a key value pair to the GNS for the given GUID if it doesn't not exist "
          + "otherwise append value onto existing value. "
          + "Field must be world writeable as this command does not specify "
          + "the writer and is not signed.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.VALUE}),

  /**
   *
   */
  AppendUnsigned(131, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendUnsigned.class,
          CommandResultType.NULL, true, false,
          "Appends the value onto the key value pair for the given GUID. "
          + "Treats the list as a set, removing duplicates."
          + "Field must be world writeable as this command does not specify the writer and is not signed.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.VALUE}),

  /**
   *
   */
  AppendWithDuplication(132, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendWithDuplication.class,
          CommandResultType.NULL, true, false,
          "Appends the values onto this key value pair for the given GUID. "
          + "Treats the list as a list, allows dupicates. "
          + "Field must be writeable by the WRITER guid.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.VALUE,
            GNSCommandProtocol.WRITER,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  AppendWithDuplicationUnsigned(134, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendWithDuplicationUnsigned.class,
          CommandResultType.NULL, true, false,
          "Appends the values onto this key value pair for the given GUID. "
          + "Treats the list as a list, allows dupicates. "
          + "Field must be world writeable as this command does not specify the writer and is not signed.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.VALUE}),

  /**
   *
   */
  Clear(140, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Clear.class,
          CommandResultType.NULL, true, false,
          "Clears the key value pair from the GNS for the given GUID. "
          + "Field must be writeable by the WRITER guid.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.WRITER,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  ClearUnsigned(142, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ClearUnsigned.class,
          CommandResultType.NULL, true, false,
          "Clears the key value pair from the GNS for the given guid after authenticating that "
          + "GUID making request has access authority. "
          + "Field must be world writeable as this command does not specify "
          + "the writer and is not signed.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.WRITER}),

  /**
   *
   */
  Create(150, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Create.class,
          CommandResultType.NULL, true, false,
          "Adds a key value pair to the GNS for the given GUID. Field must be writeable by the WRITER guid.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.VALUE,
            GNSCommandProtocol.WRITER,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  CreateEmpty(151, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.CreateEmpty.class,
          CommandResultType.NULL, true, false,
          "Adds an empty field to the GNS for the given GUID. Field must be writeable by the WRITER guid."
          + "A shorthand for Create with an missing value field.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.WRITER,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  CreateList(153, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.CreateList.class,
          CommandResultType.NULL, true, false,
          "Adds a key value pair to the GNS for the given GUID. Value is a list of items "
          + "formated as a JSON list. Field must be writeable by the WRITER guid.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.VALUE,
            GNSCommandProtocol.WRITER,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  Read(160, CommandCategory.READ, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Read.class,
          CommandResultType.MAP, true, false,
          "Returns a key value pair from the GNS for the given guid after authenticating that "
          + "READER making request has access authority. "
          + "Field can use dot notation to access subfields. "
          + "Specify +ALL+ as the <field> to return all fields as a JSON object.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.READER,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  ReadUnsigned(162, CommandCategory.READ, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReadUnsigned.class,
          CommandResultType.MAP, true, false,
          "Returns one key value pair from the GNS. Does not require authentication but "
          + "field must be set to be readable by everyone. "
          + "Specify +ALL+ as the <field> to return all fields. ",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD}),

  /**
   *
   */
  ReadMultiField(163, CommandCategory.READ, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReadMultiField.class,
          CommandResultType.MAP, true, false,
          "Returns multiple key value pairs from the GNS for the given guid after "
          + "authenticating that READER making request has access authority. "
          + "Fields can use dot notation to access subfields.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELDS,
            GNSCommandProtocol.READER,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  ReadMultiFieldUnsigned(164, CommandCategory.READ, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReadMultiFieldUnsigned.class,
          CommandResultType.MAP, true, false,
          "Returns multiple key value pairs from the GNS for the given guid after "
          + "authenticating that READER making request has access authority. "
          + "Fields can use dot notation to access subfields.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELDS}),

  /**
   *
   */
  ReadArray(170, CommandCategory.READ, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReadArray.class,
          CommandResultType.LIST, true, false,
          "Returns one key value pair from the GNS for the given guid after authenticating "
          + "that READER making request has access authority. "
          + "Values are always returned as a JSON list. "
          + "Specify +ALL+ as the <field> to return all fields as a JSON object.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.READER,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  ReadArrayOne(171, CommandCategory.READ, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReadArrayOne.class,
          CommandResultType.STRING, true, false,
          "Returns one key value pair from the GNS for the given guid after authenticating "
          + "that the READER has access authority. Treats the value of key value pair "
          + "as a singleton item and returns that item. "
          + "Specify +ALL+ as the <field> to return all fields. ",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.READER,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  ReadArrayOneUnsigned(173, CommandCategory.READ, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReadArrayOneUnsigned.class,
          CommandResultType.STRING, true, false,
          "Returns one key value pair from the GNS for the given guid. Does not require authentication"
          + " but field must be set to be readable by everyone. "
          + "Treats the value of key value pair as a singleton item and returns that item. "
          + "Specify +ALL+ as the <field> to return all fields. ",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD}),

  /**
   *
   */
  ReadArrayUnsigned(175, CommandCategory.READ, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReadArrayUnsigned.class,
          CommandResultType.LIST, true, false,
          "Returns one key value pair from the GNS. Does not require authentication but "
          + "field must be set to be readable by everyone. Values are always returned as a JSON list. "
          + "Specify +ALL+ as the <field> to return all fields. ",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD}),

  /**
   *
   */
  Remove(180, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Remove.class,
          CommandResultType.NULL, true, false,
          "Returns one key value pair from the GNS for the given guid after authenticating that "
          + "WRITER making request has access authority. "
          + "Values are always returned as a JSON list. "
          + "Specify +ALL+ as the <field> to return all fields as a JSON object.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.VALUE,
            GNSCommandProtocol.WRITER,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  RemoveList(181, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.RemoveList.class,
          CommandResultType.NULL, true, false,
          "Removes all the values from the key value pair for the given GUID. "
          + "Value is a list of items formated as a JSON list. Field must be writeable by the WRITER guid.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.VALUE,
            GNSCommandProtocol.WRITER,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  RemoveListUnsigned(183, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.RemoveListUnsigned.class,
          CommandResultType.NULL, true, false,
          "Removes all the values from the key value pair for the given GUID. "
          + "Value is a list of items formated as a JSON list. "
          + "Field must be world writeable as this command does not specify the writer and is not signed.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.VALUE}),

  /**
   *
   */
  RemoveUnsigned(185, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.RemoveUnsigned.class,
          CommandResultType.NULL, true, false,
          "Removes the value from the key value pair for the given GUID. "
          + "Field must be world writeable as this command does not specify the writer and is not signed.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.VALUE}),

  /**
   *
   */
  Replace(190, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Replace.class,
          CommandResultType.NULL, true, false,
          "Replaces the current value key value pair from the GNS for the given guid. "
          + "Field must be writeable by the WRITER guid.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.VALUE,
            GNSCommandProtocol.WRITER,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  ReplaceList(191, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceList.class,
          CommandResultType.NULL, true, false,
          "Replaces the current value key value pair from the GNS for the given guid with the given values. "
          + "Field must be writeable by the WRITER guid.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.VALUE,
            GNSCommandProtocol.WRITER,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  ReplaceListUnsigned(193, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceListUnsigned.class,
          CommandResultType.NULL, true, false,
          "Replaces the current value key value pair from the GNS for the given guid with the given values. "
          + "Field must be world writeable as this command does not specify the writer and is not signed.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.VALUE}),

  /**
   *
   */
  ReplaceOrCreate(210, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceOrCreate.class,
          CommandResultType.NULL, true, false,
          "Adds a key value pair to the GNS for the given GUID if it doesn not exist otherwise "
          + "replaces the value of this key value pair for the given GUID. "
          + "Field must be writeable by the WRITER guid.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.VALUE,
            GNSCommandProtocol.WRITER,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  ReplaceOrCreateList(211, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceOrCreateList.class,
          CommandResultType.NULL, true, false,
          "Adds a key value pair to the GNS for the given GUID if it does not exist otherwise "
          + "replaces the value of this key value pair for the given GUID with the value. "
          + "Value is a list of items formated as a JSON list. "
          + "Field must be writeable by the WRITER guid.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.VALUE,
            GNSCommandProtocol.WRITER,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  ReplaceOrCreateListUnsigned(213, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceOrCreateListUnsigned.class,
          CommandResultType.NULL, true, false,
          "Adds a key value pair to the GNS for the given GUID if it does not exist otherwise "
          + "replaces the value of this key value pair for the given GUID with the value. "
          + "Value is a list of items formated as a JSON list. "
          + "Field must be world writeable as this command does not specify the writer and is not signed.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.VALUE}),

  /**
   *
   */
  ReplaceOrCreateUnsigned(215, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceOrCreateUnsigned.class,
          CommandResultType.NULL, true, false,
          "Adds a key value pair to the GNS for the given GUID if it doesn not exist otherwise "
          + "replaces the value of this key value pair for the given GUID. "
          + "Field must be world writeable as this command does not specify the writer and is not signed.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.VALUE}),

  /**
   *
   */
  ReplaceUnsigned(217, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceUnsigned.class,
          CommandResultType.NULL, true, false,
          "Replaces the current value key value pair from the GNS for the given guid. "
          + "Field must be world writeable as this command does not specify the writer and is not signed.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.VALUE}),

  /**
   *
   */
  ReplaceUserJSON(220, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceUserJSON.class,
          CommandResultType.NULL, true, false,
          "Replaces existing fields in JSON record with the given JSONObject's fields. "
          + "Doesn't touch top-level fields that aren't in the given JSONObject.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.USER_JSON,
            GNSCommandProtocol.WRITER,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  ReplaceUserJSONUnsigned(221, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceUserJSONUnsigned.class,
          CommandResultType.NULL, true, false,
          "Replaces existing fields in JSON record with the given JSONObject's fields. "
          + "Field must be world writeable as this command does "
          + "not specify the writer and is not signed. Doesn't touch top-level "
          + "fields that aren't in the given JSONObject.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.USER_JSON,
            GNSCommandProtocol.WRITER}),
  //Fixme: CREATE_INDEX should be an ADMIN_UPDATE command.

  /**
   *
   */
  CreateIndex(230, CommandCategory.OTHER, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.CreateIndex.class,
          CommandResultType.NULL, true, false,
          "Creates an index for field. The value is a string containing the index type.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.VALUE,
            GNSCommandProtocol.WRITER,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  Substitute(231, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Substitute.class,
          CommandResultType.NULL, true, false,
          "Replaces OLD_VALUE with newvalue in the key value pair for the given GUID. "
          + "Field must be writeable by the WRITER guid.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.VALUE,
            GNSCommandProtocol.OLD_VALUE,
            GNSCommandProtocol.WRITER,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  SubstituteList(232, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.SubstituteList.class,
          CommandResultType.NULL, true, false,
          "Replaces OLD_VALUE with newvalue in the key value pair for the given GUID. "
          + "Value is a list of items formated as a JSON list. "
          + "Field must be writeable by the WRITER guid. "
          + "If the new value and old values list are different sizes this does the obvious things and "
          + "stops when it runs out of values on the shorter list.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.VALUE,
            GNSCommandProtocol.OLD_VALUE,
            GNSCommandProtocol.WRITER,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  SubstituteListUnsigned(234, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.SubstituteListUnsigned.class,
          CommandResultType.NULL, true, false,
          "Replaces OLD_VALUE with newvalue in the key value pair for the given GUID. "
          + "Value is a list of items formated as a JSON list. "
          + "Field must be world writeable as this command does not specify the writer and is not signed. "
          + "If the new value and old values list are different sizes this does the obvious things and "
          + "stops when it runs out of values on the shorter list.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.VALUE,
            GNSCommandProtocol.OLD_VALUE}),

  /**
   *
   */
  SubstituteUnsigned(236, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.SubstituteUnsigned.class,
          CommandResultType.NULL, true, false,
          "Replaces OLD_VALUE with newvalue in the key value pair for the given GUID. "
          + "See below for more on the signature. "
          + "Field must be world writeable as this command does not specify the writer and is not signed.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.VALUE,
            GNSCommandProtocol.OLD_VALUE}),

  /**
   *
   */
  RemoveField(240, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.RemoveField.class,
          CommandResultType.NULL, true, false,
          "Removes the key value pair from the GNS for the given guid after authenticating "
          + "that WRITER making request has access authority.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.WRITER,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  RemoveFieldUnsigned(242, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.RemoveFieldUnsigned.class,
          CommandResultType.NULL, true, false,
          "Removes the key value pair from the GNS for the given guid. "
          + "Field must be world writeable as this command does not specify the writer and is not signed.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.WRITER}),

  /**
   *
   */
  Set(250, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Set.class,
          CommandResultType.NULL, true, false,
          "Replaces element N with newvalue in the key value pair for the given GUID. "
          + "Field must be writeable by the WRITER guid.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.VALUE,
            GNSCommandProtocol.N,
            GNSCommandProtocol.WRITER,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  SetFieldNull(252, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.SetFieldNull.class,
          CommandResultType.NULL, true, false,
          "Sets the field to contain a null value. Field must be writeable by the WRITER guid.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.WRITER,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),
  //
  // Basic select commands
  //

  /**
   *
   */
  Select(310, CommandCategory.SELECT, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.Select.class,
          CommandResultType.LIST, false, false,
          "Returns the guids of all records that have a field with the given value. "
          + "Values are returned as a JSON array of guids. "
          + "This command is a shorthand for a mongo find query.",
          new String[]{GNSCommandProtocol.FIELD,
            GNSCommandProtocol.VALUE}),

  /**
   *
   */
  SelectNear(320, CommandCategory.SELECT, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.SelectNear.class,
          CommandResultType.LIST, false, false,
          "Return the guids of all records that are within max distance of value. Key must be a GeoSpatial field. "
          + "Value is a point specified as a JSONArray string tuple: [LONG, LAT]. Max Distance is in meters. "
          + "Values are returned as a JSON array of guids. "
          + "This command is a shorthand for a mongo $near query.",
          new String[]{GNSCommandProtocol.FIELD,
            GNSCommandProtocol.NEAR,
            GNSCommandProtocol.MAX_DISTANCE}),

  /**
   *
   */
  SelectWithin(321, CommandCategory.SELECT, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.SelectWithin.class,
          CommandResultType.LIST, false, false,
          "Returns the guids of all records that are within value which is a bounding box specified. "
          + "Key must be a GeoSpatial field. "
          + "Bounding box is a nested JSONArray string tuple of paired tuples: [[LONG_UL, LAT_UL],[LONG_BR, LAT_BR]] "
          + "Values are returned as a JSON array of guids. "
          + "This command is a shorthand for a mongo $within query.",
          new String[]{GNSCommandProtocol.FIELD,
            GNSCommandProtocol.WITHIN}),

  /**
   *
   */
  SelectQuery(322, CommandCategory.SELECT, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.SelectQuery.class,
          CommandResultType.LIST, false, false,
          "Returns the guids of all records that satisfy the query. "
          + "For details see http://gns.name/wiki/index.php/Query_Syntax "
          + "Values are returned as a JSON array of guids.",
          new String[]{GNSCommandProtocol.QUERY}),
  //
  // Select commands that maintain a group guid
  //

  /**
   *
   */
  SelectGroupLookupQuery(311, CommandCategory.SELECT, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.SelectGroupLookupQuery.class,
          CommandResultType.LIST, false, false,
          "Returns all records for a group guid that was previously setup with a query. "
          + "For details see http://gns.name/wiki/index.php/Query_Syntax "
          + "Values are returned as a JSON array of JSON Objects.",
          new String[]{GNSCommandProtocol.GUID}),

  /**
   *
   */
  SelectGroupSetupQuery(312, CommandCategory.SELECT, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.SelectGroupSetupQuery.class,
          CommandResultType.LIST, false, false,
          "Initializes a new group guid to automatically update and maintain all records that satisfy the query. "
          + "For details see http://gns.name/wiki/index.php/Query_Syntax "
          + "Values are returned as a JSON array of JSON Objects.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.QUERY}),

  /**
   *
   */
  SelectGroupSetupQueryWithGuid(313, CommandCategory.SELECT, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.SelectGroupSetupQueryWithGuid.class,
          CommandResultType.LIST, false, false,
          "Initializes the given group guid to automatically update and maintain all records that satisfy the query. "
          + "For details see http://gns.name/wiki/index.php/Query_Syntax "
          + "Values are returned as a JSON array of JSON Objects.",
          new String[]{GNSCommandProtocol.QUERY,
            GNSCommandProtocol.GUID}),

  /**
   *
   */
  SelectGroupSetupQueryWithGuidAndInterval(314, CommandCategory.SELECT, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.SelectGroupSetupQueryWithGuidAndInterval.class,
          CommandResultType.LIST, false, false,
          "Initializes the group guid to automatically update and maintain all records that satisfy the query. "
          + "Interval is the minimum refresh interval of the query - lookups happening more quickly than this "
          + "interval will retrieve a stale value.For details see http://gns.name/wiki/index.php/Query_Syntax"
          + "Values are returned as a JSON array of JSON Objects.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.QUERY,
            GNSCommandProtocol.INTERVAL}),

  /**
   *
   */
  SelectGroupSetupQueryWithInterval(315, CommandCategory.SELECT, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.SelectGroupSetupQueryWithInterval.class,
          CommandResultType.LIST, false, false,
          "Initializes a new group guid to automatically update and maintain all records that satisfy the query. "
          + "Interval is the minimum refresh interval of the query - lookups happening more "
          + "quickly than this interval will retrieve a stale value. "
          + "For details see http://gns.name/wiki/index.php/Query_Syntax "
          + "Values are returned as a JSON array of JSON Objects.",
          new String[]{GNSCommandProtocol.QUERY,
            GNSCommandProtocol.INTERVAL}),
  //
  // Account commands
  //

  /**
   *
   */
  AddAlias(410, CommandCategory.CREATE_DELETE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.AddAlias.class,
          CommandResultType.NULL, false, false,
          "Adds a additional human readble name to the account associated with the GUID. "
          + "Must be signed by the guid. Returns +BADGUID+ if the GUID has not been registered.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.NAME,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  AddGuid(411, CommandCategory.CREATE_DELETE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.AddGuid.class,
          CommandResultType.NULL, false, false,
          "Adds a GUID to the account associated with the GUID. Must be signed by the guid. "
          + "Returns +BADGUID+ if the GUID has not been registered.",
          new String[]{GNSCommandProtocol.NAME,
            GNSCommandProtocol.GUID,
            GNSCommandProtocol.PUBLIC_KEY,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  AddMultipleGuids(412, CommandCategory.CREATE_DELETE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.AddMultipleGuids.class,
          CommandResultType.NULL, false, false,
          "Creates multiple guids for the account associated with the account guid. "
          + "Must be signed by the account guid. Returns +BADGUID+ if the account guid has not been registered.",
          new String[]{GNSCommandProtocol.NAMES,
            GNSCommandProtocol.GUID,
            GNSCommandProtocol.PUBLIC_KEYS,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  AddMultipleGuidsFast(413, CommandCategory.CREATE_DELETE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.AddMultipleGuidsFast.class,
          CommandResultType.NULL, false, false,
          "Creates multiple guids for the account associated with the account guid. "
          + "Must be signed by the account guid. The created guids can only be accessed "
          + "using the account guid because the haveno private key info stored on the client. "
          + "Returns +BADGUID+ if the account guid has not been registered.",
          new String[]{GNSCommandProtocol.NAMES,
            GNSCommandProtocol.GUID,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  AddMultipleGuidsFastRandom(414, CommandCategory.CREATE_DELETE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.AddMultipleGuidsFastRandom.class,
          CommandResultType.NULL, false, false,
          "Creates multiple guids for the account associated with the account guid. "
          + "Must be signed by the account guid. The created guids can only be accessed using the "
          + "account guid because the haveno private key info stored on the client."
          + "Returns +BADGUID+ if the account guid has not been registered.",
          new String[]{GNSCommandProtocol.GUIDCNT,
            GNSCommandProtocol.GUID,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  LookupAccountRecord(420, CommandCategory.SYSTEM_LOOKUP, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.LookupAccountRecord.class,
          CommandResultType.MAP, true, false,
          "Returns the account info associated with the given GUID. "
          + "Returns +BADGUID+ if the GUID has not been registered.",
          new String[]{GNSCommandProtocol.GUID}),

  /**
   *
   */
  LookupRandomGuids(421, CommandCategory.SYSTEM_LOOKUP, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.LookupRandomGuids.class,
          CommandResultType.LIST, true, false,
          "Returns the account info associated with the given GUID. "
          + "Returns +BADGUID+ if the GUID has not been registered.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.GUIDCNT}),

  /**
   *
   */
  LookupGuid(422, CommandCategory.SYSTEM_LOOKUP, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.LookupGuid.class,
          CommandResultType.STRING, true, false,
          "Returns the guid associated with for the human readable name. "
          + "Returns +BADACCOUNT+ if the GUID has not been registered.",
          new String[]{GNSCommandProtocol.NAME}),

  /**
   *
   */
  LookupPrimaryGuid(423, CommandCategory.SYSTEM_LOOKUP, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.LookupPrimaryGuid.class,
          CommandResultType.STRING, true, false,
          "Returns the account guid associated the guid. Returns +BADGUID+ if the GUID does not exist.",
          new String[]{GNSCommandProtocol.GUID}),

  /**
   *
   */
  LookupGuidRecord(424, CommandCategory.SYSTEM_LOOKUP, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.LookupGuidRecord.class,
          CommandResultType.MAP, true, false,
          "Returns human readable name and public key associated with the given GUID. "
          + "Returns +BADGUID+ if the GUID has not been registered.",
          new String[]{GNSCommandProtocol.GUID}),

  /**
   *
   */
  RegisterAccount(430, CommandCategory.CREATE_DELETE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.RegisterAccount.class,
          CommandResultType.NULL, false, false,
          "Creates an account GUID associated with the human readable name and the supplied public key. "
          + "Must be sign with the public key. Returns a guid.",
          new String[]{GNSCommandProtocol.NAME,
            GNSCommandProtocol.PUBLIC_KEY,
            GNSCommandProtocol.PASSWORD,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  RegisterAccountUnsigned(432, CommandCategory.CREATE_DELETE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.RegisterAccountUnsigned.class,
          CommandResultType.NULL, false, false,
          "Creates an account GUID associated with the human readable name and the supplied public key. "
          + "Must be sign dwith the public key. Returns a guid.",
          new String[]{GNSCommandProtocol.NAME,
            GNSCommandProtocol.PUBLIC_KEY,
            GNSCommandProtocol.PASSWORD}),

  /**
   *
   */
  RemoveAccount(440, CommandCategory.CREATE_DELETE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.RemoveAccount.class,
          CommandResultType.NULL, false, false,
          "Removes the account GUID associated with the human readable name. "
          + "Must be signed by the guid.",
          new String[]{GNSCommandProtocol.NAME,
            GNSCommandProtocol.GUID,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),
 
  /**
   *
   */
  RemoveAlias(441, CommandCategory.CREATE_DELETE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.RemoveAlias.class,
          CommandResultType.NULL, false, false,
          "Removes the alias from the account associated with the GUID. "
          + "Must be signed by the guid. Returns +BADGUID+ if the GUID has not been registered.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.NAME,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  RemoveGuid(442, CommandCategory.CREATE_DELETE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.RemoveGuid.class,
          CommandResultType.NULL, false, false,
          "Removes the GUID from the account associated with the ACCOUNT_GUID. "
          + "Must be signed by the account guid or the guid if account guid is not provided. "
          + "Returns +BADGUID+ if the GUID has not been registered.",
          new String[]{GNSCommandProtocol.ACCOUNT_GUID,
            GNSCommandProtocol.GUID,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  RemoveGuidNoAccount(443, CommandCategory.CREATE_DELETE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.RemoveGuidNoAccount.class,
          CommandResultType.NULL, false, false,
          "Removes the GUID. Must be signed by the guid. Returns +BADGUID+ if the GUID has not been registered.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  RetrieveAliases(444, CommandCategory.SYSTEM_LOOKUP, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.RetrieveAliases.class,
          CommandResultType.LIST, true, false,
          "Retrieves all aliases for the account associated with the GUID. "
          + "Must be signed by the guid. Returns +BADGUID+ if the GUID has not been registered.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  RemoveAccountWithPassword(445, CommandCategory.CREATE_DELETE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.RemoveAccountWithPassword.class,
          CommandResultType.NULL, false, false,
          "Removes the account GUID associated with the human readable name authorized by the account password.",
          new String[]{GNSCommandProtocol.NAME, GNSCommandProtocol.PASSWORD}),

  /**
   *
   */
  SetPassword(450, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.SetPassword.class,
          CommandResultType.NULL, true, false,
          "Sets the password. Must be signed by the guid. Returns +BADGUID+ if the GUID has not been registered.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.PASSWORD,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  VerifyAccount(451, CommandCategory.OTHER, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.VerifyAccount.class,
          CommandResultType.STRING, true, false,
          "Handles the completion of the verification process for a guid by supplying the correct code.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.CODE}),

  /**
   *
   */
  ResendAuthenticationEmail(452, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.ResendAuthenticationEmail.class,
          CommandResultType.NULL, true, false,
          "Resends the verification code email to the user. Must be signed by the guid. Returns +BADGUID+ if the GUID has not been registered.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  ResetKey(460, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.ResetKey.class,
          CommandResultType.NULL, true, false,
          "Resets the publickey for the account guid.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.PUBLIC_KEY,
            GNSCommandProtocol.PASSWORD}),
  //
  // ACL Commands
  //

  /**
   *
   */
  AclAdd(510, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.acl.AclAdd.class,
          CommandResultType.NULL, true, false,
          "Updates the access control list of the given GUID's field to include the accesser guid. Accessor guid can "
          + "be guid or group guid or +ALL+ which means anyone. "
          + "Field can be also be +ALL+ which means all fields can be read by the accessor. "
          + "See below for description of ACL type and signature.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.ACCESSER,
            GNSCommandProtocol.WRITER,
            GNSCommandProtocol.ACL_TYPE,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  AclAddSelf(511, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.acl.AclAddSelf.class,
          CommandResultType.NULL, true, false,
          "Updates the access control list of the given GUID's field to include the accesser guid. "
          + "Accessor should a guid or group guid or +ALL+ which means anyone. "
          + "Field can be also be +ALL+ which means all fields can be read by the accessor. "
          + "See below for description of ACL type and signature.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.ACCESSER,
            GNSCommandProtocol.ACL_TYPE,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  AclRemove(512, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.acl.AclRemove.class,
          CommandResultType.NULL, true, false,
          "Updates the access control list of the given GUID's field to remove the accesser guid. "
          + "Accessor should be the guid or group guid to be removed.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.ACCESSER,
            GNSCommandProtocol.WRITER,
            GNSCommandProtocol.ACL_TYPE,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  AclRemoveSelf(513, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.acl.AclRemoveSelf.class,
          CommandResultType.NULL, true, false,
          "Updates the access control list of the given GUID's field to remove the accesser guid. "
          + "Accessor should be the guid or group guid to be removed.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.ACCESSER,
            GNSCommandProtocol.ACL_TYPE,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  AclRetrieve(514, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.acl.AclRetrieve.class,
          CommandResultType.LIST, true, false,
          "Returns the access control list for a guids's field.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.ACL_TYPE,
            GNSCommandProtocol.READER,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  AclRetrieveSelf(515, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.acl.AclRetrieveSelf.class,
          CommandResultType.LIST, true, false,
          "Returns the access control list for a guids's field.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.FIELD,
            GNSCommandProtocol.ACL_TYPE,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),
  //
  // Group Commands
  //

  /**
   *
   */
  AddMembersToGroup(610, CommandCategory.OTHER, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.AddMembersToGroup.class,
          CommandResultType.NULL, false, false,
          "Adds the member guids to the group specified by guid. "
          + "Writer guid needs to have write access and sign the command.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.MEMBERS,
            GNSCommandProtocol.WRITER,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  AddMembersToGroupSelf(611, CommandCategory.OTHER, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.AddMembersToGroupSelf.class,
          CommandResultType.NULL, false, false,
          "Adds the member guids to the group specified by guid. "
          + "Writer guid needs to have write access and sign the command.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.MEMBERS,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  AddToGroup(612, CommandCategory.OTHER, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.AddToGroup.class,
          CommandResultType.NULL, false, false,
          "Adds the member guid to the group specified by guid. "
          + "Writer guid needs to have write access and sign the command.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.MEMBER,
            GNSCommandProtocol.WRITER,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  AddToGroupSelf(613, CommandCategory.OTHER, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.AddToGroupSelf.class,
          CommandResultType.NULL, false, false,
          "Adds the member guid to the group specified by guid.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.MEMBER,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  GetGroupMembers(614, CommandCategory.SYSTEM_LOOKUP, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.GetGroupMembers.class,
          CommandResultType.LIST, true, false,
          "Returns the members of the group formatted as a JSON Array. "
          + "Reader guid needs to have read access and sign the command.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.READER,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  GetGroupMembersSelf(615, CommandCategory.SYSTEM_LOOKUP, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.GetGroupMembersSelf.class,
          CommandResultType.LIST, true, false,
          "Returns the members of the group formatted as a JSON Array.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  GetGroups(616, CommandCategory.SYSTEM_LOOKUP, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.GetGroups.class,
          CommandResultType.LIST, true, false,
          "Returns the groups that a guid is a member of formatted as a JSON Array. "
          + "Reader guid needs to have read access and sign the command.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.READER,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  GetGroupsSelf(617, CommandCategory.SYSTEM_LOOKUP, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.GetGroupsSelf.class,
          CommandResultType.LIST, true, false,
          "Returns the groups that a guid is a member of formatted as a JSON Array.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  RemoveFromGroup(620, CommandCategory.OTHER, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.RemoveFromGroup.class,
          CommandResultType.NULL, false, false,
          "Removes the member guid from the group specified by guid. "
          + "Writer guid needs to have write access and sign the command.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.MEMBER,
            GNSCommandProtocol.WRITER,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  RemoveFromGroupSelf(621, CommandCategory.OTHER, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.RemoveFromGroupSelf.class,
          CommandResultType.NULL, false, false,
          "Removes the member guid from the group specified by guid.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.MEMBER,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  RemoveMembersFromGroup(622, CommandCategory.OTHER, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.RemoveMembersFromGroup.class,
          CommandResultType.NULL, false, false,
          "Removes the member guids from the group specified by guid. "
          + "Writer guid needs to have write access and sign the command.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.MEMBERS,
            GNSCommandProtocol.WRITER,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  RemoveMembersFromGroupSelf(623, CommandCategory.OTHER, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.RemoveMembersFromGroupSelf.class,
          CommandResultType.NULL, false, false,
          "Removes the member guids from the group specified by guid.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.MEMBERS,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  Help(710, CommandCategory.OTHER, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Help.class,
          CommandResultType.STRING, true, false,
          "Returns this help message.",
          new String[]{}),

  /**
   *
   */
  HelpTcp(711, CommandCategory.OTHER, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.HelpTcp.class,
          CommandResultType.STRING, true, false,
          "Returns the help message for TCP commands.",
          new String[]{"tcp"}),

  /**
   *
   */
  HelpTcpWiki(712, CommandCategory.OTHER, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.HelpTcpWiki.class,
          CommandResultType.STRING, true, false,
          "Returns the help message for TCP commands in wiki format.",
          new String[]{"tcpwiki"}),

  /**
   *
   */
  Admin(715, CommandCategory.MUTUAL_AUTH, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.Admin.class,
          CommandResultType.NULL, true, true,
          "Turns on admin mode.",
          new String[]{GNSCommandProtocol.PASSKEY}),

  /**
   *
   */
  Dump(716, CommandCategory.MUTUAL_AUTH, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.Dump.class,
          CommandResultType.STRING, true, true,
          "[ONLY IN ADMIN MODE] Returns the contents of the GNS.",
          new String[]{GNSCommandProtocol.PASSKEY}),

  /**
   *
   */
  GetParameter(720, CommandCategory.MUTUAL_AUTH, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.GetParameter.class,
          CommandResultType.STRING, true, true,
          "Returns one key value pair from the GNS for the given guid after authenticating "
          + "that GUID making request has access authority. "
          + "Values are always returned as a JSON list. "
          + "Specify +ALL+ as the <field> to return all fields as a JSON object.",
          new String[]{GNSCommandProtocol.FIELD}),

  /**
   *
   */
  SetParameter(721, CommandCategory.MUTUAL_AUTH, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.SetParameter.class,
          CommandResultType.NULL, true, true,
          "[ONLY IN ADMIN MODE] Changes a parameter value.",
          new String[]{GNSCommandProtocol.FIELD,
            GNSCommandProtocol.VALUE}),

  /**
   *
   */
  ListParameters(722, CommandCategory.MUTUAL_AUTH, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.ListParameters.class,
          CommandResultType.STRING, true, true,
          "[ONLY IN ADMIN MODE] Lists all parameter values.",
          new String[]{}),

  /**
   *
   */
  ClearCache(725, CommandCategory.MUTUAL_AUTH, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.ClearCache.class,
          CommandResultType.NULL, true, true,
          "[ONLY IN ADMIN MODE] Clears the local name server cache.",
          new String[]{}),

  /**
   *
   */
  DumpCache(726, CommandCategory.MUTUAL_AUTH, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.DumpCache.class,
          CommandResultType.STRING, true, false,
          "[ONLY IN ADMIN MODE] Returns the contents of the local name server cache.",
          new String[]{}),

  /**
   *
   */
  ConnectionCheck(737, CommandCategory.OTHER, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.ConnectionCheck.class,
          CommandResultType.STRING, true, false,
          "Checks connectivity.",
          new String[]{}),

  /**
   *
   */
  SetCode(810, CommandCategory.OTHER, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.activecode.SetCode.class,
          CommandResultType.NULL, true, false,
          "Sets the given active code for the specified GUID and action, ensuring the writer has permission.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.WRITER,
            GNSCommandProtocol.AC_ACTION,
            GNSCommandProtocol.AC_CODE,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  ClearCode(811, CommandCategory.OTHER, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.activecode.ClearCode.class,
          CommandResultType.NULL, true, false,
          "Clears the active code for the specified GUID and action, ensuring the writer has permission.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.WRITER,
            GNSCommandProtocol.AC_ACTION,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  GetCode(812, CommandCategory.OTHER, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.activecode.GetCode.class,
          CommandResultType.STRING, true, false,
          "Returns the active code for the specified action, ensuring the reader has permission.",
          new String[]{GNSCommandProtocol.GUID,
            GNSCommandProtocol.READER,
            GNSCommandProtocol.AC_ACTION,
            GNSCommandProtocol.SIGNATURE,
            GNSCommandProtocol.SIGNATUREFULLMESSAGE}),

  /**
   *
   */
  Unknown(999, CommandCategory.OTHER, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.Unknown.class,
          CommandResultType.NULL, true, true,
          "A null command.",
          new String[]{});

  private final int number;
  private final CommandCategory category;
  private final Class<?> commandClass;
  private final CommandResultType returnType;

  /**
   * True means that this command can be safely coordinated by RemoteQuery at
   * servers. True must mean that it is never the case that execute(name1) for
   * any canBeSafelyCoordinated command never remote-query-invokes this
   * command also for name1.
   */
  private boolean canBeSafelyCoordinated;

  /**
   * Commands that are not intended to be run by rogue clients.
   */
  private boolean notForRogueClients;

  /**
   * A description of the command.
   */
  private final String commandDescription;

  /**
   * The parameters of the command.
   */
  private final String[] commandParameters;

  /**
   * Other commands that may be remote-query-invoked by this command.
   * Non-final only because we can not name enums before they are defined.
   */
  private CommandType[] invokedCommands;

  /**
   * The command category.
   */
  public enum CommandCategory {

    /**
     *
     */
    READ,

    /**
     *
     */
    UPDATE,

    /**
     *
     */
    CREATE_DELETE,

    /**
     *
     */
    SELECT,

    /**
     *
     */
    OTHER,

    /**
     *
     */
    SYSTEM_LOOKUP,

    /**
     *
     */
    MUTUAL_AUTH
  }

  private CommandType(int number, CommandCategory category, Class<?> commandClass,
          CommandResultType returnType, boolean canBeSafelyCoordinated,
          boolean notForRogueClients, String description, String[] parameters) {
    this.number = number;
    this.category = category;
    this.commandClass = commandClass;
    this.returnType = returnType;

    this.canBeSafelyCoordinated = canBeSafelyCoordinated;

    // presumably every command is currently available to thugs
    this.notForRogueClients = notForRogueClients;

    this.commandDescription = description;
    this.commandParameters = parameters;

  }

  private static final Map<Integer, CommandType> map = new HashMap<>();
  private static final Map<String, CommandType> lowerCaseCommandNameMapForHttp = new HashMap<>();

  static {
    for (CommandType type : CommandType.values()) {
      if (!type.getCommandClass().getSimpleName().equals(type.name())) {
        GNSConfig.getLogger().log(Level.WARNING,
                "Enum name should be the same as implementing class: {0} vs. {1}",
                new Object[]{type.getCommandClass().getSimpleName(), type.name()});
      }
      if (map.containsKey(type.getInt())) {
        GNSConfig.getLogger().warning(
                "**** Duplicate number for command type " + type + ": "
                + type.getInt());
      }
      map.put(type.getInt(), type);
      lowerCaseCommandNameMapForHttp.put(type.name().toLowerCase(), type);
    }
  }

  // In general, isCoordinated is not equivalent to isUpdate()
  private boolean isCoordinated() {
    return this.isUpdate();
  }

  /**
   *
   * @return the int
   */
  public int getInt() {
    return number;
  }

  /**
   *
   * @return the category
   */
  public CommandCategory getCategory() {
    return category;
  }

  /**
   *
   * @return the command class
   */
  public Class<?> getCommandClass() {
    return commandClass;
  }

  /**
   *
   * @return the result type
   */
  public CommandResultType getResultType() {
    return this.returnType;
  }

  /**
   *
   * @return true if the command can be safely coordinated
   */
  public boolean isCanBeSafelyCoordinated() {
    return canBeSafelyCoordinated;
  }

  /**
   *
   * @return true if the should not be executed by rogue clients
   */
  public boolean isNotForRogueClients() {
    return notForRogueClients;
  }

  /**
   *
   * @return the command description
   */
  public String getCommandDescription() {
    return commandDescription;
  }

  /**
   *
   * @return the command parameters
   */
  public String[] getCommandParameters() {
    return commandParameters;
  }

  // add isCoordinated
  // add strictly local flag or remote flag
  // what are the set of commands that will be invoked by this command
  // AKA multi-transactional commands

  /**
   *
   * @return true if it's a read command
   */
  public boolean isRead() {
    return category.equals(CommandCategory.READ);
  }

  /**
   *
   * @return true if it's an update command
   */
  public boolean isUpdate() {
    return category.equals(CommandCategory.UPDATE);
  }

  /**
   *
   * @return true if it's a create or delete command
   */
  public boolean isCreateDelete() {
    return category.equals(CommandCategory.CREATE_DELETE);
  }

  /**
   *
   * @return true if it's a select command
   */
  public boolean isSelect() {
    return category.equals(CommandCategory.SELECT);
  }

  /**
   *
   * @return true if it's a system lookup command
   */
  public boolean isSystemLookup() {
    return category.equals(CommandCategory.SYSTEM_LOOKUP);
  }

  /**
   *
   * @return true if it's mutual auth command
   */
  public boolean isMutualAuth() {
    return category.equals(CommandCategory.MUTUAL_AUTH);
  }

  /**
   *
   * @param number
   * @return the command type
   */
  public static CommandType getCommandType(int number) {
    return map.get(number);
  }
  
  /**
   * Returns the command that corresponds to the name ignoring case.
   * 
   * @param name
   * @return the command
   */
  public static CommandType getCommandForHttp(String name) {
    return lowerCaseCommandNameMapForHttp.get(name.toLowerCase());
  }

  private void setChain(CommandType... invokedCommands) {
    if (this.invokedCommands == null) {
      this.invokedCommands = invokedCommands;
    } else {
      throw new RuntimeException("Can set command chain exactly once");
    }
  }

  /**
   * The chain of any command must contain a list
   * of all commands that the execution of that command MAY invoke. For
   * example, if AddGuid may (not necessarily every time) invoke LookupGuid as
   * a step, LookupGuid belongs to AddGuid's chain. The list interpretation is
   * somewhat loose as the call chain is really a DAG, but it is good to
   * flatten the DAG in breadth-first order, e.g., if A->B and B->C and B->D
   * and D->E, where "->" means "invokes", the chain of A is {B,C,D,E}. It is
   * okay to stop recursively unraveling a chain, e.g., stop at A->B, if what
   * follows is identical to B's chain.
   *
   * Hopefully there are no cycles in these chains. Assume standard execution
   * for all commands, i.e., with no active code enabled, while listing chains.
   */
  static {
    Read.setChain(ReadUnsigned);
    //ReadSelf.setChain(ReadUnsigned);
    ReadUnsigned.setChain();
    ReadMultiField.setChain(ReadUnsigned);
    ReadMultiFieldUnsigned.setChain(ReadUnsigned);
    ReadArray.setChain(ReadUnsigned);
    ReadArrayOne.setChain(ReadUnsigned);
    //ReadArrayOneSelf.setChain(ReadUnsigned);
    ReadArrayOneUnsigned.setChain();
    //ReadArraySelf.setChain(ReadUnsigned);
    ReadArrayUnsigned.setChain();
    // Every command that is a subclass of AbstractUpdate could potentially call ReadUnsigned 
    // because of the group guid check in NSAuthentication.signatureAndACLCheck.
    // Add to that all the other commands that call NSAuthentication.signatureAndACLCheck
    // which is most of them.
    Append.setChain(ReadUnsigned);
    AppendList.setChain(ReadUnsigned);
    //AppendListSelf.setChain(ReadUnsigned);
    AppendListUnsigned.setChain(ReadUnsigned);
    AppendListWithDuplication.setChain(ReadUnsigned);
    //AppendListWithDuplicationSelf.setChain(ReadUnsigned);
    AppendListWithDuplicationUnsigned.setChain(ReadUnsigned);
    AppendOrCreate.setChain(ReadUnsigned);
    AppendOrCreateList.setChain(ReadUnsigned);
    //AppendOrCreateListSelf.setChain(ReadUnsigned);
    AppendOrCreateListUnsigned.setChain(ReadUnsigned);
    //AppendOrCreateSelf.setChain(ReadUnsigned);
    AppendOrCreateUnsigned.setChain(ReadUnsigned);
    //AppendSelf.setChain(ReadUnsigned);
    AppendUnsigned.setChain(ReadUnsigned);
    AppendWithDuplication.setChain(ReadUnsigned);
    //AppendWithDuplicationSelf.setChain(ReadUnsigned);
    AppendWithDuplicationUnsigned.setChain(ReadUnsigned);
    Clear.setChain(ReadUnsigned);
    //ClearSelf.setChain(ReadUnsigned);
    ClearUnsigned.setChain(ReadUnsigned);
    Create.setChain(ReadUnsigned);
    CreateEmpty.setChain(ReadUnsigned);
    //CreateEmptySelf.setChain(ReadUnsigned);
    CreateList.setChain(ReadUnsigned);
    //CreateListSelf.setChain(ReadUnsigned);
    //CreateSelf.setChain(ReadUnsigned);
    Remove.setChain(ReadUnsigned);
    RemoveList.setChain(ReadUnsigned);
    //RemoveListSelf.setChain(ReadUnsigned);
    RemoveListUnsigned.setChain(ReadUnsigned);
    //RemoveSelf.setChain(ReadUnsigned);
    RemoveUnsigned.setChain(ReadUnsigned);
    Replace.setChain(ReadUnsigned);
    ReplaceList.setChain(ReadUnsigned);
    //ReplaceListSelf.setChain(ReadUnsigned);
    ReplaceListUnsigned.setChain(ReadUnsigned);
    ReplaceOrCreate.setChain(ReadUnsigned);
    ReplaceOrCreateList.setChain(ReadUnsigned);
    //ReplaceOrCreateListSelf.setChain(ReadUnsigned);
    ReplaceOrCreateListUnsigned.setChain(ReadUnsigned);
    //ReplaceOrCreateSelf.setChain(ReadUnsigned);
    ReplaceOrCreateUnsigned.setChain(ReadUnsigned);
    //ReplaceSelf.setChain(ReadUnsigned);
    ReplaceUnsigned.setChain(ReadUnsigned);
    ReplaceUserJSON.setChain(ReadUnsigned);
    ReplaceUserJSONUnsigned.setChain(ReadUnsigned);
    CreateIndex.setChain(ReadUnsigned);
    Substitute.setChain(ReadUnsigned);
    SubstituteList.setChain(ReadUnsigned);
    //SubstituteListSelf.setChain(ReadUnsigned);
    SubstituteListUnsigned.setChain(ReadUnsigned);
    //SubstituteSelf.setChain(ReadUnsigned);
    SubstituteUnsigned.setChain(ReadUnsigned);
    RemoveField.setChain(ReadUnsigned);
    //RemoveFieldSelf.setChain(ReadUnsigned);
    RemoveFieldUnsigned.setChain(ReadUnsigned);
    Set.setChain(ReadUnsigned);
    //SetSelf.setChain(ReadUnsigned);
    SetFieldNull.setChain(ReadUnsigned);
    //SetFieldNullSelf.setChain(ReadUnsigned);
    //
    Select.setChain();
    SelectGroupLookupQuery.setChain();
    SelectGroupSetupQueryWithGuid.setChain();
    SelectGroupSetupQueryWithGuidAndInterval.setChain();
    SelectGroupSetupQueryWithInterval.setChain();
    SelectNear.setChain();
    SelectWithin.setChain();
    SelectQuery.setChain();
    //
    AddGuid.setChain(LookupGuid, ReplaceUserJSONUnsigned, ReadUnsigned); // what else?
    RemoveGuid.setChain(ReadUnsigned);
    RemoveAccount.setChain(ReadUnsigned);
    SelectGroupSetupQuery.setChain(ReadUnsigned);
    VerifyAccount.setChain(ReplaceUserJSONUnsigned);

    AddAlias.setChain(ReadUnsigned, ReplaceUserJSONUnsigned);
    AddMultipleGuids.setChain(ReadUnsigned);
    AddMultipleGuidsFast.setChain(ReadUnsigned);
    AddMultipleGuidsFastRandom.setChain(ReadUnsigned);
    // Fixme: Some inconsistencies in the way these account commands are implmented
    // insofar as whether they go remote or not.
    LookupAccountRecord.setChain();
    LookupRandomGuids.setChain();
    LookupGuid.setChain();
    LookupPrimaryGuid.setChain(ReadUnsigned);
    LookupGuidRecord.setChain();
    RegisterAccount.setChain(ReadUnsigned);
    RegisterAccountUnsigned.setChain(ReadUnsigned);
    ResendAuthenticationEmail.setChain();
    RemoveAlias.setChain(ReadUnsigned, ReplaceUserJSONUnsigned);
    RemoveGuidNoAccount.setChain(ReadUnsigned, ReplaceUserJSONUnsigned);
    RetrieveAliases.setChain(ReadUnsigned);
    SetPassword.setChain(ReadUnsigned);
    ResetKey.setChain(ReadUnsigned);
    //
    AclAdd.setChain(ReadUnsigned);
    AclAddSelf.setChain(ReadUnsigned);
    AclRemoveSelf.setChain(ReadUnsigned);
    AclRetrieveSelf.setChain(ReadUnsigned);
    AclRetrieve.setChain(ReadUnsigned);
    AclRemove.setChain(ReadUnsigned);
    //
    AddMembersToGroup.setChain(AppendListUnsigned);
    AddMembersToGroupSelf.setChain(AppendListUnsigned);
    AddToGroup.setChain(AppendListUnsigned);
    AddToGroupSelf.setChain(ReadUnsigned);
    GetGroupMembers.setChain(ReadUnsigned);
    GetGroupMembersSelf.setChain(ReadUnsigned);
    GetGroupsSelf.setChain(ReadUnsigned);
    GetGroups.setChain(ReadUnsigned);
    RemoveFromGroup.setChain(RemoveUnsigned);
    RemoveMembersFromGroup.setChain(RemoveUnsigned);
    RemoveFromGroupSelf.setChain(ReadUnsigned);
    RemoveMembersFromGroupSelf.setChain(ReadUnsigned);
    //
    SetCode.setChain(RemoveUnsigned);
    ClearCode.setChain(RemoveUnsigned);
    GetCode.setChain(RemoveUnsigned);
    // admin
    Help.setChain();
    HelpTcp.setChain();
    HelpTcpWiki.setChain();
    Admin.setChain();
    Dump.setChain();
    GetParameter.setChain();
    SetParameter.setChain();
    ListParameters.setChain();
    ClearCache.setChain();
    DumpCache.setChain();
    ConnectionCheck.setChain();
    Unknown.setChain();

  }

//  public static Class<?>[] getCommandClassesArray() {
//    return (Class<?>[]) Stream.of(values()).map(CommandType::getCommandClass).toArray();
//  }

  /**
   *
   * @return the command classes
   */
  public static List<Class<?>> getCommandClasses() {
    List<Class<?>> result = new ArrayList<>();
    for (CommandType commandType : values()) {
      result.add(commandType.getCommandClass());
    }
    return result;
    // Android doesn't like Lambdas (there's one hidden here) - 9/16
    //return Stream.of(values()).map(CommandType::getCommandClass).collect(Collectors.toList());
  }

  private static String insertUnderScoresBeforeCapitals(String str) {
    StringBuilder result = new StringBuilder();
    // SKIP THE FIRST CAPITAL
    result.append(str.charAt(0));
    // START AT ONE SO WE SKIP THE FIRST CAPITAL
    for (int i = 1; i < str.length(); i++) {
      if (Character.isUpperCase(str.charAt(i))) {
        result.append("_");
      }
      result.append(str.charAt(i));
    }
    return result.toString();
  }

  private static String generateSwiftConstants() {
    StringBuilder result = new StringBuilder();
    for (CommandType commandType : CommandType.values()) {
      result.append("    public static let ");
      result.append(insertUnderScoresBeforeCapitals(
              commandType.toString()).toUpperCase());
      result.append("\t\t\t\t = ");
      result.append("\"");
      result.append(commandType.toString());
      result.append("\"\n");
    }
    return result.toString();
  }

  /**
   * Append(110, CommandCategory.UPDATE,
   * edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Append.class,
   * GNSCommand.ResultType.NULL, true, false),
   *
   * @return the code as a string
   */
  private static String generateCommandTypeCode() {
    StringBuilder result = new StringBuilder();
    String prefix = "";
    for (CommandType commandType : CommandType.values()) {
      result.append(prefix);
      result.append(commandType.toString());
      try {
        Class<?> clazz = commandType.getCommandClass();
        result.append("(");
        result.append(commandType.getInt());
        result.append(", Type.");
        result.append(commandType.getCategory());
        result.append(", ");
        result.append(commandType.getCommandClass().getName());
        result.append(".class,\n GNSCommand.ResultType.");
        result.append(commandType.getResultType());
        result.append(", ");
        result.append(commandType.isCanBeSafelyCoordinated());
        result.append(", ");
        result.append(commandType.isNotForRogueClients());
        result.append(",\n\"");
        Method descriptionMethod = clazz.getMethod("getCommandDescription");
        Constructor<?> constructor = clazz.getConstructor(CommandModule.class
        );
        result.append(descriptionMethod.invoke(constructor.newInstance(new CommandModule())));
        result.append("\",\n");
        Method parametersMethod = clazz.getMethod("getCommandParameters");
        constructor
                = clazz.getConstructor(CommandModule.class
                );
        result.append(arrayForCode((String[]) parametersMethod.invoke(constructor.newInstance(new CommandModule()))));
        result.append(")");
      } catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
        GNSConfig.getLogger().log(Level.WARNING, "Problem: " + e);
      }
      prefix = ", \n";
    }
    return result.toString();
  }

  private static String arrayForCode(String[] array) {
    StringBuilder result = new StringBuilder();
    result.append("new String[]{");
    String prefix = "";
    for (String string : array) {
      result.append(prefix);
      result.append("GNSCommandProtocol.");
      result.append(string.toUpperCase());
      result.append("");
      prefix = ", \n";
    }
    result.append("}");
    return result.toString();
  }

  private static String generateEmptySetChains() {
    StringBuilder result = new StringBuilder();
    for (CommandType commandType : CommandType.values()) {
      if (commandType.invokedCommands == null) {
        result.append("    ");
        result.append(commandType.name());
        result.append(".setChain(ReadUnsigned);");
        result.append("\n");
      }
    }
    return result.toString();
  }

  private static void enforceChecks() {
    HashSet<CommandType> curLevel, nextLevel = new HashSet<CommandType>(), cumulative = new HashSet<CommandType>();

    for (CommandType ctype : CommandType.values()) {
      curLevel = new HashSet<CommandType>(Arrays.asList(ctype));
      nextLevel.clear();
      cumulative.clear();

      while (!curLevel.isEmpty()) {
        nextLevel.clear();

        for (CommandType curLevelType : curLevel) {
          if (curLevelType.invokedCommands != null) {
            nextLevel.addAll(new HashSet<CommandType>(Arrays.asList(curLevelType.invokedCommands)));
          } else {
            System.out.println("!!!!! Need to add " + curLevelType.name() + ".setChain(); !!!!!");
          }
        }
        curLevel = nextLevel;
        cumulative.addAll(nextLevel);
        if (curLevel.size() > 256) {
          Util.suicide("Likely cycle in chain for command " + ctype);
        }

        GNSConfig.getLogger().log(Level.FINE,
                "{0} expanding next level {1}",
                new Object[]{ctype, nextLevel});
      }
      GNSConfig.getLogger().log(Level.INFO,
              "Cumulative chain for {0} = {1}",
              new Object[]{ctype, cumulative});

      for (CommandType downstream : cumulative) {
        if ((ctype.isUpdate() && downstream.isCoordinated())) {
          throw new RuntimeException("Coordinated " + ctype
                  + " is invoking another coordinated command "
                  + downstream);

        }
      }

    }
  }

  /**
   * The CommandTypeTest.
   */
  public static class CommandTypeTest extends DefaultTest {

    /**
     * Enforce checks.
     */
    @Test
    public void enforceChecks() {
      CommandType.enforceChecks();
    }
  }

  /**
   *
   * @param args
   */
  public static void main(String args[]) {
    CommandType.enforceChecks();
    //System.out.println(generateEmptySetChains());
    //System.out.println(generateSwiftConstants());
    //System.out.println(generateCommandTypeCode());
  }
}
