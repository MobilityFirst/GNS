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
import edu.umass.cs.gnsserver.main.GNSConfig;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import org.junit.Assert;

/**
 * All the commands supported by the GNS server are listed here.
 *
 * Each one of these has a corresponding method in an array defined in
 * edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandDefs
 */
public enum CommandType {
  /**
   *
   */
  Append(110, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Append.class,
          CommandResultType.NULL, true, false,
          "Appends the value onto the field for the given GUID. "
          + "Treats the list as a set, removing duplicates. "
          + "Field must be writeable by the WRITER guid.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.VALUE.toString(),
            GNSProtocol.WRITER.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()}, new String[]{}),
  /**
   *
   */
  AppendList(111, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendList.class,
          CommandResultType.NULL, true, false,
          "Appends the value onto of this field for the given GUID. "
          + "Value is a list of items formated as a JSON list. "
          + "Field must be writeable by the WRITER guid.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.VALUE.toString(),
            GNSProtocol.WRITER.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()}, new String[]{}),
  /**
   *
   */
  AppendListUnsigned(113, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendListUnsigned.class,
          CommandResultType.NULL, true, false,
          "Appends the value onto of this field for the given GUID. "
          + "Value is a list of items formated as a JSON list. "
          + "Field must be world writeable as this command does not specify "
          + "the writer and is not signed.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.VALUE.toString()}, new String[]{}),
  /**
   *
   */
  AppendListWithDuplication(114, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendListWithDuplication.class,
          CommandResultType.NULL, true, false,
          "Appends the values onto this field for the given GUID. "
          + "Treats the list as a list, allows dupicates. "
          + "Value is a list of items formated as a JSON list. "
          + "Field must be writeable by the WRITER guid.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.VALUE.toString(),
            GNSProtocol.WRITER.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()}, new String[]{}),
  /**
   *
   */
  AppendListWithDuplicationUnsigned(116, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendListWithDuplicationUnsigned.class,
          CommandResultType.NULL, true, false,
          "Appends the values onto of this field for the given GUID. "
          + "Treats the list as a list, allows dupicate. "
          + "Value is a list of items formated as a JSON list. "
          + "Field must be world writeable as this command does not specify the writer and is not signed.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.VALUE.toString()}, new String[]{}),
  /**
   *
   */
  AppendOrCreate(120, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendOrCreate.class,
          CommandResultType.NULL, true, false,
          "Adds a field to the GNS for the given guid if it doesn't not exist "
          + "otherwise append value onto existing value. "
          + "Field must be writeable by the WRITER guid.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.VALUE.toString(),
            GNSProtocol.WRITER.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()}, new String[]{}),
  /**
   *
   */
  AppendOrCreateList(121, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendOrCreateList.class,
          CommandResultType.NULL, true, false,
          "Adds a field to the GNS for the given guid if it doesn not exist "
          + "otherwise appends values onto existing value. "
          + "Value is a list of items formated as a JSON list. Field must be writeable by the WRITER guid.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.VALUE.toString(),
            GNSProtocol.WRITER.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()}, new String[]{}),
  /**
   *
   */
  AppendOrCreateListUnsigned(123, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendOrCreateListUnsigned.class,
          CommandResultType.NULL, true, false,
          "Adds a field to the GNS for the given guid if it doesn not exist "
          + "otherwise appends values onto existing value. "
          + "Value is a list of items formated as a JSON list. "
          + "Field must be world writeable as this command does not specify the "
          + "writer and is not signed.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.VALUE.toString()}, new String[]{}),
  /**
   *
   */
  AppendOrCreateUnsigned(125, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendOrCreateUnsigned.class,
          CommandResultType.NULL, true, false,
          "Adds a field to the GNS for the given guid if it doesn't not exist "
          + "otherwise append value onto existing value. "
          + "Field must be world writeable as this command does not specify "
          + "the writer and is not signed.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.VALUE.toString()}, new String[]{}),
  /**
   *
   */
  AppendUnsigned(131, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendUnsigned.class,
          CommandResultType.NULL, true, false,
          "Appends the value onto the field for the given GUID. "
          + "Treats the list as a set, removing duplicates."
          + "Field must be world writeable as this command does not specify the writer and is not signed.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.VALUE.toString()}, new String[]{}),
  /**
   *
   */
  AppendWithDuplication(132, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendWithDuplication.class,
          CommandResultType.NULL, true, false,
          "Appends the values onto this field for the given GUID. "
          + "Treats the list as a list, allows dupicates. "
          + "Field must be writeable by the WRITER guid.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.VALUE.toString(),
            GNSProtocol.WRITER.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()}, new String[]{}),
  /**
   *
   */
  AppendWithDuplicationUnsigned(134, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.AppendWithDuplicationUnsigned.class,
          CommandResultType.NULL, true, false,
          "Appends the values onto this field for the given GUID. "
          + "Treats the list as a list, allows dupicates. "
          + "Field must be world writeable as this command does not specify the writer and is not signed.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.VALUE.toString()}, new String[]{}),
  /**
   *
   */
  Clear(140, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Clear.class,
          CommandResultType.NULL, true, false,
          "Clears the field from the GNS for the given GUID. "
          + "Field must be writeable by the WRITER guid.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.WRITER.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()}, new String[]{}),
  /**
   *
   */
  ClearUnsigned(142, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ClearUnsigned.class,
          CommandResultType.NULL, true, false,
          "Clears the field from the GNS for the given guid after authenticating that "
          + "GUID making request has access authority. "
          + "Field must be world writeable as this command does not specify "
          + "the writer and is not signed.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.WRITER.toString()}, new String[]{}),
  /**
   *
   */
  Create(150, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Create.class,
          CommandResultType.NULL, true, false,
          "Adds a field to the GNS for the given GUID. Field must be writeable by the WRITER guid.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()},
          //optional parameters
          new String[]{GNSProtocol.VALUE.toString(),
            GNSProtocol.WRITER.toString()}),
  /**
   *
   */
  CreateEmpty(151, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.CreateEmpty.class,
          CommandResultType.NULL, true, false,
          "Adds an empty field to the GNS for the given GUID. Field must be writeable by the WRITER guid."
          + "A shorthand for Create with an missing value field.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.WRITER.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()},
          new String[]{}),
  /**
   *
   */
  CreateList(153, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.CreateList.class,
          CommandResultType.NULL, true, false,
          "Adds a field to the GNS for the given GUID. Value is a list of items "
          + "formated as a JSON list. Field must be writeable by the WRITER guid.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.VALUE.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()},
          // Optional parameters
          new String[]{GNSProtocol.WRITER.toString()}),
  /**
   *
   */
  Read(160, CommandCategory.READ, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Read.class,
          CommandResultType.MAP, true, false,
          "Returns the value for the key from the GNS for the given guid after authenticating that "
          + "READER making request has access authority. "
          + "Field can use dot notation to access subfields. "
          + "Specify +ALL+ as the <field> to return all fields as a JSON object.",
          new String[]{GNSProtocol.GUID.toString()},
          // Optional parameters
          new String[]{GNSProtocol.FIELD.toString(),
            GNSProtocol.FIELDS.toString(),
            GNSProtocol.READER.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()}),
  /**
   *
   */
  ReadSecured(161, CommandCategory.READ, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.secured.ReadSecured.class,
          CommandResultType.MAP, true, false,
          "Returns the value for the key from the GNS for the given guid after authenticating that "
          + "READER making request has access authority. "
          + "Field can use dot notation to access subfields. "
          + "Specify +ALL+ as the <field> to return all fields as a JSON object. "
          + "Sent on the mutual auth channel. "
          + "Can only be sent from a client that has the correct ssl keys.",
          new String[]{GNSProtocol.GUID.toString()},
          // Optional parameters
          new String[]{GNSProtocol.FIELD.toString(),
            GNSProtocol.FIELDS.toString()},
          CommandFlag.MUTUAL_AUTH // This is important - without this the command isn't secure.
  ),
  /**
   *
   */
  ReadUnsigned(162, CommandCategory.READ, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReadUnsigned.class,
          CommandResultType.MAP, true, false,
          "Returns the value for the key from the GNS for the given guid. Does not require authentication but "
          + "field must be set to be readable by everyone. "
          + "Specify +ALL+ as the <field> to return all fields. ",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString()}, new String[]{}),
  /**
   *
   */
  ReadMultiField(163, CommandCategory.READ, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReadMultiField.class,
          CommandResultType.MAP, true, false,
          "Returns multiple values for the keys from the GNS for the given guid after "
          + "authenticating that READER making request has access authority. "
          + "Fields can use dot notation to access subfields.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELDS.toString(),
            GNSProtocol.READER.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()}, new String[]{}),
  /**
   *
   */
  ReadMultiFieldUnsigned(164, CommandCategory.READ, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReadMultiFieldUnsigned.class,
          CommandResultType.MAP, true, false,
          "Returns multiple values for the keys from the GNS for the given guid after "
          + "authenticating that READER making request has access authority. "
          + "Fields can use dot notation to access subfields.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELDS.toString()}, new String[]{}),
  /**
   *
   */
  ReadArray(170, CommandCategory.READ, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReadArray.class,
          CommandResultType.LIST, true, false,
          "Returns the value of the field as an array from the GNS for the given guid after authenticating "
          + "that READER making request has access authority. "
          + "Values are always returned as a JSON list. "
          + "Specify +ALL+ as the <field> to return all fields as a JSON object.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString()},
          // optional parameters
          new String[]{GNSProtocol.READER.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()}),
  /**
   *
   */
  ReadArrayOne(171, CommandCategory.READ, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReadArrayOne.class,
          CommandResultType.STRING, true, false,
          "Returns the first value of the field as an array from the GNS for the given guid after authenticating "
          + "that the READER has access authority. Treats the value of field "
          + "as a singleton item and returns that item. "
          + "Specify +ALL+ as the <field> to return all fields. ",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.READER.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()}, new String[]{}),
  /**
   *
   */
  ReadArrayOneUnsigned(173, CommandCategory.READ, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReadArrayOneUnsigned.class,
          CommandResultType.STRING, true, false,
          "Returns the first value of the field as an array from the GNS for the given guid. "
          + "Does not require authentication"
          + " but field must be set to be readable by everyone. "
          + "Treats the value of field as a singleton item and returns that item. "
          + "Specify +ALL+ as the <field> to return all fields. ",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString()}, new String[]{}),
  /**
   *
   */
  ReadArrayUnsigned(175, CommandCategory.READ, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReadArrayUnsigned.class,
          CommandResultType.LIST, true, false,
          "Returns the value of the field as an array from the GNS. Does not require authentication but "
          + "field must be set to be readable by everyone. Values are always returned as a JSON list. "
          + "Specify +ALL+ as the <field> to return all fields. ",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString()}, new String[]{}),
  /**
   *
   */
  Remove(180, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Remove.class,
          CommandResultType.NULL, true, false,
          "Removes the value from the field for the given guid after authenticating that "
          + "WRITER making request has access authority. "
          + "Values are always returned as a JSON list. "
          + "Specify +ALL+ as the <field> to return all fields as a JSON object.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.VALUE.toString(),
            GNSProtocol.WRITER.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()}, new String[]{}),
  /**
   *
   */
  RemoveList(181, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.RemoveList.class,
          CommandResultType.NULL, true, false,
          "Removes all the values from the field for the given GUID. "
          + "Value is a list of items formated as a JSON list. Field must be writeable by the WRITER guid.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.VALUE.toString(),
            GNSProtocol.WRITER.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()}, new String[]{}),
  /**
   *
   */
  RemoveListUnsigned(183, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.RemoveListUnsigned.class,
          CommandResultType.NULL, true, false,
          "Removes all the values from the field for the given GUID. "
          + "Value is a list of items formated as a JSON list. "
          + "Field must be world writeable as this command does not specify the writer and is not signed.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.VALUE.toString()}, new String[]{}),
  /**
   *
   */
  RemoveUnsigned(185, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.RemoveUnsigned.class,
          CommandResultType.NULL, true, false,
          "Removes the value from the field for the given GUID. "
          + "Field must be world writeable as this command does not specify the writer and is not signed.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.VALUE.toString()}, new String[]{}),
  /**
   *
   */
  Replace(190, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Replace.class,
          CommandResultType.NULL, true, false,
          "Replaces the current value for the field from the GNS for the given guid with the given value. "
          + "Field must be writeable by the WRITER guid.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.VALUE.toString(),
            GNSProtocol.WRITER.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()}, new String[]{}),
  /**
   *
   */
  ReplaceList(191, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceList.class,
          CommandResultType.NULL, true, false,
          "Replaces the current value for the field from the GNS for the given guid with the given values. "
          + "Field must be writeable by the WRITER guid.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.VALUE.toString(),
            GNSProtocol.WRITER.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()}, new String[]{}),
  /**
   *
   */
  ReplaceListUnsigned(193, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceListUnsigned.class,
          CommandResultType.NULL, true, false,
          "Replaces the current value value for the field from the GNS for the given guid with the given values. "
          + "Field must be world writeable as this command does not specify the writer and is not signed.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.VALUE.toString()}, new String[]{}),
  /**
   *
   */
  ReplaceOrCreate(210, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceOrCreate.class,
          CommandResultType.NULL, true, false,
          "Adds field with the value to the GNS for the given guid if it doesn not exist otherwise "
          + "replaces the value of the field for the given GUID. "
          + "Field must be writeable by the WRITER guid.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.VALUE.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()},
          // Optional values
          new String[]{GNSProtocol.WRITER.toString()}),
  /**
   *
   */
  ReplaceOrCreateList(211, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceOrCreateList.class,
          CommandResultType.NULL, true, false,
          "Adds a field with the value to the GNS for the given guid if it does not exist otherwise "
          + "replaces the value of this field for the given guid with the value. "
          + "Value is a list of items formated as a JSON list. "
          + "Field must be writeable by the WRITER guid.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.VALUE.toString(),
            GNSProtocol.WRITER.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()},
          new String[]{GNSProtocol.WRITER.toString()}),
  /**
   *
   */
  ReplaceOrCreateListUnsigned(213, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceOrCreateListUnsigned.class,
          CommandResultType.NULL, true, false,
          "Adds a field to the GNS for the given guid if it does not exist otherwise "
          + "replaces the value of this field for the given guid with the value. "
          + "Value is a list of items formated as a JSON list. "
          + "Field must be world writeable as this command does not specify the writer and is not signed.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.VALUE.toString()},
          new String[]{}),
  /**
   *
   */
  ReplaceOrCreateUnsigned(215, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceOrCreateUnsigned.class,
          CommandResultType.NULL, true, false,
          "Adds a field to the GNS for the given guid if it doesn not exist otherwise "
          + "replaces the value of this field for the given GUID. "
          + "Field must be world writeable as this command does not specify the writer and is not signed.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.VALUE.toString()},
          new String[]{}),
  /**
   *
   */
  ReplaceUnsigned(217, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceUnsigned.class,
          CommandResultType.NULL, true, false,
          "Replaces the current value field from the GNS for the given guid. "
          + "Field must be world writeable as this command does not specify the writer and is not signed.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.VALUE.toString()},
          new String[]{}),
  /**
   *
   */
  ReplaceUserJSON(220, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceUserJSON.class,
          CommandResultType.NULL, true, false,
          "Replaces existing fields in JSON record with the given JSONObject's fields. "
          + "Doesn't touch top-level fields that aren't in the given JSONObject.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.USER_JSON.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()},
          new String[]{GNSProtocol.WRITER.toString()}),
  /**
   *
   */
  ReplaceUserJSONUnsigned(221, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.ReplaceUserJSONUnsigned.class,
          CommandResultType.NULL, true, false,
          "Replaces existing fields in JSON record with the given JSONObject's fields. "
          + "Field must be world writeable as this command does "
          + "not specify the writer and is not signed. Doesn't touch top-level "
          + "fields that aren't in the given JSONObject.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.USER_JSON.toString()},
          new String[]{}),
  //Fixme: CREATE_INDEX should be an ADMIN_UPDATE command.

  /**
   *
   */
  CreateIndex(230, CommandCategory.OTHER, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.CreateIndex.class,
          CommandResultType.NULL, true, false,
          "Creates an index for field. The value is a string containing the index type.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.VALUE.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()},
          new String[]{GNSProtocol.WRITER.toString()}),
  /**
   *
   */
  Substitute(231, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Substitute.class,
          CommandResultType.NULL, true, false,
          "Replaces OLD_VALUE with newvalue in the field for the given GUID. "
          + "Field must be writeable by the WRITER guid.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.VALUE.toString(),
            GNSProtocol.OLD_VALUE.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()},
          new String[]{GNSProtocol.WRITER.toString()}),
  /**
   *
   */
  SubstituteList(232, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.SubstituteList.class,
          CommandResultType.NULL, true, false,
          "Replaces OLD_VALUE with newvalue in the field for the given GUID. "
          + "Value is a list of items formated as a JSON list. "
          + "Field must be writeable by the WRITER guid. "
          + "If the new value and old values list are different sizes this does the obvious things and "
          + "stops when it runs out of values on the shorter list.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.VALUE.toString(),
            GNSProtocol.OLD_VALUE.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()},
          new String[]{GNSProtocol.WRITER.toString()}),
  /**
   *
   */
  SubstituteListUnsigned(234, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.SubstituteListUnsigned.class,
          CommandResultType.NULL, true, false,
          "Replaces OLD_VALUE with newvalue in the field for the given GUID. "
          + "Value is a list of items formated as a JSON list. "
          + "Field must be world writeable as this command does not specify the writer and is not signed. "
          + "If the new value and old values list are different sizes this does the obvious things and "
          + "stops when it runs out of values on the shorter list.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.VALUE.toString(),
            GNSProtocol.OLD_VALUE.toString()},
          new String[]{}),
  /**
   *
   */
  SubstituteUnsigned(236, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.SubstituteUnsigned.class,
          CommandResultType.NULL, true, false,
          "Replaces OLD_VALUE with newvalue in the field for the given GUID. "
          + "Field must be world writeable as this command does not specify the writer and is not signed.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.VALUE.toString(),
            GNSProtocol.OLD_VALUE.toString()},
          new String[]{}),
  /**
   *
   */
  RemoveField(240, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.RemoveField.class,
          CommandResultType.NULL, true, false,
          "Removes the field from the GNS for the given guid after authenticating "
          + "that WRITER making request has access authority.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()},
          new String[]{GNSProtocol.WRITER.toString()}),
  /**
   *
   */
  RemoveFieldUnsigned(242, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.RemoveFieldUnsigned.class,
          CommandResultType.NULL, true, false,
          "Removes the field from the GNS for the given guid. "
          + "Field must be world writeable as this command does not specify the writer and is not signed.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.WRITER.toString()},
          new String[]{}),
  /**
   *
   */
  Set(250, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Set.class,
          CommandResultType.NULL, true, false,
          "Replaces element N with newvalue in the field for the given GUID. "
          + "Field must be writeable by the WRITER guid.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.VALUE.toString(),
            GNSProtocol.N.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()},
          new String[]{GNSProtocol.WRITER.toString()}),
  /**
   *
   */
  SetFieldNull(252, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.SetFieldNull.class,
          CommandResultType.NULL, true, false,
          "Sets the field to contain a null value. Field must be writeable by the WRITER guid.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()},
          new String[]{GNSProtocol.WRITER.toString()}),
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
          new String[]{GNSProtocol.FIELD.toString(),
            GNSProtocol.VALUE.toString()},
          // optional parameters
          new String[]{GNSProtocol.GUID.toString(), // the reader
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()}),
  /**
   *
   */
  SelectNear(320, CommandCategory.SELECT, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.SelectNear.class,
          CommandResultType.LIST, false, false,
          "Return the guids of all records that are within max distance of value. Key must be a GeoSpatial field. "
          + "Value is a point specified as a JSONArray string tuple: [LONG, LAT]. Max Distance is in meters. "
          + "Values are returned as a JSON array of guids. "
          + "This command is a shorthand for a mongo $near query.",
          new String[]{GNSProtocol.FIELD.toString(),
            GNSProtocol.NEAR.toString(),
            GNSProtocol.MAX_DISTANCE.toString()},
          // optional parameters
          new String[]{GNSProtocol.GUID.toString(), // the reader
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()}),
  /**
   *
   */
  SelectWithin(321, CommandCategory.SELECT, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.SelectWithin.class,
          CommandResultType.LIST, false, false,
          "Returns the guids of all records that are within value which is a bounding box specified. "
          + "Key must be a GeoSpatial field. "
          + "Bounding box is a nested JSONArray string tuple of paired tuples: [[LONG_BOTTOM_LEFT, LAT_BOTTOM_LEFT],[LONG_UPPER_RIGHT, LAT_UPPER_RIGHT]] "
          + "Values are returned as a JSON array of guids. "
          + "This command is a shorthand for a mongo $geoWithin query.",
          new String[]{GNSProtocol.FIELD.toString(),
            GNSProtocol.WITHIN.toString()},
          // optional parameters
          new String[]{GNSProtocol.GUID.toString(), // the reader
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()}),
  /**
   *
   */
  SelectQuery(322, CommandCategory.SELECT, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.SelectQuery.class,
          CommandResultType.LIST, false, false,
          "Returns the guids of all records that satisfy the query. "
          + "For details see http://gns.name/wiki/index.php/Query_Syntax "
          + "Values are returned as a JSON array of guids.",
          new String[]{GNSProtocol.QUERY.toString()},
          // optional parameters
          new String[]{GNSProtocol.GUID.toString(), // the reader
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()}),
  //
  // Select commands that maintain a group guid
  //

  /**
   *
   */
  SelectGroupLookupQuery(311, CommandCategory.SELECT, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.SelectGroupLookupQuery.class,
          CommandResultType.LIST, false, false,
          "Prototype functionality of a full-fledged Context Name Service. "
          + "Returns all records for a group guid that was previously setup with SelectGroupSetupQuery. "
          + "For details see http://gns.name/wiki/index.php/Query_Syntax "
          + "Values are returned as a JSON array of guids.",
          new String[]{GNSProtocol.ACCOUNT_GUID.toString()},
          // optional parameters
          new String[]{GNSProtocol.GUID.toString(), // the reader
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()}),
  /**
   *
   */
  SelectGroupSetupQuery(312, CommandCategory.SELECT, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.SelectGroupSetupQuery.class,
          CommandResultType.LIST, false, false,
          "Prototype functionality of a full-fledged Context Name Service. "
          + "Initializes a new group guid to automatically update and maintain all records that satisfy the query. "
          + "For details see http://gns.name/wiki/index.php/Query_Syntax "
          + "Values are returned as a JSON array of guids.",
          new String[]{GNSProtocol.ACCOUNT_GUID.toString(),
            GNSProtocol.QUERY.toString()},
          // optional parameters
          new String[]{GNSProtocol.GUID.toString(), // the reader
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()}),
  /**
   *
   */
  SelectGroupSetupQueryWithGuid(313, CommandCategory.SELECT, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.SelectGroupSetupQueryWithGuid.class,
          CommandResultType.LIST, false, false,
          "Prototype functionality of a full-fledged Context Name Service. "
          + "Initializes the given group guid to automatically update and maintain all records that satisfy the query. "
          + "For details see http://gns.name/wiki/index.php/Query_Syntax "
          + "Values are returned as a JSON array of guids.",
          new String[]{GNSProtocol.QUERY.toString(),
            GNSProtocol.ACCOUNT_GUID.toString()},
          // optional parameters
          new String[]{GNSProtocol.GUID.toString(), // the reader
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()}),
  /**
   *
   */
  SelectGroupSetupQueryWithGuidAndInterval(314, CommandCategory.SELECT, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.SelectGroupSetupQueryWithGuidAndInterval.class,
          CommandResultType.LIST, false, false,
          "Prototype functionality of a full-fledged Context Name Service. "
          + "Initializes the group guid to automatically update and maintain all records that satisfy the query. "
          + "Interval is the minimum refresh interval of the query - lookups happening more quickly than this "
          + "interval will retrieve a stale value.For details see http://gns.name/wiki/index.php/Query_Syntax"
          + "Values are returned as a JSON array of guids.",
          new String[]{GNSProtocol.ACCOUNT_GUID.toString(),
            GNSProtocol.QUERY.toString(),
            GNSProtocol.INTERVAL.toString()},
          // optional parameters
          new String[]{GNSProtocol.GUID.toString(), // the reader
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()}),
  /**
   *
   */
  SelectGroupSetupQueryWithInterval(315, CommandCategory.SELECT, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.select.SelectGroupSetupQueryWithInterval.class,
          CommandResultType.LIST, false, false,
          "Prototype functionality of a full-fledged Context Name Service. "
          + "Initializes a new group guid to automatically update and maintain all records that satisfy the query. "
          + "Interval is the minimum refresh interval of the query - lookups happening more "
          + "quickly than this interval will retrieve a stale value. "
          + "For details see http://gns.name/wiki/index.php/Query_Syntax "
          + "Values are returned as a JSON array of guids.",
          new String[]{GNSProtocol.QUERY.toString(),
            GNSProtocol.INTERVAL.toString()},
          // optional parameters
          new String[]{GNSProtocol.GUID.toString(), // the reader
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()}),
  //
  // Account commands
  //

  /**
   *
   */
  AddAlias(410, CommandCategory.CREATE_DELETE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.AddAlias.class,
          CommandResultType.NULL, false, false,
          "Adds a additional human readble name to the account associated with the GUID. "
          + "Must be signed by the guid. Returns +BADGUID+ if the guid has not been registered.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.NAME.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()},
          new String[]{}),
  /**
   *
   */
  AddGuid(411, CommandCategory.CREATE_DELETE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.AddGuid.class,
          CommandResultType.NULL, false, false,
          "Adds a guid to the account associated with the GUID. Must be signed by the guid. "
          + "Returns +BADGUID+ if the guid has not been registered.",
          new String[]{GNSProtocol.NAME.toString(),
            GNSProtocol.GUID.toString(),
            GNSProtocol.PUBLIC_KEY.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()},
          new String[]{}),
  /**
   *
   */
  AddMultipleGuids(412, CommandCategory.CREATE_DELETE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.AddMultipleGuids.class,
          CommandResultType.NULL, false, false,
          "Creates multiple guids for the account associated with the account guid. "
          + "Must be signed by the account guid. Returns +BADGUID+ if the account guid has not been registered.",
          new String[]{GNSProtocol.NAMES.toString(),
            GNSProtocol.GUID.toString(),
            GNSProtocol.PUBLIC_KEYS.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()},
          new String[]{}),
  /**
   *
   */
  AddMultipleGuidsFast(413, CommandCategory.CREATE_DELETE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.AddMultipleGuidsFast.class,
          CommandResultType.NULL, false, false,
          "Creates multiple guids for the account associated with the account guid. "
          + "Must be signed by the account guid. The created guids can only be accessed "
          + "using the account guid because they have no private key info stored on the client. "
          + "Returns +BADGUID+ if the account guid has not been registered.",
          new String[]{GNSProtocol.NAMES.toString(),
            GNSProtocol.GUID.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()},
          new String[]{}),
  /**
   *
   */
  AddMultipleGuidsFastRandom(414, CommandCategory.CREATE_DELETE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.AddMultipleGuidsFastRandom.class,
          CommandResultType.NULL, false, false,
          "Creates multiple guids for the account associated with the account guid. "
          + "Must be signed by the account guid. The created guids can only be accessed using the "
          + "account guid because the have no private key info stored on the client."
          + "Returns +BADGUID+ if the account guid has not been registered.",
          new String[]{GNSProtocol.GUIDCNT.toString(),
            GNSProtocol.GUID.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()},
          new String[]{}),
  /**
   *
   */
  LookupAccountRecord(420, CommandCategory.READ, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.LookupAccountRecord.class,
          CommandResultType.MAP, true, false,
          "Returns the account info associated with the given GUID. "
          + "Returns +BADGUID+ if the guid has not been registered.",
          new String[]{GNSProtocol.GUID.toString()},
          new String[]{}),
  /**
   *
   */
  LookupRandomGuids(421, CommandCategory.READ, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.LookupRandomGuids.class,
          CommandResultType.LIST, true, false,
          "Returns some number of random subguids from and account guid. Only for testing purposes. "
          + "Returns +BADGUID+ if the guid has not been registered.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.GUIDCNT.toString()},
          new String[]{}),
  /**
   *
   */
  LookupGuid(422, CommandCategory.READ, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.LookupGuid.class,
          CommandResultType.STRING, true, false,
          "Returns the guid associated with the name which is the human readable name (alias). "
          + "Returns +BADACCOUNT+ if the name has not been registered.",
          new String[]{GNSProtocol.NAME.toString()},
          new String[]{}),
  /**
   *
   */
  LookupPrimaryGuid(423, CommandCategory.READ, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.LookupPrimaryGuid.class,
          CommandResultType.STRING, true, false,
          "Returns the account guid associated the guid. Returns +BADGUID+ if the guid does not exist.",
          new String[]{GNSProtocol.GUID.toString()},
          new String[]{}),
  /**
   *
   */
  LookupGuidRecord(424, CommandCategory.READ, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.LookupGuidRecord.class,
          CommandResultType.MAP, true, false,
          "Returns human readable name and public key associated with the given GUID. "
          + "Returns +BADGUID+ if the guid has not been registered.",
          new String[]{GNSProtocol.GUID.toString()},
          new String[]{}),
  /**
   *
   */
  RegisterAccount(430, CommandCategory.CREATE_DELETE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.RegisterAccount.class,
          CommandResultType.NULL, false, false,
          "Creates an account guid associated with the human readable name and the supplied public key. "
          + "Must be signed with the public key. Returns a guid.",
          new String[]{GNSProtocol.NAME.toString(),
            GNSProtocol.PUBLIC_KEY.toString(),
            GNSProtocol.PASSWORD.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()},
          new String[]{}),
  /**
   *
   */
  RegisterAccountSecured(431, CommandCategory.CREATE_DELETE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.secured.RegisterAccountSecured.class,
          CommandResultType.NULL, false, false,
          "Creates an account guid associated with the human readable name and the supplied public key. "
          + "Returns a guid. "
          + "Sent on the mutual auth channel. "
          + "Can only be sent from a client that has the correct ssl keys.",
          new String[]{GNSProtocol.NAME.toString(),
            GNSProtocol.PUBLIC_KEY.toString(),
            GNSProtocol.PASSWORD.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()},
          new String[]{},
          CommandFlag.MUTUAL_AUTH // This is important - without this the command isn't secure.
  ),
  /**
   *
   */
  RemoveAccount(440, CommandCategory.CREATE_DELETE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.RemoveAccount.class,
          CommandResultType.NULL, false, false,
          "Removes the account guid associated with the human readable name. "
          + "Must be signed by the guid.",
          new String[]{GNSProtocol.NAME.toString(),
            GNSProtocol.GUID.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()}, new String[]{}),
  /**
   *
   */
  RemoveAlias(441, CommandCategory.CREATE_DELETE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.RemoveAlias.class,
          CommandResultType.NULL, false, false,
          "Removes the alias from the account associated with the GUID. "
          + "Must be signed by the guid. Returns +BADGUID+ if the guid has not been registered.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.NAME.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()}, new String[]{}),
  /**
   *
   */
  RemoveGuid(442, CommandCategory.CREATE_DELETE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.RemoveGuid.class,
          CommandResultType.NULL, false, false,
          "Removes the guid from the account associated with the ACCOUNT_GUID. "
          + "Must be signed by the account guid or the guid if account guid is not provided. "
          + "Returns +BADGUID+ if the guid has not been registered.",
          new String[]{
            GNSProtocol.ACCOUNT_GUID.toString(),
            GNSProtocol.GUID.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()}, new String[]{}),
  /**
   *
   */
  RemoveGuidNoAccount(443, CommandCategory.CREATE_DELETE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.RemoveGuidNoAccount.class,
          CommandResultType.NULL, false, false,
          "Removes the GUID. Must be signed by the guid. Returns +BADGUID+ if the guid has not been registered.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()}, new String[]{}),
  /**
   *
   */
  RetrieveAliases(444, CommandCategory.READ, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.RetrieveAliases.class,
          CommandResultType.LIST, true, false,
          "Retrieves all aliases for the account associated with the GUID. "
          + "Must be signed by the guid. Returns +BADGUID+ if the guid has not been registered.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()}, new String[]{}),
  /**
   *
   */
  RemoveAccountWithPassword(445, CommandCategory.CREATE_DELETE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.secured.RemoveAccountWithPassword.class,
          CommandResultType.NULL, false, false,
          "Removes the account guid associated with the human readable name authorized by the account password.",
          new String[]{GNSProtocol.NAME.toString(), GNSProtocol.PASSWORD.toString()}, new String[]{}),
  /**
   *
   */
  RemoveAccountSecured(446, CommandCategory.CREATE_DELETE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.secured.RemoveAccountSecured.class,
          CommandResultType.NULL, false, false,
          "Removes the account guid associated with the human readable name."
          + "Sent on the mutual auth channel. "
          + "Can only be sent from a client that has the correct ssl keys.",
          new String[]{GNSProtocol.NAME.toString()},
          new String[]{},
          CommandFlag.MUTUAL_AUTH // This is important - without this the command isn't secure.
  ),
  /**
   *
   */
  SetPassword(450, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.SetPassword.class,
          CommandResultType.NULL, true, false,
          "Sets the password. Must be signed by the guid. Returns +BADGUID+ if the guid has not been registered.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.PASSWORD.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()}, new String[]{}),
  /**
   *
   */
  VerifyAccount(451, CommandCategory.OTHER, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.VerifyAccount.class,
          CommandResultType.STRING, true, false,
          "Handles the completion of the verification process for a guid by supplying the correct code.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.CODE.toString()}, new String[]{}),
  /**
   *
   */
  ResendAuthenticationEmail(452, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.ResendAuthenticationEmail.class,
          CommandResultType.NULL, true, false,
          "Resends the verification code email to the user. Must be signed by the guid. Returns +BADGUID+ if the guid has not been registered.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()}, new String[]{}),
  /**
   *
   */
  ResetKey(460, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account.ResetKey.class,
          CommandResultType.NULL, true, false,
          "Resets the publickey for the account guid.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.PUBLIC_KEY.toString(),
            GNSProtocol.PASSWORD.toString()}, new String[]{}),
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
          + "Field can be also be +ALL+ which means all fields can be read by the accessor. ",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.ACCESSER.toString(),
            GNSProtocol.ACL_TYPE.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()},
          // optional parameters
          new String[]{GNSProtocol.WRITER.toString()}),
  /**
   *
   */
  AclAddSecured(520, CommandCategory.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.secured.AclAddSecured.class,
          CommandResultType.NULL, true, false,
          "Updates the access control list of the given GUID's field to include the accesser guid. Accessor guid can "
          + "be guid or group guid or +ALL+ which means anyone. "
          + "Field can be also be +ALL+ which means all fields can be read by the accessor. "
          + "Sent on the mutual auth channel. "
          + "Can only be sent from a client that has the correct ssl keys.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.ACCESSER.toString(),
            GNSProtocol.ACL_TYPE.toString()},
          new String[]{},
          CommandFlag.MUTUAL_AUTH // This is important - without this the command isn't secure.
  ),
  /**
   *
   */
  AclAddSelf(511, CommandCategory.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.acl.AclAddSelf.class,
          CommandResultType.NULL, true, false,
          "Updates the access control list of the given GUID's field to include the accesser guid. "
          + "Accessor should a guid or group guid or +ALL+ which means anyone. "
          + "Field can be also be +ALL+ which means all fields can be read by the accessor. ",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.ACCESSER.toString(),
            GNSProtocol.ACL_TYPE.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()}, new String[]{}),
  /**
   *
   */
  AclRemove(512, CommandCategory.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.acl.AclRemove.class,
          CommandResultType.NULL, true, false,
          "Updates the access control list of the given GUID's field to remove the accesser guid. "
          + "Accessor should be the guid or group guid to be removed.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.ACCESSER.toString(),
            GNSProtocol.ACL_TYPE.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()},
          // optional parameters
          new String[]{GNSProtocol.WRITER.toString()}),
  /**
   *
   */
  AclRemoveSecured(522, CommandCategory.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.secured.AclRemoveSecured.class,
          CommandResultType.NULL, true, false,
          "Updates the access control list of the given GUID's field to remove the accesser guid. "
          + "Accessor should be the guid or group guid to be removed. "
          + "Sent on the mutual auth channel. "
          + "Can only be sent from a client that has the correct ssl keys.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.ACCESSER.toString(),
            GNSProtocol.ACL_TYPE.toString()},
          new String[]{},
          CommandFlag.MUTUAL_AUTH // This is important - without this the command isn't secure.
  ),
  /**
   *
   */
  AclRemoveSelf(513, CommandCategory.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.acl.AclRemoveSelf.class,
          CommandResultType.NULL, true, false,
          "Updates the access control list of the given GUID's field to remove the accesser guid. "
          + "Accessor should be the guid or group guid to be removed.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.ACCESSER.toString(),
            GNSProtocol.ACL_TYPE.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()},
          new String[]{}),
  /**
   *
   */
  AclRetrieve(514, CommandCategory.READ,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.acl.AclRetrieve.class,
          CommandResultType.LIST, true, false,
          "Returns the access control list for a guids's field.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.ACL_TYPE.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()},
          // optional parameters
          new String[]{GNSProtocol.READER.toString()}),
  /**
   *
   */
  AclRetrieveSecured(524, CommandCategory.READ,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.secured.AclRetrieveSecured.class,
          CommandResultType.LIST, true, false,
          "Returns the access control list for a guids's field. "
          + "Sent on the mutual auth channel. "
          + "Can only be sent from a client that has the correct ssl keys.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.ACL_TYPE.toString()},
          new String[]{},
          CommandFlag.MUTUAL_AUTH // This is important - without this the command isn't secure.
  ),
  /**
   *
   */
  AclRetrieveSelf(515, CommandCategory.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.acl.AclRetrieveSelf.class,
          CommandResultType.LIST, true, false,
          "Returns the access control list for a guids's field.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.ACL_TYPE.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()}, new String[]{}),
  /**
   *
   */
  FieldCreateAcl(516, CommandCategory.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.acl.FieldCreateAcl.class,
          CommandResultType.NULL, true, false,
          "Creates an empty ACL.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.ACL_TYPE.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()},
          // optional parameters
          new String[]{GNSProtocol.WRITER.toString()}),
  /**
   *
   */
  FieldDeleteAcl(517, CommandCategory.UPDATE,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.acl.FieldDeleteAcl.class,
          CommandResultType.NULL, true, false,
          "Deletes an ACL.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.ACL_TYPE.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()},
          // optional parameters
          new String[]{GNSProtocol.WRITER.toString()}),
  /**
   *
   */
  FieldAclExists(518, CommandCategory.READ,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.acl.FieldAclExists.class,
          CommandResultType.BOOLEAN, true, false,
          "Returns \"true\" if the ACL exists \"false\" otherwise.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.FIELD.toString(),
            GNSProtocol.ACL_TYPE.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()},
          // Optional paramaters
          new String[]{GNSProtocol.READER.toString()}),
  //
  // Group Commands
  //

  /**
   *
   */
  AddMembersToGroup(610, CommandCategory.OTHER,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.AddMembersToGroup.class,
          CommandResultType.NULL, false, false,
          "Adds the member guids to the group specified by guid. "
          + "Writer guid needs to have write access and sign the command.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.MEMBERS.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()},
          // optional parameters
          new String[]{GNSProtocol.WRITER.toString()}),
  //  /**
  //   *
  //   */
  //  AddMembersToGroupSelf(611, CommandCategory.OTHER,
  //          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.AddMembersToGroupSelf.class,
  //          CommandResultType.NULL, false, false,
  //          "Adds the member guids to the group specified by guid. "
  //          + "Writer guid needs to have write access and sign the command.",
  //          new String[]{GNSProtocol.GUID.toString(),
  //            GNSProtocol.MEMBERS.toString(),
  //            GNSProtocol.SIGNATURE.toString(),
  //            GNSProtocol.SIGNATUREFULLMESSAGE.toString()}, new String[]{}),
  /**
   *
   */
  AddToGroup(612, CommandCategory.OTHER,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.AddToGroup.class,
          CommandResultType.NULL, false, false,
          "Adds the member guid to the group specified by guid. "
          + "Writer guid needs to have write access and sign the command.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.MEMBER.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()},
          // optional parameters
          new String[]{GNSProtocol.WRITER.toString()}),
  //  /**
  //   *
  //   */
  //  AddToGroupSelf(613, CommandCategory.UPDATE,
  //          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.AddToGroupSelf.class,
  //          CommandResultType.NULL, false, false,
  //          "Adds the member guid to the group specified by guid.",
  //          new String[]{GNSProtocol.GUID.toString(),
  //            GNSProtocol.MEMBER.toString(),
  //            GNSProtocol.SIGNATURE.toString(),
  //            GNSProtocol.SIGNATUREFULLMESSAGE.toString()}, new String[]{}),
  /**
   *
   */
  GetGroupMembers(614, CommandCategory.READ, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.GetGroupMembers.class,
          CommandResultType.LIST, true, false,
          "Returns the members of the group formatted as a JSON Array. "
          + "Reader guid needs to have read access and sign the command.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()},
          // optional parameters
          new String[]{GNSProtocol.READER.toString()}),
  //  /**
  //   *
  //   */
  //  GetGroupMembersSelf(615, CommandCategory.READ, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.GetGroupMembersSelf.class,
  //          CommandResultType.LIST, true, false,
  //          "Returns the members of the group formatted as a JSON Array.",
  //          new String[]{GNSProtocol.GUID.toString(),
  //            GNSProtocol.SIGNATURE.toString(),
  //            GNSProtocol.SIGNATUREFULLMESSAGE.toString()}, new String[]{}),
  /**
   *
   */
  GetGroups(616, CommandCategory.READ, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.GetGroups.class,
          CommandResultType.LIST, true, false,
          "Returns the groups that a guid is a member of formatted as a JSON Array. "
          + "Reader guid needs to have read access and sign the command.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()},
          // optional parameters
          new String[]{GNSProtocol.READER.toString()}),
  //  /**
  //   *
  //   */
  //  GetGroupsSelf(617, CommandCategory.READ, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.GetGroupsSelf.class,
  //          CommandResultType.LIST, true, false,
  //          "Returns the groups that a guid is a member of formatted as a JSON Array.",
  //          new String[]{GNSProtocol.GUID.toString(),
  //            GNSProtocol.SIGNATURE.toString(),
  //            GNSProtocol.SIGNATUREFULLMESSAGE.toString()}, new String[]{}),
  /**
   *
   */
  RemoveFromGroup(620, CommandCategory.OTHER, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.RemoveFromGroup.class,
          CommandResultType.NULL, false, false,
          "Removes the member guid from the group specified by guid. "
          + "Writer guid needs to have write access and sign the command.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.MEMBER.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()},
          // optional parameters
          new String[]{GNSProtocol.WRITER.toString()}),
  //  /**
  //   *
  //   */
  //  RemoveFromGroupSelf(621, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.RemoveFromGroupSelf.class,
  //          CommandResultType.NULL, false, false,
  //          "Removes the member guid from the group specified by guid.",
  //          new String[]{GNSProtocol.GUID.toString(),
  //            GNSProtocol.MEMBER.toString(),
  //            GNSProtocol.SIGNATURE.toString(),
  //            GNSProtocol.SIGNATUREFULLMESSAGE.toString()}, new String[]{}),
  /**
   *
   */
  RemoveMembersFromGroup(622, CommandCategory.OTHER, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.RemoveMembersFromGroup.class,
          CommandResultType.NULL, false, false,
          "Removes the member guids from the group specified by guid. "
          + "Writer guid needs to have write access and sign the command.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.MEMBERS.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()},
          // optional parameters
          new String[]{GNSProtocol.WRITER.toString()}),
  //  /**
  //   *
  //   */
  //  RemoveMembersFromGroupSelf(623, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.group.RemoveMembersFromGroupSelf.class,
  //          CommandResultType.NULL, false, false,
  //          "Removes the member guids from the group specified by guid.",
  //          new String[]{GNSProtocol.GUID.toString(),
  //            GNSProtocol.MEMBERS.toString(),
  //            GNSProtocol.SIGNATURE.toString(),
  //            GNSProtocol.SIGNATUREFULLMESSAGE.toString()}, new String[]{}),
  /**
   *
   */
  Help(710, CommandCategory.OTHER, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.Help.class,
          CommandResultType.STRING, true, false,
          "Returns this help message.",
          new String[]{},
          new String[]{},
          CommandFlag.LOCAL),
  /**
   *
   */
  HelpTcp(711, CommandCategory.OTHER, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.HelpTcp.class,
          CommandResultType.STRING, true, false,
          "Returns the help message for TCP commands.",
          new String[]{"tcp"},
          new String[]{},
          CommandFlag.LOCAL),
  /**
   *
   */
  HelpTcpWiki(712, CommandCategory.OTHER, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data.HelpTcpWiki.class,
          CommandResultType.STRING, true, false,
          "Returns the help message for TCP commands in wiki format.",
          new String[]{"tcpwiki"},
          new String[]{},
          CommandFlag.LOCAL),
  /**
   *
   */
  Dump(716, CommandCategory.OTHER, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.Dump.class,
          CommandResultType.STRING, true, true,
          "Returns the contents of the GNS.",
          new String[]{},
          new String[]{},
          CommandFlag.MUTUAL_AUTH, CommandFlag.LOCAL),
  /**
   *
   */
  ConnectionCheck(737, CommandCategory.OTHER, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.ConnectionCheck.class,
          CommandResultType.STRING, true, false,
          "Checks connectivity.",
          new String[]{},
          new String[]{}),
  /**
   *
   */
  SetCode(810, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.activecode.SetCode.class,
          CommandResultType.NULL, true, false,
          "Sets the given active code for the specified guid and action, ensuring the writer has permission.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.WRITER.toString(),
            GNSProtocol.AC_ACTION.toString(),
            GNSProtocol.AC_CODE.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()}, new String[]{}),
  /**
   *
   */
  ClearCode(811, CommandCategory.UPDATE, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.activecode.ClearCode.class,
          CommandResultType.NULL, true, false,
          "Clears the active code for the specified guid and action, ensuring the writer has permission.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.WRITER.toString(),
            GNSProtocol.AC_ACTION.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()}, new String[]{}),
  /**
   *
   */
  GetCode(812, CommandCategory.READ,
          edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.activecode.GetCode.class,
          CommandResultType.STRING, true, false,
          "Returns the active code for the specified action, ensuring the reader has permission.",
          new String[]{GNSProtocol.GUID.toString(),
            GNSProtocol.READER.toString(),
            GNSProtocol.AC_ACTION.toString(),
            GNSProtocol.SIGNATURE.toString(),
            GNSProtocol.SIGNATUREFULLMESSAGE.toString()}, new String[]{}),
  /**
   *
   */
  Unknown(999, CommandCategory.OTHER, edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.admin.Unknown.class,
          CommandResultType.NULL, true, true,
          "A null command.",
          new String[]{},
          new String[]{});

  private final int number;
  private final CommandCategory category;
  private final CommandFlag[] flags;
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
   * The required parameters of the command.
   */
  private final String[] commandRequiredParameters;

  /**
   * The optional parameters of the command.
   */
  private final String[] commandOptionalParameters;

  /**
   * Other commands that may be remote-query-invoked by this command.
   * Non-final only because we can not name enums before they are defined.
   */
  private CommandType[] invokedCommands;

  /**
   * The command category, that is what kind of command it is.
   */
  private enum CommandCategory {
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
    OTHER
  }

  /**
   * Flags that describe additional command properties.
   */
  private enum CommandFlag {
    /**
     *
     */
    MUTUAL_AUTH,
    /**
     * Miscellaneous commands that can be handled by the local server.
     */
    LOCAL
  }

  private CommandType(int number, CommandCategory category, Class<?> commandClass,
          CommandResultType returnType, boolean canBeSafelyCoordinated,
          boolean notForRogueClients, String description,
          String[] requiredParameters, String[] optionalParameters,
          CommandFlag... flags) {
    this.number = number;
    this.category = category;
    this.commandClass = commandClass;
    this.returnType = returnType;

    this.canBeSafelyCoordinated = canBeSafelyCoordinated;

    // presumably every command is currently available to thugs
    this.notForRogueClients = notForRogueClients;

    this.commandDescription = description;
    this.commandRequiredParameters = requiredParameters;
    this.commandOptionalParameters = optionalParameters;
    this.flags = flags;

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
   * @return the required command parameters
   */
  public String[] getCommandRequiredParameters() {
    return commandRequiredParameters;
  }

  /**
   *
   * @return the optional command parameters
   */
  public String[] getCommandOptionalParameters() {
    return commandOptionalParameters;
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
   * @return true if it's mutual auth command
   */
  public boolean isMutualAuth() {
    return Arrays.asList(flags).contains(CommandFlag.MUTUAL_AUTH);
  }

  /**
   *
   * @return true if this is a command any replica can handle itself
   */
  public boolean isLocallyHandled() {
    return isCreateDelete() || isSelect() || Arrays.asList(flags).contains(CommandFlag.LOCAL);
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
    ReadSecured.setChain(ReadUnsigned);
    ReadUnsigned.setChain();
    ReadMultiField.setChain(ReadUnsigned);
    ReadMultiFieldUnsigned.setChain(ReadUnsigned);
    ReadArray.setChain(ReadUnsigned);
    ReadArrayOne.setChain(ReadUnsigned);
    ReadArrayOneUnsigned.setChain();
    ReadArrayUnsigned.setChain();
    // Every command that is a subclass of AbstractUpdate could potentially call ReadUnsigned 
    // because of the group guid check in NSAuthentication.signatureAndACLCheck.
    // Add to that all the other commands that call NSAuthentication.signatureAndACLCheck
    // which is most of them.
    Append.setChain(ReadUnsigned);
    AppendList.setChain(ReadUnsigned);
    AppendListUnsigned.setChain(ReadUnsigned);
    AppendListWithDuplication.setChain(ReadUnsigned);
    AppendListWithDuplicationUnsigned.setChain(ReadUnsigned);
    AppendOrCreate.setChain(ReadUnsigned);
    AppendOrCreateList.setChain(ReadUnsigned);
    AppendOrCreateListUnsigned.setChain(ReadUnsigned);
    AppendOrCreateUnsigned.setChain(ReadUnsigned);
    AppendUnsigned.setChain(ReadUnsigned);
    AppendWithDuplication.setChain(ReadUnsigned);
    AppendWithDuplicationUnsigned.setChain(ReadUnsigned);
    Clear.setChain(ReadUnsigned);
    ClearUnsigned.setChain(ReadUnsigned);
    Create.setChain(ReadUnsigned);
    CreateEmpty.setChain(ReadUnsigned);
    CreateList.setChain(ReadUnsigned);
    Remove.setChain(ReadUnsigned);
    RemoveList.setChain(ReadUnsigned);
    RemoveListUnsigned.setChain(ReadUnsigned);
    RemoveUnsigned.setChain(ReadUnsigned);
    Replace.setChain(ReadUnsigned);
    ReplaceList.setChain(ReadUnsigned);
    ReplaceListUnsigned.setChain(ReadUnsigned);
    ReplaceOrCreate.setChain(ReadUnsigned);
    ReplaceOrCreateList.setChain(ReadUnsigned);
    ReplaceOrCreateListUnsigned.setChain(ReadUnsigned);
    ReplaceOrCreateUnsigned.setChain(ReadUnsigned);
    ReplaceUnsigned.setChain(ReadUnsigned);
    ReplaceUserJSON.setChain(ReadUnsigned);
    ReplaceUserJSONUnsigned.setChain(ReadUnsigned);
    CreateIndex.setChain(ReadUnsigned);
    Substitute.setChain(ReadUnsigned);
    SubstituteList.setChain(ReadUnsigned);
    SubstituteListUnsigned.setChain(ReadUnsigned);
    SubstituteUnsigned.setChain(ReadUnsigned);
    RemoveField.setChain(ReadUnsigned);
    RemoveFieldUnsigned.setChain(ReadUnsigned);
    Set.setChain(ReadUnsigned);
    SetFieldNull.setChain(ReadUnsigned);
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
    RemoveAccountWithPassword.setChain(ReadUnsigned);
    RemoveAccountSecured.setChain(ReadUnsigned);
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
    RegisterAccountSecured.setChain(ReadUnsigned);
    ResendAuthenticationEmail.setChain();
    RemoveAlias.setChain(ReadUnsigned, ReplaceUserJSONUnsigned);
    RemoveGuidNoAccount.setChain(ReadUnsigned, ReplaceUserJSONUnsigned);
    RetrieveAliases.setChain(ReadUnsigned);
    SetPassword.setChain(ReadUnsigned);
    ResetKey.setChain(ReadUnsigned);
    //
    AclAdd.setChain(ReadUnsigned);
    AclAddSecured.setChain(ReadUnsigned);
    AclAddSelf.setChain(ReadUnsigned);
    AclRemoveSelf.setChain(ReadUnsigned);
    AclRetrieveSelf.setChain(ReadUnsigned);
    AclRetrieve.setChain(ReadUnsigned);
    AclRetrieveSecured.setChain(ReadUnsigned);
    AclRemove.setChain(ReadUnsigned);
    AclRemoveSecured.setChain(ReadUnsigned);
    FieldCreateAcl.setChain(ReadUnsigned);
    FieldDeleteAcl.setChain(ReadUnsigned);
    FieldAclExists.setChain(ReadUnsigned);
    //
    AddMembersToGroup.setChain(AppendListUnsigned);
    AddToGroup.setChain(AppendListUnsigned);
    GetGroupMembers.setChain(ReadUnsigned);
    GetGroups.setChain(ReadUnsigned);
    RemoveFromGroup.setChain(RemoveUnsigned);
    RemoveMembersFromGroup.setChain(RemoveUnsigned);
    //
    SetCode.setChain();
    ClearCode.setChain();
    GetCode.setChain();
    // admin
    Help.setChain();
    HelpTcp.setChain();
    HelpTcpWiki.setChain();
    Dump.setChain();
    ConnectionCheck.setChain();
    Unknown.setChain();

  }

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

  private static String generateSwiftStructStaticConstants() {
    StringBuilder result = new StringBuilder();
    result.append("extension CommandType {\n");
    for (CommandType commandType : CommandType.values()) {
      result.append("  static let ");
      result.append(commandType.toString());
      result.append(" = CommandType(\"");
      result.append(commandType.toString());
      result.append("\"");
      result.append(", ");
      result.append(commandType.getInt());
      result.append(")");
      result.append("\n");
    }
    result.append("  static let allValues = [");
    String prefix = "";
    for (CommandType commandType : CommandType.values()) {
      result.append(prefix);
      result.append(commandType.toString());
      prefix = ", ";
    }
    result.append("]\n");
    result.append("}");
    return result.toString();
  }

  /**
   * *
   * Run all checks on Command Type enums.
   */
  public static void enforceChecks() {
    HashSet<CommandType> curLevel, nextLevel = new HashSet<>(), cumulative = new HashSet<>();

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
            Assert.fail("!!!!! Need to add " + curLevelType.name() + ".setChain(); !!!!!");
          }
        }
        curLevel = nextLevel;
        cumulative.addAll(nextLevel);
        if (curLevel.size() > 256) {
          Assert.fail("Likely cycle in chain for command " + ctype);
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
          Assert.fail("Coordinated " + ctype
                  + " is invoking another coordinated command "
                  + downstream);
        }
      }

    }
  }

  /**
   *
   * @param args
   */
  public static void main(String args[]) {
    //CommandType.enforceChecks();
    //System.out.println(generateSwiftEnum());
    //System.out.println(generateSwiftStructStaticConstants());
    //System.out.println(generateEmptySetChains());
    //System.out.println(generateSwiftConstants());
    //System.out.println(generateCommandTypeCode());
  }
}
