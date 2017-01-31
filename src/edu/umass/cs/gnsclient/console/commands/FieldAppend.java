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
package edu.umass.cs.gnsclient.console.commands;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import java.util.StringTokenizer;

import org.json.JSONArray;

import edu.umass.cs.gnsclient.console.ConsoleModule;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.utils.StringUtil;
import java.io.IOException;

/**
 * Command to update a field in the GNS
 */
public class FieldAppend extends ConsoleCommand {

  /**
   * Creates a new <code>FieldAppend</code> object
   *
   * @param module
   */
  public FieldAppend(ConsoleModule module) {
    super(module);
  }

  @Override
  public String getCommandDescription() {
    return "Append a value in the given field of the target GUID (using the credential of the current GUID/alias)."
            + " Assumes the field is a list. Use in conjunction with field_write_list.";
  }

  @Override
  public String getCommandName() {
    return "field_append";
  }

  @Override
  public String getCommandParameters() {
    return "[target_guid_or_alias] field_to_write value_to_append";
  }

  /**
   * Override execute to check for a selected guid
   *
   * @throws java.lang.Exception
   */
  @Override
  public void execute(String commandText) throws Exception {
    if (!module.isCurrentGuidSetAndVerified()) {
      return;
    }
    super.execute(commandText);
  }

  @Override
  public void parse(String commandText) throws Exception {
    GNSClientCommands gnsClient = module.getGnsClient();
    try {
      StringTokenizer st = new StringTokenizer(commandText.trim());
      String guid;
      switch (st.countTokens()) {
        case 2:
          guid = module.getCurrentGuid().getGuid();
          break;
        case 3:
          guid = st.nextToken();
          if (!StringUtil.isValidGuidString(guid)) {
            // We probably have an alias, lookup the GUID
            guid = gnsClient.lookupGuid(guid);
          }
          break;
        default:
          wrongArguments();
          return;
      }
      String field = st.nextToken();
      String value = st.nextToken();

      gnsClient.fieldAppend(guid, field, new JSONArray().put(value), module.getCurrentGuid());
      console.printString("Value '" + value + "' appended to field " + field + " for GUID " + guid);
      console.printNewline();
    } catch (IOException | ClientException e) {
      console.printString("Failed to access GNS ( " + e + ")\n");
    }
  }
}
