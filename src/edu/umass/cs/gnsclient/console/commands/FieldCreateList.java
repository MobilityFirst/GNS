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
import java.io.IOException;

/**
 * Command to create a new field in the GNS
 */
public class FieldCreateList extends ConsoleCommand {

  /**
   * Creates a new <code>FieldCreateList</code> object
   *
   * @param module
   */
  public FieldCreateList(ConsoleModule module) {
    super(module);
  }

  @Override
  public String getCommandDescription() {
    return "Create a new field with an initial value for the current GUID/alias)";
  }

  @Override
  public String getCommandName() {
    return "field_create_list";
  }

  @Override
  public String getCommandParameters() {
    return "field_to_create [initial_value]";
  }

  /**
   * Override execute to check for existing connectivity
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
    try {
      StringTokenizer st = new StringTokenizer(commandText.trim());
      if ((st.countTokens() != 1) && (st.countTokens() != 2)) {
        wrongArguments();
        return;
      }
      String field = st.nextToken();
      String value = "";
      if (st.hasMoreTokens()) {
        value = st.nextToken();
      }

      GNSClientCommands gnsClient = module.getGnsClient();
      gnsClient.fieldCreateList(module.getCurrentGuid().getGuid(), field, new JSONArray().put(value),
              module.getCurrentGuid());
      console.printString("New field " + field + " created with value '" + value + "'");
      console.printNewline();
    } catch (IOException | ClientException e) {
      console.printString("Failed to access GNS ( " + e + ")\n");
    }
  }
}
