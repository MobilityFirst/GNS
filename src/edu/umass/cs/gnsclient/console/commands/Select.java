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

import java.util.StringTokenizer;

import org.json.JSONArray;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.console.ConsoleModule;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import java.io.IOException;

/**
 * Select records in the GNS
 */
public class Select extends ConsoleCommand {

  /**
   * Creates a new <code>Select</code> object
   *
   * @param module
   */
  public Select(ConsoleModule module) {
    super(module);
  }

  @Override
  public String getCommandDescription() {
    return "Returns all records that have a field that contains the given value";
  }

  @Override
  public String getCommandName() {
    return "select";
  }

  @Override
  public String getCommandParameters() {
    return "[alias] field value";
  }

  /**
   * Override execute to not check for existing connectivity
   *
   * @throws java.lang.Exception
   */
  @Override
  public void execute(String commandText) throws Exception {
    parse(commandText);
  }

  @Override
  public void parse(String commandText) throws Exception {
    try {
      GNSClientCommands gnsClient = module.getGnsClient();

      StringTokenizer st = new StringTokenizer(commandText.trim());
      GuidEntry guidEntry;
      switch (st.countTokens()) {
        case 2:
          guidEntry = null;
          break;
        case 3:
          String alias = st.nextToken();
          guidEntry = KeyPairUtils.getGuidEntry(module.getGnsInstance(), alias);
          if (guidEntry == null) {
            console.printString("Unknown alias " + alias);
            console.printNewline();
            return;
          }
          break;
        default:
          wrongArguments();
          return;
      }
      String field = st.nextToken();

      String value = st.nextToken();
      JSONArray result;
      if (guidEntry != null) {
        result = gnsClient.select(guidEntry, field, value);
      } else {
        result = gnsClient.select(field, value);
      }
      console.printString(result.toString());
      console.printNewline();
    } catch (IOException | ClientException e) {
      console.printString("Failed to access GNS ( " + e + ")\n");
    }
  }
}
