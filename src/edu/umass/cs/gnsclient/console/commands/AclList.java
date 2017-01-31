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

import org.json.JSONArray;

import edu.umass.cs.gnscommon.AclAccessType;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.console.ConsoleModule;
import java.util.StringTokenizer;

/**
 * List the ACLs of a field of the current GUID in the GNS
 */
public class AclList extends ConsoleCommand {

  /**
   * Creates a new <code>AclList</code> object
   *
   * @param module
   */
  public AclList(ConsoleModule module) {
    super(module);
  }

  @Override
  public String getCommandDescription() {
    return "List the current ACLs defined for the given field in the current GUID (using the credential of the current GUID/alias)";
  }

  @Override
  public String getCommandName() {
    return "acl_list";
  }

  @Override
  public String getCommandParameters() {
    return "field";
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
      GNSClientCommands gnsClient = module.getGnsClient();

      StringTokenizer st = new StringTokenizer(commandText.trim());
      if (st.countTokens() != 1) {
        wrongArguments();
        return;
      }
      String field = st.nextToken();

      if (gnsClient.fieldAclExists(AclAccessType.READ_WHITELIST, module.getCurrentGuid(), field)) {
        JSONArray read = gnsClient.aclGet(AclAccessType.READ_WHITELIST, module.getCurrentGuid(), field, module
                .getCurrentGuid().getGuid());
        console.printString("Read ACL: " + read.toString());
        console.printNewline();
      } else {
        console.printString("Read ACL for field " + field + " does not exist");
        console.printNewline();
      }

      // Then write ACLs
      if (gnsClient.fieldAclExists(AclAccessType.WRITE_WHITELIST, module.getCurrentGuid(), field)) {
        JSONArray write = gnsClient.aclGet(AclAccessType.WRITE_WHITELIST, module.getCurrentGuid(), field, module
                .getCurrentGuid().getGuid());
        console.printString("Write ACL: " + write.toString());
        console.printNewline();
      } else {
        console.printString("Write ACL for field " + field + " does not exist");
        console.printNewline();
      }
    } catch (Exception e) {
      console.printString("Failed to access GNS ( " + e + ")\n");
    }
  }
}
