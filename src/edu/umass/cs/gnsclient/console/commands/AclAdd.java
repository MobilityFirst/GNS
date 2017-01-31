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

import edu.umass.cs.gnscommon.AclAccessType;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.console.ConsoleModule;

/**
 * Add a GUID to the ACL of a field of the current GUID in the GNS
 */
public class AclAdd extends ConsoleCommand {

  /**
   * Creates a new <code>AclAdd</code> object
   *
   * @param module
   */
  public AclAdd(ConsoleModule module) {
    super(module);
  }

  @Override
  public String getCommandDescription() {
    return "Add a GUID to the read or write ACL for the given field in the current GUID (using the credential of the current GUID/alias). "
            + "Use +ALL+ as the field name to set the ACL on all fields. "
            + "Use +ALL+ in the GUID field to make the field public to everyone.";
  }

  @Override
  public String getCommandName() {
    return "acl_add";
  }

  @Override
  public String getCommandParameters() {
    return "field read|write guid";
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
      if (st.countTokens() != 3) {
        wrongArguments();
        return;
      }
      String field = st.nextToken();
      boolean isWrite = "write".equalsIgnoreCase(st.nextToken());
      String guid = st.nextToken();

      // Set ACL
      gnsClient.aclAdd(isWrite ? AclAccessType.WRITE_WHITELIST : AclAccessType.READ_WHITELIST, module.getCurrentGuid(),
              field, "+ALL+".equals(guid) ? null : guid);

      // Then read ACLs
      JSONArray write = gnsClient.aclGet(isWrite ? AclAccessType.WRITE_WHITELIST : AclAccessType.READ_WHITELIST,
              module.getCurrentGuid(), field, module.getCurrentGuid().getGuid());
      console.printString("New ACL is: " + write.toString());
      console.printNewline();
    } catch (Exception e) {
      console.printString("Failed to access GNS ( " + e + ")\n");
    }
  }
}
