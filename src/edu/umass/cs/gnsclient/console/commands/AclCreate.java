/*
 *
 *  Copyright (c) 2017 University of Massachusetts
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
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsclient.console.commands;

import java.util.StringTokenizer;
import edu.umass.cs.gnscommon.AclAccessType;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.console.ConsoleModule;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import java.io.IOException;

/**
 * Create an empty ACL for the field of the current GUID
 */
public class AclCreate extends ConsoleCommand {

  /**
   * Creates a new <code>AclCreate</code> object
   *
   * @param module
   */
  public AclCreate(ConsoleModule module) {
    super(module);
  }

  @Override
  public String getCommandDescription() {
    return "Create an empty read or write ACL for the given field in the current GUID (using the credential of the current GUID/alias). "
            + "Use +ALL+ as the field name to delete the ACL for all fields. ";
  }

  @Override
  public String getCommandName() {
    return "acl_create";
  }

  @Override
  public String getCommandParameters() {
    return "field read|write";
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
      if (st.countTokens() != 2) {
        wrongArguments();
        return;
      }
      String field = st.nextToken();
      boolean isWrite = "write".equalsIgnoreCase(st.nextToken());

      // Set ACL
      gnsClient.fieldCreateAcl(isWrite ? AclAccessType.WRITE_WHITELIST
              : AclAccessType.READ_WHITELIST, module.getCurrentGuid(),
              field);
      console.printString((isWrite ? "Write" : "Read") + " ACL for field " + field + " has been created");
      console.printNewline();
    } catch (IOException | ClientException e) {
      console.printString("Failed to access GNS ( " + e + ")\n");
    }
  }
}
