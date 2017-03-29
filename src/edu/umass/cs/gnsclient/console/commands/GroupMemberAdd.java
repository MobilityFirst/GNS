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

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.console.ConsoleModule;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.utils.StringUtil;
import java.io.IOException;

/**
 * Command to add a new member to a group GUID
 */
public class GroupMemberAdd extends ConsoleCommand {

  /**
   * Creates a new <code>GroupMemberAdd</code> object
   *
   * @param module
   */
  public GroupMemberAdd(ConsoleModule module) {
    super(module);
  }

  @Override
  public String getCommandDescription() {
    return "Add a new member to a group GUID (the current GUID/alias must have permissions to change group membership in the group GUID)";
  }

  @Override
  public String getCommandName() {
    return "group_member_add";
  }

  @Override
  public String getCommandParameters() {
    return "[group_guid_or_alias] guid_to_add";
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
      String groupGuid;
      switch (st.countTokens()) {
        case 1:
          groupGuid = module.getCurrentGuid().getGuid();
          break;
        case 2:
          groupGuid = st.nextToken();
          if (!StringUtil.isValidGuidString(groupGuid)) {
            // We probably have an alias, lookup the GUID
            groupGuid = gnsClient.lookupGuid(groupGuid);
          }
          break;
        default:
          wrongArguments();
          return;
      }
      String guidToAdd = st.nextToken();

      gnsClient.groupAddGuid(groupGuid, guidToAdd, module.getCurrentGuid());
      printString("GUID " + guidToAdd + " added to group " + groupGuid + "\n");
    } catch (IOException | ClientException e) {
      printString("Failed to access GNS ( " + e + ")\n");
    }
  }
}
