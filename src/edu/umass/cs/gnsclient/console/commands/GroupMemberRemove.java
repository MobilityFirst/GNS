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
import edu.umass.cs.gnscommon.utils.Util;

/**
 * Command to remove a member from a group GUID
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class GroupMemberRemove extends ConsoleCommand
{

  /**
   * Creates a new <code>GroupMemberRemove</code> object
   * 
   * @param module
   */
  public GroupMemberRemove(ConsoleModule module)
  {
    super(module);
  }

  @Override
  public String getCommandDescription()
  {
    return "Remove a member from a group GUID (the current GUID/alias must have permissions to change group membership in the group GUID)";
  }

  @Override
  public String getCommandName()
  {
    return "group_member_remove";
  }

  @Override
  public String getCommandParameters()
  {
    return "[group_guid_or_alias] guid_to_remove";
  }

  /**
   * Override execute to check for existing connectivity
   * @throws java.lang.Exception
   */
  @Override
  public void execute(String commandText) throws Exception
  {
    if (!module.isCurrentGuidSetAndVerified())
    {
      return;
    }
    super.execute(commandText);
  }

  @Override
  public void parse(String commandText) throws Exception
  {
    GNSClientCommands gnsClient = module.getGnsClient();
    try
    {
      StringTokenizer st = new StringTokenizer(commandText.trim());
      String groupGuid;
      if (st.countTokens() == 1)
      {
        groupGuid = module.getCurrentGuid().getGuid();
      }
      else if (st.countTokens() == 2)
      {
        groupGuid = st.nextToken();
        if (!Util.isValidGuidString(groupGuid))
        {
          // We probably have an alias, lookup the GUID
          groupGuid = gnsClient.lookupGuid(groupGuid);
        }
      }
      else
      {
        console.printString("Wrong number of arguments for this command.\n");
        return;
      }
      String guidToRemove = st.nextToken();

      gnsClient.groupRemoveGuid(groupGuid, guidToRemove, module.getCurrentGuid());
      printString("GUID " + guidToRemove + " removed from group " + groupGuid + "\n");
    }
    catch (Exception e)
    {
      printString("Failed to access GNS ( " + e + ")\n");
    }
  }
}
