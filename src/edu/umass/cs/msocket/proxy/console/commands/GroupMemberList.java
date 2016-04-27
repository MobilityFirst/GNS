/*******************************************************************************
 *
 * Mobility First - mSocket library
 * Copyright (C) 2013, 2014 - University of Massachusetts Amherst
 * Contact: arun@cs.umass.edu
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. 
 *
 * Initial developer(s): Arun Venkataramani, Aditya Yadav, Emmanuel Cecchet.
 * Contributor(s): ______________________.
 *
 *******************************************************************************/

package edu.umass.cs.msocket.proxy.console.commands;

import java.util.StringTokenizer;

import org.json.JSONArray;




import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnscommon.utils.Util;
import edu.umass.cs.msocket.proxy.console.ConsoleModule;

/**
 * Command to list members in a group GUID
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class GroupMemberList extends ConsoleCommand
{

  /**
   * Creates a new <code>GroupMemberList</code> object
   * 
   * @param module
   */
  public GroupMemberList(ConsoleModule module)
  {
    super(module);
  }

  @Override
  public String getCommandDescription()
  {
    return "List members of a group GUID";
  }

  @Override
  public String getCommandName()
  {
    return "group_member_list";
  }

  @Override
  public String getCommandParameters()
  {
    return "[group_guid_or_alias]";
  }

  @Override
  public void parse(String commandText) throws Exception
  {
	  GNSClientCommands gnsClient = module.getGnsClient();
    try
    {
      StringTokenizer st = new StringTokenizer(commandText.trim());
      String groupGuid;
      if (st.countTokens() == 0)
      {
        groupGuid = module.getAccountGuid().getGuid();
      }
      else if (st.countTokens() == 1)
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

      console.printString("Members in group " + groupGuid);
      console.printNewline();
      JSONArray members = gnsClient.groupGetMembers(groupGuid, module.getAccountGuid());
      for (int i = 0; i < members.length(); i++)
      {
        console.printString(i + ": " + members.getString(i));
        console.printNewline();
      }
    }
    catch (Exception e)
    {
      console.printString("Failed to access GNS ( " + e + ")\n");
    }
  }
  
}