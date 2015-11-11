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

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.StringTokenizer;

import edu.umass.cs.gnsclient.client.UniversalTcpClient;
import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.gnsclient.console.ConsoleModule;
import edu.umass.cs.gnsclient.console.GnsUtils;

/**
 * Command to update a field in the GNS
 * 
 * @author mbadov
 * @version 0.1
 */
public class ActiveCodeSet extends ConsoleCommand
{

  /**
   * Creates a new <code>ActiveCodeSet</code> object
   * 
   * @param module
   */
  public ActiveCodeSet(ConsoleModule module)
  {
    super(module);
  }

  @Override
  public String getCommandDescription()
  {
    return "Set active code for the target GUID and action (using the credential of the current GUID/alias), "
        + "if the callbacks don't exist, they are created, otherwise they are overwritten";
  }

  @Override
  public String getCommandName()
  {
    return "activecode_set";
  }

  @Override
  public String getCommandParameters()
  {
    return "[target_guid_or_alias] action filename";
  }

  /**
   * Override execute to check for a selected guid
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
    UniversalTcpClient gnsClient = module.getGnsClient();
    try
    {
      StringTokenizer st = new StringTokenizer(commandText.trim());
      String guid;
      if (st.countTokens() == 2)
      {
        guid = module.getCurrentGuid().getGuid();
      }
      else if (st.countTokens() == 3)
      {
        guid = st.nextToken();
        if (!GnsUtils.isValidGuidString(guid))
        {
          // We probably have an alias, lookup the GUID
          guid = gnsClient.lookupGuid(guid);
        }
      }
      else
      {
        console.printString("Wrong number of arguments for this command.\n");
        return;
      }
      
      String action = st.nextToken();
      String filename = st.nextToken();
      String code = new String(Files.readAllBytes(Paths.get(filename)));
      
      String code64 = Base64.encodeToString(code.getBytes("utf-8"), true);
      gnsClient.activeCodeSet(guid, action, code64, module.getCurrentGuid());
      
      console.printString("Code in '" + filename + "' installed for GUID " + guid + "for action '" + action + "'");
      console.printNewline();
    }
    catch (Exception e)
    {
      console.printString("Failed to access GNS ( " + e + ")\n");
    }
  }
}
