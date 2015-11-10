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


import edu.umass.cs.gnsclient.client.UniversalTcpClient;
import edu.umass.cs.gnsclient.console.ConsoleModule;
import edu.umass.cs.gnsclient.console.GnsUtils;
import org.json.JSONObject;

/**
 * Reads a field in the GNS
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class Read extends ConsoleCommand
{

  /**
   * Creates a new <code>FieldRead</code> object
   * 
   * @param module
   */
  public Read(ConsoleModule module)
  {
    super(module);
  }

  @Override
  public String getCommandDescription()
  {
    return "Read the entire JOSN Object record of target GUID (defaults to the current GUID/alias)";
  }

  @Override
  public String getCommandName()
  {
    return "read";
  }

  @Override
  public String getCommandParameters()
  {
    return "[target_guid_or_alias]";
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
    try
    {
      UniversalTcpClient gnsClient = module.getGnsClient();

      StringTokenizer st = new StringTokenizer(commandText.trim());
      String guid;
      if (st.countTokens() == 0)
      {
        guid = module.getCurrentGuid().getGuid();
      }
      else if (st.countTokens() == 1)
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

      JSONObject value = gnsClient.read(guid, module.getCurrentGuid());
      console.printString(value.toString());
      console.printNewline();
    }
    catch (Exception e)
    {
      console.printString("Failed to access GNS ( " + e + ")\n");
    }
  }
}
