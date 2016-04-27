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

import org.json.JSONArray;



import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.msocket.common.Constants;
import edu.umass.cs.msocket.proxy.console.ConsoleModule;

/**
 * Command that list GUIDs in a list
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class ListGuids extends ConsoleCommand
{

  /**
   * Creates a new <code>ListGuids</code> object
   * 
   * @param module
   */
  public ListGuids(ConsoleModule module)
  {
    super(module);
  }

  @Override
  public String getCommandDescription()
  {
    return "Gives the current list of GUIDs in list_name (current valid lists are\n"
        + "   Proxy related lists: ACTIVE_PROXY,INACTIVE_PROXY,SUSPICIOUS_PROXY\n"
        + "   Watchdog service related lists: ACTIVE_WATCHDOG,INACTIVE_WATCHDOG,SUSPICIOUS_WATCHDOG\n"
        + "   Location service related lists: ACTIVE_LOCATION,INACTIVE_LOCATION,SUSPICIOUS_LOCATION.";
  }

  @Override
  public String getCommandName()
  {
    return "list_guids";
  }

  @Override
  public String getCommandParameters()
  {
    return "list_name";
  }

  /**
   * Override execute to check for existing connectivity
   */
  public void execute(String commandText) throws Exception
  {
    if (module.getProxyGroupGuid() == null)
    {
      console.printString("You have to connect to a proxy group first.\n");
      return;
    }
    super.execute(commandText);
  }

  @Override
  public void parse(String commandText) throws Exception
  {
    try
    {
      String listName = commandText.trim();
      GNSClientCommands gnsClient = module.getGnsClient();
      JSONArray proxies;

      if (Constants.isValidList(listName))
      {
        proxies = gnsClient.fieldReadArray(module.getProxyGroupGuid().getGuid(), listName, module.getProxyGroupGuid());
      }
      else
      {
        console.printString("List " + listName + " is invalid\n");
        return;
      }

      console.printString("List of GUIDs in " + listName + ": \n");
      console.printString(proxies.toString(2));
      console.printNewline();
    }
    catch (Exception e)
    {
      console.printString("Failed to access GNS ( " + e + ")\n");
    }
  }
}