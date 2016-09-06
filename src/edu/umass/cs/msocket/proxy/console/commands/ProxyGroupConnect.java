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


import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.msocket.proxy.console.ConsoleModule;

/**
 * Command that connects to the DiCloud server
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class ProxyGroupConnect extends ConsoleCommand
{

  /**
   * Creates a new <code>ProxyGroupConnect</code> object
   * 
   * @param module
   */
  public ProxyGroupConnect(ConsoleModule module)
  {
    super(module);
  }

  @Override
  public String getCommandDescription()
  {
    return "Connect to a proxy group (you must own the private key to the group)";
  }

  @Override
  public String getCommandName()
  {
    return "proxy_group_connect";
  }

  @Override
  public String getCommandParameters()
  {
    return "proxy_group_name";
  }

  /**
   * Override execute to not check for existing connectivity
   */
  public void execute(String commandText) throws Exception
  {
    parse(commandText);
  }

  @Override
  public void parse(String commandText) throws Exception
  {
    try
    {
      String proxyGroupName = commandText.trim();

      if (!module.isSilent())
        console.printString("Looking for proxy group " + proxyGroupName + " GUID and certificates...\n");
      GuidEntry myGuid = KeyPairUtils.getGuidEntry(module.getGnsClient().getGNSProvider(), proxyGroupName);

      if (myGuid == null)
      {
        console.printString("No keys found for proxy group " + proxyGroupName + ". Cannot connect without the key\n");
        return;
      }
      if (!module.isSilent())
        console.printString("We are guid " + myGuid.getGuid() + "\n");
      module.setProxyGroupGuid(myGuid);
      module.setPromptString(ConsoleModule.CONSOLE_PROMPT + proxyGroupName + ">");
    }
    catch (Exception e)
    {
      console.printString("Failed to coonect to proxy group ( " + e + ")\n");
    }
  }
}
