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

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.msocket.proxy.console.ConsoleModule;

/**
 * Command that connects to the GNS instance and sets the default GUID account
 * to use
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class Connect extends ConsoleCommand
{

  /**
   * Creates a new <code>GnsConnect</code> object
   * 
   * @param module
   */
  public Connect(ConsoleModule module)
  {
    super(module);
  }

  @Override
  public String getCommandDescription()
  {
    return "Connects to the GNS using the specified account GUID. If no GNS hostname or port number is provided, the default is gns.name 8080";
  }

  @Override
  public String getCommandName()
  {
    return "gns_connect";
  }

  @Override
  public String getCommandParameters()
  {
    return "Account_GUID Gns_Host_Name Gns_Port_Number";
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
      StringTokenizer st = new StringTokenizer(commandText);
      if (st.countTokens() != 3)
      {
        console.printString("Invalid number of parameters for gns_connect");
        console.printNewline();
        return;
      }
      String accountGuid = st.nextToken();
      String gnsHost = st.nextToken();
      int gnsPort = Integer.valueOf(st.nextToken());

      // Create a client
      GNSClientCommands gnsClient = new GNSClientCommands(null);
      if (!module.isSilent())
        console.printString("Connected to GNS at " + gnsHost + ":" + gnsPort + "\n");

      module.setGnsClient(gnsClient);
      GuidEntry accountGuidEntry = KeyPairUtils.getGuidEntry(module.getGnsClient().getGNSProvider(), accountGuid);
      if (accountGuidEntry == null)
      {
        console.printString("No information found for GUID " + accountGuidEntry);
        console.printNewline();
        return;
      }

      
      module.setPromptString(ConsoleModule.CONSOLE_PROMPT + accountGuid + "@" + gnsHost + ":" + gnsPort + ">");
      
      module.setGnsClient(gnsClient);
      module.setAccountGuid(accountGuidEntry);
    }
    catch (Exception e)
    {
      console.printString("Failed to connect to GNS ( " + e + ")\n");
      module.setGnsClient(null);
      module.setPromptString(ConsoleModule.CONSOLE_PROMPT + "not connected to GNS>");
    }
  }
}
