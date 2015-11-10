/**
 * Mobility First - Global Name Service (GNS)
 * Copyright (C) 2015 University of Massachusetts
 * Contact: support@gns.name
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. 
 *
 * Initial developer(s): Emmanuel Cecchet.
 * Contributor(s): ______________________.
 */

package edu.umass.cs.gnsclient.console.commands;

import java.util.StringTokenizer;

import edu.umass.cs.gnsclient.client.UniversalTcpClient;
import edu.umass.cs.gnsclient.console.ConsoleModule;

/**
 * Lookup a GUID corresponding to an alias in the GNS
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class GuidLookup extends ConsoleCommand
{

  /**
   * Creates a new <code>GuidLookup</code> object
   * 
   * @param module
   */
  public GuidLookup(ConsoleModule module)
  {
    super(module);
  }

  @Override
  public String getCommandDescription()
  {
    return "Lookup the GUID corresponding to an alias";
  }

  @Override
  public String getCommandName()
  {
    return "guid_lookup";
  }

  @Override
  public String getCommandParameters()
  {
    return "alias";
  }

  @Override
  public void parse(String commandText) throws Exception
  {
    try
    {
      StringTokenizer st = new StringTokenizer(commandText.trim());
      if (st.countTokens() != 1)
      {
        console.printString("Wrong number of arguments for this command.\n");
        return;
      }
      String alias = st.nextToken();

      UniversalTcpClient gnsClient = module.getGnsClient();
      String value = gnsClient.lookupGuid(alias);
      console.printString(alias + " has GUID " + value);
      console.printNewline();
    }
    catch (Exception e)
    {
      console.printString("Failed to access GNS ( " + e + ")\n");
    }
  }
}
