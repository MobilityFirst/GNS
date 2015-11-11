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

import org.json.JSONObject;

import edu.umass.cs.gnscommon.GnsProtocol;
import edu.umass.cs.gnsclient.client.UniversalTcpClient;
import edu.umass.cs.gnsclient.console.ConsoleModule;

/**
 * Lookup a GUID corresponding to an alias in the GNS
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class AliasLookup extends ConsoleCommand
{

  /**
   * Creates a new <code>AliasLookup</code> object
   * 
   * @param module
   */
  public AliasLookup(ConsoleModule module)
  {
    super(module);
  }

  @Override
  public String getCommandDescription()
  {
    return "Lookup the alias corresponding to an GUID";
  }

  @Override
  public String getCommandName()
  {
    return "alias_lookup";
  }

  @Override
  public String getCommandParameters()
  {
    return "GUID";
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
      String guid = st.nextToken();

      UniversalTcpClient gnsClient = module.getGnsClient();
      JSONObject entityInfo = gnsClient.lookupGuidRecord(guid);
      String alias = entityInfo.getString(GnsProtocol.GUID_RECORD_NAME);
      console.printString(guid + " has alias " + alias);
      console.printNewline();
    }
    catch (Exception e)
    {
      console.printString("Failed to access GNS ( " + e + ")\n");
    }
  }
}
