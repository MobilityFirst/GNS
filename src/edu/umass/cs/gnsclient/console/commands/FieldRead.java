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

/**
 * Reads a field in the GNS
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class FieldRead extends ConsoleCommand
{

  /**
   * Creates a new <code>FieldRead</code> object
   * 
   * @param module
   */
  public FieldRead(ConsoleModule module)
  {
    super(module);
  }

  @Override
  public String getCommandDescription()
  {
    return "Read the first value of given field in the target GUID (using the credential of the current GUID/alias)";
  }

  @Override
  public String getCommandName()
  {
    return "field_read";
  }

  @Override
  public String getCommandParameters()
  {
    return "[target_guid_or_alias] field_to_read";
  }

  /**
   * Override execute to check for existing connectivity
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
      if (st.countTokens() == 1)
      {
        guid = module.getCurrentGuid().getGuid();
      }
      else if (st.countTokens() == 2)
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

      String field = st.nextToken();

      String value = gnsClient.fieldRead(guid, field, module.getCurrentGuid());
      console.printString(value);
      console.printNewline();
    }
    catch (Exception e)
    {
      console.printString("Failed to access GNS ( " + e + ")\n");
    }
  }
}
