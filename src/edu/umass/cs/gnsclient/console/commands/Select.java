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

import org.json.JSONArray;
import org.json.JSONObject;

import edu.umass.cs.gnsclient.client.UniversalTcpClient;
import edu.umass.cs.gnsclient.console.ConsoleModule;

/**
 * Reads a field in the GNS
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class Select extends ConsoleCommand
{

  /**
   * Creates a new <code>Select</code> object
   * 
   * @param module
   */
  public Select(ConsoleModule module)
  {
    super(module);
  }

  @Override
  public String getCommandDescription()
  {
    return "Returns all records that have a field that contains the given value";
  }

  @Override
  public String getCommandName()
  {
    return "select";
  }

  @Override
  public String getCommandParameters()
  {
    return "field value";
  }

  /**
   * Override execute to not check for existing connectivity
   * @throws java.lang.Exception
   */
  @Override
  public void execute(String commandText) throws Exception
  {
    parse(commandText);
  }

  @Override
  public void parse(String commandText) throws Exception
  {
    try
    {
      UniversalTcpClient gnsClient = module.getGnsClient();

      StringTokenizer st = new StringTokenizer(commandText.trim());
      if (st.countTokens() != 2)
      {
        console.printString("Wrong number of arguments for this command.\n");
        return;
      }
      String field = st.nextToken();

      String value = st.nextToken();

      JSONArray result = gnsClient.select(field, value);
      // console.printString(result.toString());
      // console.printNewline();
      for (int i = 0; i < result.length(); i++)
      {
        console.printString(result.getJSONObject(i).getString("GUID") + " -> "
            + toPrettyRecordString(result.getJSONObject(i)));
        console.printNewline();
      }
    }
    catch (Exception e)
    {
      console.printString("Failed to access GNS ( " + e + ")\n");
    }
  }

  private String toPrettyRecordString(JSONObject json) throws Exception
  {
    json.remove("GUID");
    return json.toString();
  }
}
