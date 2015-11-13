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

import edu.umass.cs.msocket.proxy.console.ConsoleModule;

/**
 * Command that sets the new location (longitude/latitude) of a proxy
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class ProxyLocation extends ConsoleCommand
{

  /**
   * Creates a new <code>ProxyLocation</code> object
   * 
   * @param module
   */
  public ProxyLocation(ConsoleModule module)
  {
    super(module);
  }

  @Override
  public String getCommandDescription()
  {
    return "Sets a new location for the currently running proxy";
  }

  @Override
  public String getCommandName()
  {
    return "proxy_location";
  }

  @Override
  public String getCommandParameters()
  {
    return "longitude latitude";
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
    if (module.getRunningProxy() == null)
    {
      console.printString("No proxy is running.\n");
      return;
    }
    StringTokenizer st = new StringTokenizer(commandText);
    if (st.countTokens() != 2)
    {
      console.printString("Invalid number of parameters.\n");
      return;
    }
    double longitude = Double.parseDouble(st.nextToken());
    double latitude = Double.parseDouble(st.nextToken());

    module.getRunningProxy().publishNewProxyLocation(longitude, latitude);
    console.printString("New proxy location set at " + longitude + "," + latitude + "\n");
  }
}
