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

import edu.umass.cs.msocket.proxy.console.ConsoleModule;

/**
 * Command that stops a location service
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class StopLocation extends ConsoleCommand
{

  /**
   * Creates a new <code>StopLocation</code> object
   * 
   * @param module
   */
  public StopLocation(ConsoleModule module)
  {
    super(module);
  }

  @Override
  public String getCommandDescription()
  {
    return "Stop the currently running location service";
  }

  @Override
  public String getCommandName()
  {
    return "location_stop";
  }

  @Override
  public String getCommandParameters()
  {
    return "";
  }

  /**
   * Override execute to not check for existing connectivity
   * @throws java.lang.Exception
   */
  public void execute(String commandText) throws Exception
  {
    parse(commandText);
  }

  @Override
  public void parse(String commandText) throws Exception
  {
    if (module.getRunningLocationService() == null)
    {
      console.printString("No location service is running.\n");
      return;
    }
    module.getRunningLocationService().killIt();
    module.setRunningLocationService(null);
  }
}
