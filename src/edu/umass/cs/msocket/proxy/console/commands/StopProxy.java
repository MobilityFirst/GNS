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
 * Command that stops the currently running proxy
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class StopProxy extends ConsoleCommand
{

  /**
   * Creates a new <code>StopProxy</code> object
   * 
   * @param module
   */
  public StopProxy(ConsoleModule module)
  {
    super(module);
  }

  @Override
  public String getCommandDescription()
  {
    return "Stop the currently running proxy";
  }

  @Override
  public String getCommandName()
  {
    return "proxy_stop";
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
    if (module.getRunningProxy() == null)
    {
      console.printString("No proxy is running.\n");
      return;
    }
    module.getRunningProxy().killIt();
    module.setRunningProxy(null);
  }
}
