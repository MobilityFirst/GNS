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

package edu.umass.cs.msocket.proxy.console;

import java.io.IOException;
import java.io.PrintWriter;

import jline.ConsoleReader;

/**
 * This class defines a ProxyConsole
 * 
 * @author <a href="mailto:manu@frogthinker.org">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class ProxyConsole
{

  /**
   * @param args -silent to avoid any prompt in non-interactive mode
   */
  public static void main(String[] args)
  {
    boolean silent = (args.length > 0 && "-silent".equalsIgnoreCase(args[0]));
    try
    {
      ConsoleReader consoleReader = new ConsoleReader(System.in, new PrintWriter(System.out, true));
      ConsoleModule module = new ConsoleModule(consoleReader);
      module.setSilent(silent);
      module.handlePrompt();
    }
    catch (IOException e)
    {
      e.printStackTrace();
      System.exit(-1);
    }
  }

}