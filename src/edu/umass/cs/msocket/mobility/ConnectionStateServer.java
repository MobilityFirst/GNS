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

package edu.umass.cs.msocket.mobility;

import edu.umass.cs.msocket.MServerSocketController;

/**
 * Class to store each server controller at server mobility manager, so that
 * mobility manager can migrate the server listening address and existing flows,
 * if required. Used by server mobility manager
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class ConnectionStateServer
{

  MServerSocketController mServerSocketController = null;
  String                  localIPAdress           = "";
  String                  remoteIPAdress          = "";

  /**
   * Creates a new <code>ConnectionStateServer</code> object
   * 
   * @param mServerSocketController
   */
  public ConnectionStateServer(MServerSocketController mServerSocketController)
  {
    this.mServerSocketController = mServerSocketController;
  }
}