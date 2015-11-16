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

package edu.umass.cs.msocket;

/**
 * This class keeps the constants needed for MSocket.
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class MSocketConstants
{

  // indicates whether it is client or server migration
  public static final int CLIENT_MIG     = 1;
  public static final int SERVER_MIG     = 2;

  // denotes whether MSocket is at server or client side, used mainly in case of
  // proxy, as server needs to send keep alive and client needs to receive on a
  // timer
  public static final int SERVER         = 1;
  public static final int CLIENT         = 2;

  public static final int ACTIVE         = 0;
  public static final int FIN_WAIT_1     = 1;
  public static final int FIN_WAIT_2     = 2;
  public static final int CLOSING        = 3;
  public static final int TIME_WAIT      = 4;
  public static final int CLOSE_WAIT     = 5;
  public static final int LAST_ACK       = 6;
  public static final int CLOSED         = 7;

  // serverIP was given in MSocket constructor
  public static final int CON_TO_IP      = 1;

  // server DNS name was given in MSocket constructor
  public static final int CON_TO_DNSNAME = 2;

  // server GNS name was given in MSocket constructor
  public static final int CON_TO_GNSNAME = 3;

  // server GNS GUID was given in MSocket constructor
  public static final int CON_TO_GNSGUID = 4;

}
