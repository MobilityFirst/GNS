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

package edu.umass.cs.msocket.proxy.forwarder;

import java.util.Vector;

/**
 * This class stores the information required to splice two ends of a connection
 * at the proxy.
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class ProxyTCPSplicer
{

  public static final int      GET         = 1;
  public static final int      PUT         = 2;

  public static final int      SERVER_SIDE = 1;
  public static final int      CLIENT_SIDE = 2;

  private Vector<ProxyMSocket> ClientSide  = null;
  private Vector<ProxyMSocket> ServerSide  = null;
  private final int            proxyId;

  public ProxyTCPSplicer(int ProxyId)
  {
    ClientSide = new Vector<ProxyMSocket>();
    ServerSide = new Vector<ProxyMSocket>();
    this.proxyId = ProxyId;
  }

  public synchronized ProxyMSocket ProxyTCPSplicerOperation(int Oper, int ServerOrClient, ProxyMSocket Socket)
  {
    switch (Oper)
    {
      case GET :
      {
        if (ServerOrClient == SERVER_SIDE)
        {
          return ServerSide.get(0);
        }
        else if (ServerOrClient == CLIENT_SIDE)
        {
          return ClientSide.get(0);
        }
        break;
      }
      case PUT :
      {
        if (ServerOrClient == SERVER_SIDE)
        {
          ServerSide.add(Socket);
        }
        else if (ServerOrClient == CLIENT_SIDE)
        {
          ClientSide.add(Socket);
        }
        break;
      }
    }
    return null;
  }

  public int getProxyId()
  {
    return proxyId;
  }
}