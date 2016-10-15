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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

/**
 * This class keeps the information about each proxy at the server side. It
 * stores the proxy listening address and last keep alive recvd from the proxy.
 * 
 * @author ayadav
 */
public class ProxyInfo
{
  private String        proxyName;
  private int           proxyPort;
  private SocketChannel proxyChannel;
  private long          lastKeepAlive = 0;
  private boolean       active        = true;

  private SelectionKey  selectorKey   = null;

  /**
   *
   * @param proxyName
   * @param proxyPort
   */
  public ProxyInfo(String proxyName, int proxyPort)
  {
    setProxyInfo(proxyName, proxyPort);
  }

  /**
   *
   * @param proxyName
   * @param proxyPort
   */
  public synchronized void setProxyInfo(String proxyName, int proxyPort)
  {
    this.proxyName = proxyName;
    this.proxyPort = proxyPort;
  }

  /**
   *
   * @return
   */
  public String getProxyInfo()
  {
    return proxyName + ":" + proxyPort;
  }

  /**
   *
   * @return
   */
  public SocketChannel getUnderlyingChannel()
  {
    return proxyChannel;
  }

  /**
   *
   * @return
   */
  public String getProxyName()
  {
    return proxyName;
  }

  /**
   *
   * @return
   */
  public int getProxyPort()
  {
    return proxyPort;
  }

  /**
   *
   * @param lastKeepAlive
   */
  public synchronized void setLastKeepAlive(long lastKeepAlive)
  {
    if (lastKeepAlive > this.lastKeepAlive)
    {
      this.lastKeepAlive = lastKeepAlive;
    }
  }

  /**
   *
   * @return
   */
  public long getLastKeepAlive()
  {
    return this.lastKeepAlive;
  }

  /**
   *
   * @param value
   */
  public synchronized void setActive(boolean value)
  {
    active = value;
  }

  /**
   *
   * @return
   */
  public boolean getActive()
  {
    return active;
  }

  /**
   *
   * @param selectorKey
   */
  public synchronized void setSelectorKey(SelectionKey selectorKey)
  {
    this.selectorKey = selectorKey;
  }

  /**
   *
   * @return
   */
  public SelectionKey getSelectorKey()
  {
    return selectorKey;
  }

  /**
   *
   * @param proxyName
   * @param proxyPort
   * @throws IOException
   */
  public void connectChannel(String proxyName, int proxyPort) throws IOException
  {
    proxyChannel = SocketChannel.open();
    proxyChannel.connect(new InetSocketAddress(proxyName, proxyPort));
    while (!proxyChannel.finishConnect())
      ;
  }

  /**
   *
   */
  public void closeChannel()
  {
    try
    {
      proxyChannel.close();
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
  }
}