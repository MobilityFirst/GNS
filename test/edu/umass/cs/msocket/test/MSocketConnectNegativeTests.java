/**
 * Mobility First - Global Name Resolution Service (GNS)
 * Copyright (C) 2013 University of Massachusetts - Emmanuel Cecchet.
 * Contact: cecchet@cs.umass.edu
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. 
 *
 * Initial developer(s): Emmanuel Cecchet.
 * Contributor(s): ______________________.
 */

package edu.umass.cs.msocket.test;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;

import org.junit.Test;

import edu.umass.cs.msocket.MSocket;

/**
 * This class defines a MSocketConnectNegativeTests
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class MSocketConnectNegativeTests
{

  /**
   * Connect to a standard ServerSocket (Google HTTP server) and expect
   * connection failure.
   */
  @Test
  public void connectToRegularServerSocketHTTP()
  {
    try
    {
      final InetSocketAddress google = new InetSocketAddress("www.google.com", 80);
      System.out.println("Connecting to " + google);
      new MSocket(InetAddress.getByName("www.google.com"), 80);
      fail("Google does not support MServerSocket, we should have gotten an error");
    }
    catch (SocketTimeoutException e)
    {
      // Success we got a timeout
    }
    catch (IOException e)
    {
      e.printStackTrace();
      fail("Unexpected exception " + e);
    }
  }

  /**
   * Connect to a standard daytime server and expect connection failure.
   */
  @Test
  public void connectToRegularServerSocketDaytime()
  {
    try
    {
      final InetSocketAddress time = new InetSocketAddress("time.nist.gov", 13);
      System.out.println("Connecting to " + time);
      new MSocket(InetAddress.getByName("time.nist.gov"), 13);
      fail("Daytime does not support MServerSocket, we should have gotten an error");
    }
    catch (ConnectException e)
    {
      // Success we got a timeout
    }
    catch (IOException e)
    {
      e.printStackTrace();
      fail("Unexpected exception " + e);
    }
  }

  /**
   * Connect to an invalid IP/port and expect conection failure.
   */
  @Test
  public void connectToInvalidSocket()
  {
    try
    {
      final InetSocketAddress invalid = new InetSocketAddress("www.google.com", 1234);
      System.out.println("Connecting to " + invalid);
      new MSocket(InetAddress.getByName("www.google.com"), 1234);
      fail("There is no server, we should have gotten an error");
    }
    catch (ConnectException e)
    {
      // Success we got a timeout
    }
    catch (IOException e)
    {
      e.printStackTrace();
      fail("Unexpected exception " + e);
    }
  }

}