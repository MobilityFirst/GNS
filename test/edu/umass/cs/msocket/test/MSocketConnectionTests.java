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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.umass.cs.msocket.MServerSocket;
import edu.umass.cs.msocket.MSocket;
import edu.umass.cs.msocket.common.policies.NoProxyPolicy;

/**
 * This class defines a MSocketConnectionTests. We assume that the GNS is
 * pre-configured and running so that an MServerSocket can be created.
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class MSocketConnectionTests
{
  private static final String SOCKET_HRN = "msocket.junit.test2";
  private static final int    LOCAL_PORT = 5454;
  private static final String LOCALHOST  = "127.0.0.1";
  private MServerSocket       mss;

  /**
   * Setup an MServerSocket for testing.
   * 
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception
  {
	// try catch for not workign gns
	try {
		  mss = new MServerSocket(SOCKET_HRN, new NoProxyPolicy(), new InetSocketAddress(LOCALHOST, LOCAL_PORT), 0);
  	} catch(Exception ex) {
  		 mss = new MServerSocket(SOCKET_HRN, new NoProxyPolicy(),
  		        new InetSocketAddress(LOCALHOST, LOCAL_PORT), 0);
  	}
  	System.out.println("MServerSocket created");
    EchoServerThread accepterThread = new EchoServerThread(mss);
    accepterThread.start();
  }

  /**
   * Terminate the MServerSocket
   * 
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception
  {
    mss.close();
  }

  /**
   * Test a simple connection to a local IP without GNS resolution
   */
  @Test
  public void ipConnectionTest()
  {
    try
    {
      MSocket ms = new MSocket(InetAddress.getByName(LOCALHOST), LOCAL_PORT);
      InputStream is = ms.getInputStream();
      OutputStream os = ms.getOutputStream();
      echoTest(is, os, 54);
      echoTest(is, os, 45);
      ms.close();
    }
    catch (Exception e)
    {
      e.printStackTrace();
      fail("IP Connection test failed");
    }
  }

  /**
   * Test a simple connection to a local IP without GNS resolution
   */
  @Test
  public void gnsConnectionTest()
  {
    try
    {
      MSocket ms = new MSocket(SOCKET_HRN, 0);
      InputStream is = ms.getInputStream();
      OutputStream os = ms.getOutputStream();
      echoTest(is, os, 54);
      echoTest(is, os, 45);
      System.out.println("Closing MSocket");
      ms.close();
    }
    catch (Exception e)
    {
      e.printStackTrace();
      fail("IP Connection test failed");
    }
  }

  protected void echoTest(InputStream is, OutputStream os, int value) throws IOException
  {
	  System.out.println("MSocket writing " + value);
    os.write(value);
    os.flush();
    int read = is.read();
    System.out.println("MSocket read " + read);
    if (read != value)
      fail("Wrong value returned by echo server. Expected " + value + " but got " + read);
  }

  private class EchoServerThread extends Thread
  {
    private MServerSocket mss;

    public EchoServerThread(MServerSocket mss)
    {
      super("Echo server thread");
      this.mss = mss;
    }

    public void run()
    {
      try
      {
        MSocket ms = mss.accept();
        InputStream is = ms.getInputStream();
        OutputStream os = ms.getOutputStream();
        boolean done = false;
        // Read one byte at a time from input stream and write it back to output
        // stream
        while (!done)
        {
          int r = is.read();
          System.out.println("MServerSocket read " + r);
          if (r == -1)
            done = true;
          else
          {
        	  System.out.println("MServerSocket writing " + r);
            os.write(r);
            os.flush();
          }
        }
        System.out.println("Closing MServerSocket");
        ms.close();
      }
      catch (IOException e)
      {
        e.printStackTrace();
        fail("MServerSocket side error");
      }
    }
  }
  
}