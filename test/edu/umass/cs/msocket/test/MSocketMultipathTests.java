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
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.umass.cs.msocket.FlowPath;
import edu.umass.cs.msocket.MServerSocket;
import edu.umass.cs.msocket.MSocket;
import edu.umass.cs.msocket.common.proxy.policies.NoProxyPolicy;

/**
 * This class defines a MSocket multipaths tests. We assume that the GNS is
 * pre-configured and running so that an MServerSocket can be created.
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class MSocketMultipathTests
{
  private static final String SOCKET_HRN = "msocket.junit.multipath";
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
    mss = new MServerSocket(SOCKET_HRN, new NoProxyPolicy(),
        new InetSocketAddress(LOCALHOST, LOCAL_PORT), 0);
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
   * Block for 30 seconds before every write/read to/from the client to the echo
   * server. The server itself does not block.
   */
  @Test
  public void addRemovePaths()
  {
    try
    {
      MSocket ms = new MSocket(InetAddress.getByName(LOCALHOST), LOCAL_PORT);
      InputStream is = ms.getInputStream();
      OutputStream os = ms.getOutputStream();
      echoTest(is, os, 1);
      FlowPath path1 = ms.addFlowPath(null);
      System.out.println("Added 2nd flowpath");
      echoTest(is, os, 1);
      FlowPath path2 = ms.addFlowPath(null);
      System.out.println("Added 3rd flowpath");
      echoTest(is, os, 1);
      ms.removeFlowPath(path1);
      System.out.println("removing fp id "+path1.getFlowPathId());
      echoTest(is, os, 1);
      ms.removeFlowPath(path2);
      System.out.println("removing fp id "+path2.getFlowPathId());
      echoTest(is, os, 1);
      //Thread.sleep(30000);
      ms.close();
    }
    catch (Exception e)
    {
      e.printStackTrace();
      fail("addRemovePaths test failed");
    }
  }

  private void sleepFor(int ms)
  {
    try
    {
      Thread.sleep(ms);
    }
    catch (InterruptedException e)
    {
      e.printStackTrace();
    }
  }

  private void echoTest(InputStream is, OutputStream os, int ms) throws IOException
  {
    sleepFor(ms);
    int value = new Random().nextInt(255);
    System.out.println("MSocket writing " + value);
    os.write(value);
    os.flush();
    sleepFor(ms);
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
        byte[] buf = new byte[1024 * 1024];
        // Read one byte at a time from input stream and write it back to output
        // stream
        while (!done)
        {
          int r = is.read(buf);
          System.out.println("MServerSocket read " + r + " bytes");
          if (r == -1)
            done = true;
          else
          {
        	System.out.println("MServerSocket writing " + r);
            os.write(buf, 0, r);
            os.flush();
          }
        }
        System.out.println("Closing MServerSocket");
        ms.close();
      }
      catch (IOException e)
      {
    	  System.out.println("MServerSocket side error"+ e.getMessage());
        fail("MServerSocket side error");
      }
    }
  }
  
}