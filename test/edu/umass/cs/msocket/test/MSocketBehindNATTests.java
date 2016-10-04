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
import java.util.Random;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.umass.cs.msocket.MServerSocket;
import edu.umass.cs.msocket.MSocket;


/**
 * This class defines a MSocket multipaths tests. We assume that the GNS is
 * pre-configured and running so that an MServerSocket can be created.
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class MSocketBehindNATTests
{
  private static final String SOCKET_HRN = "msocket.junit.behindNAT10";
  private MServerSocket       mss;
  private static Logger       log        = Logger.getLogger("MSocket.test");

  /**
   * Setup an MServerSocket for testing.
   * 
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception
  {
    BasicConfigurator.configure();
    log.getRootLogger().setLevel(Level.INFO);
    log.addAppender(new ConsoleAppender());
    //LinkedList<InetSocketAddress> proxyList = new LinkedList<InetSocketAddress>();
    //proxyList.add(new InetSocketAddress("ananas.cs.umass.edu", 11989));
    //mss = new MServerSocket(SOCKET_HRN);
    mss = new MServerSocket(SOCKET_HRN);
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
  public void behindNAT()
  {
    try
    {
      MSocket ms = new MSocket(SOCKET_HRN, 0);
      InputStream is = ms.getInputStream();
      OutputStream os = ms.getOutputStream();
      echoTest(is, os, 1);
      echoTest(is, os, 1);
      echoTest(is, os, 1);
      echoTest(is, os, 1);
      echoTest(is, os, 1);
      echoTest(is, os, 1);
      // to test for migratio
      Thread.sleep(30000);
      
      echoTest(is, os, 1);
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
    //log.info("MSocket writing " + value);
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
          log.info("MServerSocket read " + r + " bytes");
          if (r == -1)
            done = true;
          else
          {
            log.info("MServerSocket writing " + r);
            os.write(buf, 0, r);
            os.flush();
          }
        }
        log.info("Closing MServerSocket");
        ms.close();
      }
      catch (IOException e)
      {
        log.error("MServerSocket side error", e);
        fail("MServerSocket side error");
      }
    }
  }
  
}