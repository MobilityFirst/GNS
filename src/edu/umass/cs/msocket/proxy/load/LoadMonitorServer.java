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

package edu.umass.cs.msocket.proxy.load;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Timer;
import java.util.TimerTask;

import edu.umass.cs.msocket.MServerSocket;
import edu.umass.cs.msocket.MSocket;
import edu.umass.cs.msocket.MWrappedOutputStream;
import edu.umass.cs.msocket.common.proxy.policies.NoProxyPolicy;

/**
 * This class implements a server to load the proxy.
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class LoadMonitorServer
{

  // name of the proxy, that it will load monitor
  public static String        ProxyName      = "ananas.cs.umass.edu";
  public static int           ProxyPort      = 11989;
  public static int           ServerPort     = 11990;
  public static final int     writesize      = 1000;
  public static final int     numRTTAverage  = 3;
  public static final int     measureFreq    = 1000;                   // every
                                                                        // 1000
                                                                        // msec
  public static long[]        LoadInducedRTT = new long[numRTTAverage];

  /*
   * The sub-class TestServerConnection below spawns a thread for each new
   * accepted connection.
   */
  public static MServerSocket mss            = null;
  static MSocket              msocket        = null;

  public static class TestServerConnection implements Runnable
  {

    TestServerConnection(MSocket ms)
    {
      msocket = ms;
    }

    public void run()
    {
      OutputStream out = null;
      InputStream in = null;
      byte[] sendarray = new byte[writesize];
      byte[] recvarray = new byte[writesize];
      for (int i = 0; i < writesize; i++)
      {
        sendarray[i] = 'a';
      }
      int CurrRTTNum = 0;
      for (int i = 0; i < numRTTAverage; i++)
      {
        LoadInducedRTT[i] = 0;
      }

      while (true)
      {
        try
        {

          out = msocket.getOutputStream();
          in = msocket.getInputStream();

          long start = System.currentTimeMillis();
          ((MWrappedOutputStream) out).write(sendarray, 0, writesize);

          int curpos = 0;
          while (curpos < writesize)
          {
            int bytesRead = in.read(recvarray, 0, recvarray.length);
            if (bytesRead > 0)
            {
              curpos += bytesRead;
            }
          }
          long end = System.currentTimeMillis();

          long currRTTEst = end - start;
          LoadInducedRTT[CurrRTTNum] = currRTTEst;

        }
        catch (Exception e)
        {
          e.printStackTrace();
        }
        CurrRTTNum++;
        CurrRTTNum = CurrRTTNum % numRTTAverage;
        if (CurrRTTNum == 0)
        {
          long Avg = 0;
          for (int i = 0; i < numRTTAverage; i++)
          {
            Avg += LoadInducedRTT[i];
          }

          System.out.println("Average Load Induced RTT" + (Avg / numRTTAverage));
        }
        try
        {
          Thread.sleep(measureFreq);
        }
        catch (InterruptedException e)
        {
          e.printStackTrace();
        }
      }
    }
  }

  // End TestServerConnection

  public static BufferedWriter ServerMigrationWritter = null;

  public static void startTimer(Timer timer)
  {

    timer.schedule(new TimerTask()
    {
      @Override
      public void run()
      {
    	  System.out.println();
      }
    }, 1000, 1000);
  }

  public static void main(String[] args) throws IOException
  {
	  System.out.println("Max Heap memory" + java.lang.Runtime.getRuntime().maxMemory());
	  System.out.println("args[0] " + args[0]);
    try
    {
      int newGUID = Integer.parseInt(args[2]);

      mss = new MServerSocket("ProxyloadServer" + newGUID, new NoProxyPolicy(),
          null, 0);

      for (int i = 0; true; i++)
      {
    	System.out.println("Waiting for connections");
        MSocket ms = mss.accept();
        System.out.println("Accepted new connection " + (i + 1));

        TestServerConnection tsc = new TestServerConnection(ms);
        (new Thread(tsc)).start();
      }
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }
}