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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import edu.umass.cs.msocket.MSocket;
import edu.umass.cs.msocket.MWrappedOutputStream;
import edu.umass.cs.msocket.MultipathPolicy;

/**
 * This class implements a client to load the proxy.
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class LoadMonitorClient
{
  public static final int    filesize   = 20000000;             // filesize
                                                                 // temporary
                                                                 // hardcoded

  public static final String ServerName = "ananas.cs.umass.edu";
  public static final int    ServerPort = 3520;
  public static final int    writesize  = 1000;

  public static int          current    = 0;
  public static boolean      MSStart    = false;
  public static boolean      MSStop     = false;
  public static long         start      = 0;

  static MSocket             ms;

  public static void main(String[] args)
  {

    try
    {
      int newGUID = Integer.parseInt(args[2]);
      ms = new MSocket("ProxyloadServer" + newGUID, 1);

      ms.setMultipathPolicy(MultipathPolicy.MULTIPATH_POLICY_RANDOM);

      OutputStream out = null;
      InputStream in = null;
      byte[] sendarray = new byte[writesize];
      byte[] recvarray = new byte[writesize];
      for (int i = 0; i < writesize; i++)
      {
        sendarray[i] = 'a';
      }

      while (true)
      {
        try
        {
          out = ms.getOutputStream();
          in = ms.getInputStream();

          int curpos = 0;
          while (curpos < writesize)
          {
            int bytesRead = in.read(recvarray, 0, recvarray.length);
            if (bytesRead > 0)
            {
              curpos += bytesRead;
            }
          }
          ((MWrappedOutputStream) out).write(sendarray, 0, writesize);
        }
        catch (Exception e)
        {
          e.printStackTrace();
        }
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