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

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Enumeration;
import java.util.Vector;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import edu.umass.cs.msocket.AcceptConnectionQueue;
import edu.umass.cs.msocket.logger.MSocketLogger;

/**
 * This class is for accepting connections at the proxy, It implements the
 * proxy's listening server socket. It accepts the connection from both servers
 * and clients.
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class ProxyServerSocket extends ServerSocket
{
  private ServerSocketChannel   ssc        = null;
  private ServerSocket          ss         = null;

  private boolean               Migrating  = false;                                              // used
                                                                                                  // for
                                                                                                  // catching
                                                                                                  // accept
                                                                                                  // exception
                                                                                                  // while
                                                                                                  // migrating
                                                                                                  // the
                                                                                                  // server
                                                                                                  // listening
                                                                                                  // socket

  private AcceptConnectionQueue AcceptConnectionQueueObj;
  private AcceptThreadPool      AcceptThreadPoolObj;
  private final Object          monitor    = new Object();

  private long                  SOTimeout  = 0;                                                  // set
                                                                                                  // to
                                                                                                  // 0
                                                                                                  // which
                                                                                                  // means
                                                                                                  // infinite
                                                                                                  // timeout
  private ProxyForwarder        PForwarder 			= null;

  /**
   * @param ServerName
   * @param port
   * @throws IOException
   * @throws SocketException
   */
  public ProxyServerSocket(String ServerName, int port, ProxyForwarder PForwarder) throws IOException, SocketException
  {
	this.PForwarder = PForwarder;
    InetAddress iaddr = getActiveInterfaceAddresses();

    ssc = ServerSocketChannel.open();

    ss = ssc.socket();
    ss.bind(new InetSocketAddress(iaddr, port));
    AcceptConnectionQueueObj = new AcceptConnectionQueue();

    AcceptThreadPoolObj = new AcceptThreadPool(ssc);
    (new Thread(AcceptThreadPoolObj)).start();
  }  

  public InetSocketAddress getProxyListeningAddress()
  {
    return (InetSocketAddress) ss.getLocalSocketAddress();
  }

  /**
   * 
   */
  public InetAddress getInetAddress()
  {
    return ss.getInetAddress();
  }

  public SocketAddress getLocalSocketAddress()
  {
    return ss.getLocalSocketAddress();
  }

  /**
	 * 
	 */
  public void bind(SocketAddress endpoint) throws IOException, SecurityException, IllegalArgumentException
  {
    ss.bind(endpoint);
  }

  /**
   * @return
   */
  public boolean getMigratingState()
  {
    return Migrating;
  }

  /**
   * proxy accept function
   */
  public ProxyMSocket accept() throws IOException, SocketTimeoutException
  {
    if (SOTimeout == 0)
    {
      BlockForAccept();
      return (ProxyMSocket) AcceptConnectionQueueObj.getFromQueue(AcceptConnectionQueue.GET, null);
    }
    else
    {
      try
      {
        TimeOutWaitForaccept();
        return (ProxyMSocket) AcceptConnectionQueueObj.getFromQueue(AcceptConnectionQueue.GET, null);
      }
      catch (final InterruptedException e)
      {
        e.printStackTrace();
        // The thread was interrupted during sleep, wait or join
      }
      catch (final TimeoutException e)
      {
        SocketTimeoutException ste = new SocketTimeoutException("socket time out exception");
        throw ste;
        // Took too long!
      }
      catch (final ExecutionException e)
      {
        e.printStackTrace();
        // An exception from within the Runnable task
      }
    }
    return null;
  }

  // private methods
  private void TimeOutWaitForaccept() throws InterruptedException, TimeoutException, ExecutionException
  {
    ExecutorService service = Executors.newSingleThreadExecutor();
    try
    {
      Runnable r = new Runnable()
      {
        @Override
        public void run()
        {
          BlockForAccept();
        }
      };

      Future<?> f = service.submit(r);

      f.get(SOTimeout, TimeUnit.MILLISECONDS); // attempt the task for two
                                               // minutes
    }
    finally
    {
      service.shutdown();
    }
  }

  /**
   * @author ayadav
   */
  private class Handler implements Runnable
  {
    private final SocketChannel connectionSocketChannel;

    Handler(SocketChannel connectionSocketChannel)
    {
      this.connectionSocketChannel = connectionSocketChannel;
    }

    public void run()
    {
      // read and service request on socket
      // FIXME: check for how to handle exceptions here
      MSocketLogger.getLogger().fine("new connection accepted by socket channel");

      ProxyMSocket ms = null;
      try
      {
        ms = new ProxyMSocket(connectionSocketChannel, PForwarder);
      }
      catch (IOException e)
      {
        e.printStackTrace();
        // FIXME: exception for newly accepted socket
        // close and reject socket so that client reconnects again
        // do not put in active queue as curently done
        // transitoion into all ready state as well
      }

      MSocketLogger.getLogger().fine("Accepted connection from " + ms.getInetAddress() + ":" + ms.getPort());

      AcceptConnectionQueueObj.getFromQueue(AcceptConnectionQueue.PUT, ms);
      synchronized (monitor)
      {
        monitor.notifyAll();
      }

    }
  }

  private void BlockForAccept()
  {
    MSocketLogger.getLogger().fine("accept called");
    synchronized (monitor)
    {
      while ((Integer) AcceptConnectionQueueObj.getFromQueue(AcceptConnectionQueue.GET_SIZE, null) == 0)
      {
        try
        {
          monitor.wait();
        }
        catch (InterruptedException e)
        {
          e.printStackTrace();
        }
      }
    }
    MSocketLogger.getLogger().fine("new connection socket ready");
  }

  /**
	 * 
	 */
  private class AcceptThreadPool implements Runnable
  {
    private final ServerSocketChannel serverSocketChannel;
    private final ExecutorService     pool;
    private boolean                   runstatus = true;

    public AcceptThreadPool(ServerSocketChannel ssc) throws IOException
    {
      serverSocketChannel = ssc;
      pool = Executors.newCachedThreadPool();
    }

    public void run()
    { // run the service
      while (runstatus)
      {
        try
        {
          pool.execute(new Handler(serverSocketChannel.accept()));
        }
        catch (IOException ex)
        {
          // pool.shutdown();
          // FIXME: correct here, shouldn't ignore the exception every time
          continue;
        }
      }
    }

    public void StopAcceptPool()
    {
      runstatus = false;
      pool.shutdownNow();
    }
  }

  /**
   * returns a non loop back IPv4 interface address on the proxy to start
   * listening on
   * 
   * @return
   */
  private InetAddress getActiveInterfaceAddresses()
  {
    Vector<InetAddress> CurrentInterfaceIPs = new Vector<InetAddress>();
    try
    {
      for (Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces(); en.hasMoreElements();)
      {
        NetworkInterface intf = en.nextElement();
        for (Enumeration<InetAddress> enumIpAddr = intf.getInetAddresses(); enumIpAddr.hasMoreElements();)
        {
          InetAddress inetAddress = enumIpAddr.nextElement();
          if (!inetAddress.isLoopbackAddress())
          {

            // FIXME: find better method to get ipv4 address
            String IP = inetAddress.getHostAddress();
            if (IP.contains(":")) // means IPv6
            {
              continue;
            }
            else
            {
              MSocketLogger.getLogger().fine("Interface IP " + IP);
              CurrentInterfaceIPs.add(inetAddress);
            }
          }
        }
      }
    }
    catch (Exception ex)
    {
      ex.printStackTrace();
    }
    if (CurrentInterfaceIPs.size() > 0)
    {
      return CurrentInterfaceIPs.get(0);
    }
    return null;
  }
}