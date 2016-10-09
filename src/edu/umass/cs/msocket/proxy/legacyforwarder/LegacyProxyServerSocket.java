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

package edu.umass.cs.msocket.proxy.legacyforwarder;

public class LegacyProxyServerSocket 
{
	  /*private ServerSocketChannel   	ssc        = null;
	  private ServerSocket          	ss         = null;

	  private boolean               	migrating  = false;                                           // used
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

	  private AcceptConnectionQueue 	acceptConnectionQueueObj;
	  private AcceptThreadPool      	acceptThreadPoolObj;
	  private final Object          	monitor    = new Object();

	  private long                  	SOTimeout  = 0;                                               // set
	                                                                                                  // to
	                                                                                                  // 0
	                                                                                                  // which
	                                                                                                  // means
	                                                                                                  // infinite
	                                                                                                  // timeout
	  private LegacyProxyForwarder        	pForwarder 			= null;

	  private static Logger         	log        = Logger.getLogger(LegacyProxyServerSocket.class.getName());*/

	  /**
	   * @param ServerName
	   * @param port
	   * @throws IOException
	   * @throws SocketException
	   */
	  /*public LegacyProxyServerSocket(String serverName, int port, LegacyProxyForwarder pForwarder) throws IOException, SocketException
	  {
		this.pForwarder = pForwarder;
	    InetAddress iaddr = getActiveInterfaceAddresses();

	    ssc = ServerSocketChannel.open();

	    ss = ssc.socket();
	    ss.bind(new InetSocketAddress(iaddr, port));
	    acceptConnectionQueueObj = new AcceptConnectionQueue();

	    acceptThreadPoolObj = new AcceptThreadPool(ssc);
	    (new Thread(acceptThreadPoolObj)).start();
	  }  

	  public InetSocketAddress getProxyListeningAddress()
	  {
	    return (InetSocketAddress) ss.getLocalSocketAddress();
	  }


	  public InetAddress getInetAddress()
	  {
	    return ss.getInetAddress();
	  }

	  public SocketAddress getLocalSocketAddress()
	  {
	    return ss.getLocalSocketAddress();
	  }


	  public void bind(SocketAddress endpoint) throws IOException, SecurityException, IllegalArgumentException
	  {
	    ss.bind(endpoint);
	  }

	  public boolean getMigratingState()
	  {
	    return migrating;
	  }*/
	  
	  /**
	   * proxy accept function
	   */
	  /*public ProxyMSocket accept() throws IOException, SocketTimeoutException
	  {
	    if (SOTimeout == 0)
	    {
	      BlockForAccept();
	      return (ProxyMSocket) acceptConnectionQueueObj.getFromQueue(AcceptConnectionQueue.GET, null);
	    }
	    else
	    {
	      try
	      {
	        TimeOutWaitForaccept();
	        return (ProxyMSocket) acceptConnectionQueueObj.getFromQueue(AcceptConnectionQueue.GET, null);
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

	      LegacyProxyMSocket ms = null;
	      try
	      {
	        ms = new LegacyProxyMSocket(connectionSocketChannel, pForwarder);
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

	      acceptConnectionQueueObj.getFromQueue(AcceptConnectionQueue.PUT, ms);
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
	      while ((Integer) acceptConnectionQueueObj.getFromQueue(AcceptConnectionQueue.GET_SIZE, null) == 0)
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
	  }*/

	  /**
	   * returns a non loop back IPv4 interface address on the proxy to start
	   * listening on
	   * 
	   * @return
	   */
	  /*private InetAddress getActiveInterfaceAddresses()
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
	  }*/
}