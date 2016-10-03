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

package edu.umass.cs.msocket.proxy.legacyforwarder;

/**
 * This class implements the proxy forwarding, implements the proxy listening
 * socket and proxy connection splicing thread. main() method of this class
 * starts the proxy forwarding on the node.
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class LegacyProxyForwarder
{
/*  public static final int                   LISTEN_THREAD          = 1;                                               // Proxy
                                                                                                                       // thread
                                                                                                                       // types,
                                                                                                                       // listens
                                                                                                                       // for
                                                                                                                       // connections
                                                                                                                       // at
                                                                                                                       // proxy
  public static final int                   SPLICING_THREAD        = 2;                                               // Proxy
                                                                                                                       // thread
                                                                                                                       // types,
                                                                                                                       // runs
                                                                                                                       // the
                                                                                                                       // selector
                                                                                                                       // on
                                                                                                                       // all
                                                                                                                       // channels
                                                                                                                       // and
                                                                                                                       // splices
                                                                                                                       // the
                                                                                                                       // channels
  public static final int                   GET                    = 1;                                               // GET
                                                                                                                       // PUT
                                                                                                                       // operation
                                                                                                                       // in
                                                                                                                       // shared
                                                                                                                       // data
                                                                                                                       // structures
  public static final int                   PUT                    = 2;
  public static final int                   CONTROL_SOC            = 1;                                               // socket
                                                                                                                       // types
                                                                                                                       // of
                                                                                                                       // the
                                                                                                                       // accepted
                                                                                                                       // sockets
  public static final int                   DATA_SOC               = 2;
  public static final int                   ChannelReadSize        = 10000;                                           // tries
                                                                                                                       // to
                                                                                                                       // read
                                                                                                                       // 1000
                                                                                                                       // bytes
                                                                                                                       // at
                                                                                                                       // once
  public static final int                   keepAliveFreq          = 2;                                               // after
                                                                                                                       // every
                                                                                                                       // 5
                                                                                                                       // sec

  // register queue operations apart from GET and PUT
  public static final int                   SIZE                   = 3;

  private static final int                  TimerTick              = 1000;                                            // TImer
                                                                                                                       // ticks
                                                                                                                       // after
                                                                                                                       // 1000msec

  private int                               ProxyConnectionId      = 1;                                               // ID
                                                                                                                       // at
                                                                                                                       // proxy
                                                                                                                       // uniquely
                                                                                                                       // associated
                                                                                                                       // with
                                                                                                                       // server
                                                                                                                       // and
                                                                                                                       // client
                                                                                                                       // connection

  private static String                     proxyName              = "ananas.cs.umass.edu";
  private int                               proxyPort              = 11989;

  private HashMap<String, ProxyMSocket>     ProxyControlChannelMap = null;                                            // Map
                                                                                                                       // stores
                                                                                                                       // the
                                                                                                                       // controller
                                                                                                                       // channels,
                                                                                                                       // indexed
                                                                                                                       // by
                                                                                                                       // GUID
  private LegacyProxyServerSocket           pServerSocket          = null;
  private HashMap<Integer, ProxyTCPSplicer> SpliceMap              = null;                                            // Map
                                                                                                                       // storing
                                                                                                                       // the
                                                                                                                       // Sockets
                                                                                                                       // from
                                                                                                                       // the
                                                                                                                       // client
                                                                                                                       // and
                                                                                                                       // server
                                                                                                                       // side
                                                                                                                       // which
                                                                                                                       // get
                                                                                                                       // spliced
  private Selector                          SpliceSelector         = null;
  private Timer                             localTimer             = null;
  private long                              localClock             = 0;

  private int                               numBytesSpliced        = 0;

  private Queue<RegisterQueueInfo>          registerQueue          = null;

  private final ExecutorService             pool;
  private boolean                           runstatus              = true;

  private static Logger                     log                    = Logger.getLogger(ProxyForwarder.class.getName());

  public LegacyProxyForwarder(String proxyName, int proxyPort) throws SocketException, IOException
  {
    this.proxyName = proxyName;
    this.proxyPort = proxyPort;
    ProxyControlChannelMap = new HashMap<String, ProxyMSocket>();
    pServerSocket = new LegacyProxyServerSocket(this.proxyName, this.proxyPort, this);
    SpliceMap = new HashMap<Integer, ProxyTCPSplicer>();
    SpliceSelector = Selector.open();

    localTimer = new Timer();
    startLocalTimer();

    registerQueue = new LinkedList<RegisterQueueInfo>();
    pool = Executors.newCachedThreadPool();

    ProxyForwarderThread List_Thr = new ProxyForwarderThread(pServerSocket, LISTEN_THREAD, this);
    (new Thread(List_Thr)).start();
    MSocketLogger.getLogger().fine("Proxy listen thread started");

    ProxyForwarderThread Splice_Thr = new ProxyForwarderThread(pServerSocket, SPLICING_THREAD, this);
    (new Thread(Splice_Thr)).start();
    MSocketLogger.getLogger().fine("Proxy splicing thread started");
  }*/

  /**
   * Returns the address the control channel is listening
   * 
   * @return
   */
  /*public SocketAddress getProxyListeningAddress()
  {
    return pServerSocket.getLocalSocketAddress();
  }*/

  /**
   * function to start proxy forwarder at proxy, it will initialize all other
   * components
   * 
   * @param args, args[0] is the proxy name or IP, args[1] proxy port
   * @throws Exception
   * @throws NumberFormatException
   */
  /*public static void main(String[] args) throws NumberFormatException, Exception
  {
    new ProxyForwarder(args[0], Integer.parseInt(args[1]));
  }

  public Selector getSelector()
  {
    return SpliceSelector;
  }

  public synchronized Object spliceMapOperations(int Key, int Oper, int ServerOrClient, LegacyProxyMSocket Socket)
      throws IOException
  {

    switch (Oper)
    {
      case GET :
      {
        ProxyTCPSplicer Obj = SpliceMap.get(Key);
        if (Obj == null)
        {
          return null;
        }
        else
        {
          return Obj.proxyTCPSplicerOperation(ProxyTCPSplicer.GET, ServerOrClient, Socket);
        }
      }
      case PUT :
      {
        if (SpliceMap.get(Key) == null)
        {
          ProxyConnectionId++;
          ProxyTCPSplicer Obj = new ProxyTCPSplicer(ProxyConnectionId);
          Socket.setProxyInfo(ServerOrClient, Obj.getProxyId());
          Obj.proxyTCPSplicerOperation(ProxyTCPSplicer.PUT, ServerOrClient, Socket);
          SpliceMap.put(Obj.getProxyId(), Obj);

          // register channel
          SocketChannel RegisteredChannel = Socket.getUnderlyingChannel();
          RegisteredChannel.configureBlocking(false);

          MSocketLogger.getLogger().fine("Splice PUT before register");
          RegisterQueueInfo regObj = new RegisterQueueInfo(Socket, RegisteredChannel);
          registerQueueOperations(PUT, regObj);

          MSocketLogger.getLogger().fine("Splice PUT after register");
          return Obj.getProxyId();
        }
        else
        {
          ProxyTCPSplicer Obj = SpliceMap.get(Key);
          Socket.setProxyInfo(ServerOrClient, Obj.getProxyId());
          Obj.ProxyTCPSplicerOperation(ProxyTCPSplicer.PUT, ServerOrClient, Socket);
          SpliceMap.put(Obj.getProxyId(), Obj);

          // register channel
          SocketChannel RegisteredChannel = Socket.getUnderlyingChannel();
          RegisteredChannel.configureBlocking(false);

          MSocketLogger.getLogger().fine("Splice PUT before register");
          RegisterQueueInfo regObj = new RegisterQueueInfo(Socket, RegisteredChannel);
          registerQueueOperations(PUT, regObj);
          
          return Obj.getProxyId();
        }
      }
    }
    return null;
  }

  public synchronized ProxyMSocket ProxyControlChannelMap(int Oper, String Key, ProxyMSocket Socket)
  {
    switch (Oper)
    {
      case GET :
      {
    	System.out.println("Keys in GET "+ProxyControlChannelMap.keySet());
        return ProxyControlChannelMap.get(Key);
      }
      case PUT :
      {
        ProxyControlChannelMap.put(Key, Socket);
        break;
      }
    }
    return null;
  }*/

  /**
   * Thread class that proxy forwarder uses for every task, like listening for
   * connection, forwarding packets
   * 
   * @author ayadav
   */
  /*public class ProxyForwarderThread implements Runnable
  {
    Object         TaskObj       		= null;
    LegacyProxyForwarder PForwarderObj 	= null;
    int            task          		= -1;

    ProxyForwarderThread(Object TaskObj, int Task, LegacyProxyForwarder PForwarderObj)
    {
      this.TaskObj = TaskObj;
      this.task = Task;
      this.PForwarderObj = PForwarderObj;
    }

    public void run()
    {
      switch (task)
      {
        case LISTEN_THREAD :
        {
          while (true)
          {
            try
            {
              ProxyMSocket RetSocket = ((ProxyServerSocket) (TaskObj)).accept();
              String Key = RetSocket.getUnderlyingChannel().socket().getInetAddress().toString();
              Key = Key + ":" + RetSocket.getUnderlyingChannel().socket().getPort();
              MSocketLogger.getLogger().fine("Key for socket map " + RetSocket.getStringGUID());
              if (RetSocket.getSocketType() == ProxyForwarder.CONTROL_SOC)
              {
                MSocketLogger.getLogger().fine("Control Socket inserted into Map ");
                PForwarderObj.ProxyControlChannelMap(ProxyForwarder.PUT, RetSocket.getStringGUID(), RetSocket);
              }
              else if (RetSocket.getSocketType() == ProxyForwarder.DATA_SOC)
              {
              }
            }
            catch (Exception e)
            {
              e.printStackTrace();
            }
          }
        }
        case ProxyForwarder.SPLICING_THREAD :
        {
          while (true)
          {
            try
            {

              // check for the queue, if there are any channels to register
              while (((Integer) registerQueueOperations(SIZE, null) != 0))
              {
                RegisterQueueInfo regSocket = (RegisterQueueInfo) registerQueueOperations(GET, null);

                SelectionKey SelecKey;
                try
                {
                  SelecKey = regSocket.socketChannel.register(SpliceSelector, SelectionKey.OP_READ);
                  SelecKey.attach(regSocket.Socket);
                }
                catch (Exception e)
                {
                  e.printStackTrace();
                }
              }

              int readyChannels = 0;
              try
              {
                readyChannels = PForwarderObj.getSelector().select(); // changing
                                                                      // it to
                                                                      // select(),
                                                                      // makes
                                                                      // it
                                                                      // blocking,
                                                                      // then it
                                                                      // deadlocks
                                                                      // with
                                                                      // register()
                // method on selector and select() method here
              }
              catch (Exception e)
              {
                e.printStackTrace();
              }

              if (readyChannels == 0)
                continue;

              Set<SelectionKey> selectedKeys = PForwarderObj.getSelector().selectedKeys();

              Vector<SelectionKey> SelectKeyVector = new Vector<SelectionKey>();
              SelectKeyVector.addAll(selectedKeys);

              boolean exit = true;
              do
              {
                exit = true;
                for (int i = 0; i < SelectKeyVector.size(); i++)
                {
                  SelectionKey key = SelectKeyVector.get(i);
                  if (key.isReadable())
                  {
                    // Acquiring lock here not inside the thread, as thread
                    // scheduling may not be ensure TCP's sequential property
                    // Two chunks of transmission from selectors may get out of
                    // order with out locks here.
                    ProxyMSocket ChannelObj = (ProxyMSocket) key.attachment();

                    if (ChannelObj.grabLockOrgetBlocked(true)) // acqurie lock,
                                                               // if it fails
                                                               // then ignore
                                                               // this channel
                                                               // for now.
                    {
                      pool.execute(new Handler(key, PForwarderObj));
                    }
                  }
                }
              }
              while (!exit);
              selectedKeys.clear();
            }
            catch (Exception ex)
            {
              ex.printStackTrace();

            }
          }
        }
      }
    }
  }

  private void startLocalTimer()
  {
    localTimer.scheduleAtFixedRate(new TimerTask()
    {
      @Override
      public void run()
      {
        localClock++;

        if (localClock % keepAliveFreq == 0)
        {
          Vector<ProxyMSocket> vect = new Vector<ProxyMSocket>();
          vect.addAll(ProxyControlChannelMap.values());

          for (int i = 0; i < vect.size(); i++)
          {
            ProxyMSocket Obj = vect.get(i);
            try
            {
              if (Obj.workingperations(ProxyMSocket.GET, false) == true)
              {
                MSocketLogger.getLogger().fine("proxy sending keep alive at " + localClock + "remote address "
                    + Obj.getUnderlyingChannel().socket().getRemoteSocketAddress());
                Obj.setupControlWrite(InetAddress.getLocalHost(), -1, -1, -1, SetupControlMessage.KEEP_ALIVE, -1, -1
                		, Obj.getByteGUID(), Obj.getUnderlyingChannel());
              }
            }
            catch (IOException e)
            {
              e.printStackTrace();
              Obj.workingperations(ProxyMSocket.SET, false);
            }
          }
        }
        ProxyLoadStatistics.updateCurrentThroughput(numBytesSpliced);
      }
    }, TimerTick, TimerTick);
  }

  private synchronized Object registerQueueOperations(int oper, RegisterQueueInfo registerObj)
  {
    switch (oper)
    {
      case GET :
      {
        return registerQueue.poll();
      }
      case PUT :
      {
        registerQueue.add(registerObj);
        SpliceSelector.wakeup(); // wakeup method makes the blocking select call
                                 // to return
        // and check for the channels in the queue to register
        break;
      }
      case SIZE :
      {
        return registerQueue.size();
      }
    }
    return null;
  }

  public void StopAcceptPool()
  {
    runstatus = false;
    pool.shutdownNow();
  }*/

  /**
   * for implementing executor service
   * 
   * @author ayadav
   */
  /*private class Handler implements Runnable
  {
    private final SelectionKey   key;
    private final LegacyProxyForwarder pForwarderObj;

    public Handler(SelectionKey key, LegacyProxyForwarder pForwarderObj)
    {
      this.key = key;
      this.pForwarderObj = pForwarderObj;
    }

    public void run()
    {
      // a channel is ready for reading
      ProxyMSocket ChannelObj = (ProxyMSocket) key.attachment();
      SocketChannel SourceChannel = ChannelObj.getUnderlyingChannel();
      SocketChannel DestinationChannel = null;
      try
      {
        int ServerOrClient = -1;
        if (ChannelObj.getServerOrClient() == ProxyTCPSplicer.CLIENT_SIDE)
        {
          ServerOrClient = ProxyTCPSplicer.SERVER_SIDE;
        }
        else if (ChannelObj.getServerOrClient() == ProxyTCPSplicer.SERVER_SIDE)
        {
          ServerOrClient = ProxyTCPSplicer.CLIENT_SIDE;
        }
        DestinationChannel = ((ProxyMSocket) pForwarderObj.spliceMapOperations(ChannelObj.getProxyId(), GET,
            ServerOrClient, null)).getUnderlyingChannel();

        int numread = 0;
        ByteBuffer bytebuf = ByteBuffer.allocate(ChannelReadSize);
        numread = SourceChannel.read(bytebuf);

        if (numread > 0)
        {
          MSocketLogger.getLogger().fine("Splicer: Read from source channel " + numread + "src channel port "
              + SourceChannel.socket().getPort() + " proxyId " + ChannelObj.getProxyId());
          bytebuf.flip();
          while (bytebuf.hasRemaining())
          {
            int numwrite = DestinationChannel.write(bytebuf);
            if (numwrite > 0)
              MSocketLogger.getLogger().fine("Splicer: Written into dest channel " + numwrite + " dest channel port "
                  + DestinationChannel.socket().getPort());
          }
          numBytesSpliced += numread;
        }

      }
      catch (Exception e)
      {

        MSocketLogger.getLogger().fine("Exception in splicing");
        e.printStackTrace();
        key.cancel();
      }

      // release lock here
      ChannelObj.grabLockOrgetBlocked(false);
    }
  }*/
  
}