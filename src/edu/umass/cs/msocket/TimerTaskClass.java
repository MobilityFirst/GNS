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

package edu.umass.cs.msocket;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;
import java.util.Vector;

import edu.umass.cs.msocket.common.CommonMethods;
import edu.umass.cs.msocket.gns.Integration;
import edu.umass.cs.msocket.logger.MSocketLogger;

class TimerTaskClass
{
  //private final MSocket    			mSocket;
  private final ConnectionInfo 		cinfo;
  /**
   * Proxy failure timeout in seconds (default is 15 seconds)
   */
  private static final int proxyFailureTimeout = MSocket.KEEP_ALIVE_FREQ*3;

  /**
   * Creates a new <code>TimerTaskClass</code> object
   * 
   * @param mSocket
   */
  TimerTaskClass(ConnectionInfo cinfo)
  {
	  //this.mSocket = mSocket;
	  this.cinfo = cinfo;
  }

  private void proxyFailureCheck() throws Exception
  {
    MSocketLogger.getLogger().fine("inside proxyFailureCheck");
    Vector<SocketInfo> vect = new Vector<SocketInfo>();
    vect.addAll(cinfo.getAllSocketInfo());
    MSocketLogger.getLogger().fine("vect size " + vect.size());
    int i = 0;
    while ( i < vect.size() )
    {
      SocketInfo value = vect.get(i);

      MSocketLogger.getLogger().fine("proxyFailureCheck running " + value.getLastKeepAlive() + " " + KeepAliveStaticThread.getLocalClock());
      {
        if (((KeepAliveStaticThread.getLocalClock() - value.getLastKeepAlive()) > proxyFailureTimeout))
        /*
         * && ( ! checkProxyAddressInGNS ( value ) )
         */
        /*
         * if GNS has same proxy address, then do not migrate
         */
        {
          System.out.println(cinfo.getServerOrClient()+" Inside handleProxyFailure getLocalClock " + KeepAliveStaticThread.getLocalClock() + 
        		  " getLastKeepAlive " + value.getLastKeepAlive() + "Socket Id " + value.getSocketIdentifer());
          if (CommonMethods.getActiveInterfaceInetAddresses().size() > 0)
            handleProxyFailure(value);
        }
      }
      i++;
    }
  }

  /**
   * query GNS, and migrate the specific path
   * 
   * @throws Exception
   */
  private void handleProxyFailure(SocketInfo SocketObj) throws Exception
  {
      System.out.println(cinfo.getServerOrClient()+ " Inside handleProxyFailure getLocalClock " + KeepAliveStaticThread.getLocalClock() + 
    		  " getLastKeepAlive " + SocketObj.getLastKeepAlive());

      InetSocketAddress proxyAddress = getActiveProxyAddress();
      if (proxyAddress == null)
      {
    	  proxyAddress = new InetSocketAddress(cinfo.getServerIP(), cinfo.getServerPort());
      }
      Vector<InetAddress> activeInterfaces = CommonMethods.getActiveInterfaceInetAddresses();
      if (activeInterfaces.size() > 0)
      {
        if (proxyAddress != null)
        {
          MSocketLogger.getLogger().fine(" migrateSocketwithId called with proxy address " + proxyAddress.getAddress() + ":"
              + proxyAddress.getPort() + " socketId " + SocketObj.getSocketIdentifer() );

          cinfo.closeAll(SocketObj.getSocketIdentifer());
          MSocketLogger.getLogger().fine("close done");
          cinfo.migrateSocketwithId(proxyAddress.getAddress(), proxyAddress.getPort(),
              SocketObj.getSocketIdentifer(), MSocketConstants.SERVER_MIG);
        }
      }
  }

  /**
   * queries GNS and returns one active proxy address
   * 
   * @return
   * @throws Exception
   */
  private InetSocketAddress getActiveProxyAddress() throws Exception
  {
    Random rand = new Random();
    InetSocketAddress sockAdd = null;
    try
    {
      List<InetSocketAddress> socketAddressFromGNS = Integration.getSocketAddressFromGNS(
          cinfo.getServerAlias());

      sockAdd = socketAddressFromGNS.get(rand.nextInt(socketAddressFromGNS.size()));
    }
    catch (Exception ex)
    {
      MSocketLogger.getLogger().fine("GnsIntegration.getSocketAddressFromGNS exception");
    }
    return sockAdd;
  }
  
  
  public void run()
  {
      switch (cinfo.getServerOrClient())
      {
        case MSocketConstants.SERVER :
        {
          boolean ret = cinfo.setState(ConnectionInfo.READ_WRITE, false); // timer
          																			   // cannot
          																			   // block
          																			   // to
          																			   // read
          																			   // keep
          																			   // alives
          if (ret)
          {
            cinfo.multiSocketKeepAliveRead();
			  
			cinfo.sendKeepAliveOnAllPaths();
			  
            cinfo.setState(ConnectionInfo.ALL_READY, false);
          }
          break;
        }
        case MSocketConstants.CLIENT :
        {
			boolean ret = cinfo.setState(ConnectionInfo.READ_WRITE, false); // timer
			//
			// cannot block to read keep alives
			//
			if (ret)
			{
			  
			  cinfo.multiSocketKeepAliveRead();
			  
			  cinfo.sendKeepAliveOnAllPaths();
			  
			  cinfo.setState(ConnectionInfo.ALL_READY, false);
			}
			MSocketLogger.getLogger().fine("client reading keep alive complete");
            
			try
			{
				proxyFailureCheck();
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
			catch (InterruptedException e)
			{
				e.printStackTrace();
			}
			catch (Exception e)
			{
				MSocketLogger.getLogger().fine("Mostly GNS connectivity failure, or migration failure");
				e.printStackTrace();
			}		
			break;
        }
      }
  }
  
  // commented code
  /*public void run()
  {
    while (this.mSocket.cinfo.getTimerStatus())
    {
      switch (this.mSocket.cinfo.getServerOrClient())
      {
        case MSocketConstants.SERVER :
        {
          MSocket.MSocketLogger.getLogger().fine("Timer acquiring READ_WRITE at SERVER " + this.mSocket.cinfo.getMSocketState());
          boolean ret = this.mSocket.cinfo.setState(ConnectionInfo.READ_WRITE, false); // timer
          																			   // cannot
          																			   // block
          																			   // to
          																			   // read
          																			   // keep
          																			   // alives
          if (ret)
          {
            this.mSocket.cinfo.multiSocketKeepAliveRead();
			  
			this.mSocket.cinfo.sendKeepAliveOnAllPaths();
			  
            this.mSocket.cinfo.setState(ConnectionInfo.ALL_READY, false);
          }
          break;
        }
        case MSocketConstants.CLIENT :
        {
	        MSocket.MSocketLogger.getLogger().fine("clienttt reading keep alive");
			boolean ret = this.mSocket.cinfo.setState(ConnectionInfo.READ_WRITE, false); // timer
			//
			// cannot block to read keep alives
			//
			if (ret)
			{
			  MSocket.MSocketLogger.getLogger().fine("got the lock");
			  
			  this.mSocket.cinfo.multiSocketKeepAliveRead();
			  
			  this.mSocket.cinfo.sendKeepAliveOnAllPaths();
			  
			  this.mSocket.cinfo.setState(ConnectionInfo.ALL_READY, false);
			}
			MSocket.MSocketLogger.getLogger().fine("client reading keep alive complete");
            
			try
			{
				proxyFailureCheck();
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
			catch (InterruptedException e)
			{
				e.printStackTrace();
			}
			catch (Exception e)
			{
				MSocket.MSocketLogger.getLogger().fine("Mostly GNS connectivity failure, or migration failure");
				e.printStackTrace();
			}
			
			break;
        }
      }

      try
      {
        Thread.sleep(MSocket.KEEP_ALIVE_FREQ * 1000);
      }
      catch (InterruptedException e)
      {
        e.printStackTrace();
      }
    }
  }*/
  
}