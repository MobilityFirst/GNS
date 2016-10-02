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

package edu.umass.cs.msocket;

import java.io.IOException;
import java.io.InputStream;

import edu.umass.cs.msocket.logger.MSocketLogger;

/**
 * This class implements the InputStream of the MSocket.
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */

public class MWrappedInputStream extends InputStream
{
  private ConnectionInfo cinfo = null;

  MWrappedInputStream(ConnectionInfo cinfo) throws IOException
  {
    this.cinfo = cinfo;
  }

  public synchronized int read(byte[] b) throws IOException
  {
    int numread = read(b, 0, b.length);
    if (numread > 0)
    {
      MSocketLogger.getLogger().fine("msocket read " + numread);
    }
    return numread;
  }

  /**
    *
    */
  public synchronized int read(byte[] b, int offset, int length) throws IOException
  {
    if (cinfo == null)
      throw new IOException("Connection must be established before a write");
    if (cinfo.getCloseInOutbuffer() == true)
      throw new IOException("Don't read after closing the socket");
    if (cinfo.getMSocketState() == MSocketConstants.CLOSED)
      throw new IOException(" socket already closed");
    
    MSocketLogger.getLogger().fine(cinfo.getServerOrClient()+" app read called");
    int nread = 0;
    
    while(nread == 0)
    {
	    cinfo.setState(ConnectionInfo.READ_WRITE, true);
	    
	    try
	    {
	      nread = singleRead(b, offset, length);
	      cinfo.setState(ConnectionInfo.ALL_READY, true);
	    }
	    catch (IOException e)
	    {
	      cinfo.setState(ConnectionInfo.ALL_READY, true);
	
	      if (cinfo.getMSocketState() == MSocketConstants.CLOSED)
	      {
	        throw new IOException("socket already closed");
	      }
	    }
	
	    // FIXME: if close message is not encountered or issued, then abrupt
	    // disconnection return 0, so that it keeps on reading, check if returning 0
	    // doesn't cause any issue
	    if (nread == -1)
	    {
	      nread = 0;
	    }
	    
	    // reading returned 0, and the state is also not active.
	    // other side has closed socket and no more data will arrive
	    // safe to return -1 on reads
	    if(nread == 0 )
	    {
	    	if (cinfo.getMSocketState() != MSocketConstants.ACTIVE) 
	    	{
	    		return -1;
	    	} else {
	    		MSocketLogger.getLogger().fine(cinfo.getServerOrClient()+" nread == 0, need to check for blocking");
			      
			      // if state is not active, then it means other side
			      // has issued a close, and all the data should be there
			      // no need to block on selector 
	    		 synchronized(cinfo.getInputStreamSelectorMonitor())
	    		 {
	    			 cinfo.blockOnInputStreamSelector();
	    		 }
	    	}
	    }
    }
    
    return nread;
  }
  
  public synchronized int nonBlockingRead(byte[] b, int offset, int length) throws IOException
  {
    if (cinfo == null)
      throw new IOException("Connection must be established before a write");
    if (cinfo.getCloseInOutbuffer() == true)
      throw new IOException("Don't read after closing the socket");
    if (cinfo.getMSocketState() == MSocketConstants.CLOSED)
      throw new IOException(" socket already closed");
    
    MSocketLogger.getLogger().fine(cinfo.getServerOrClient()+" app read called");
    int nread = 0;
    
  
    cinfo.setState(ConnectionInfo.READ_WRITE, true);
    
    try
    {
      nread = singleRead(b, offset, length);
      cinfo.setState(ConnectionInfo.ALL_READY, true);
    }
    catch (IOException e)
    {
      cinfo.setState(ConnectionInfo.ALL_READY, true);

      if (cinfo.getMSocketState() == MSocketConstants.CLOSED)
      {
        throw new IOException("socket already closed");
      }
    }

    // FIXME: if close message is not encountered or issued, then abrupt
    // disconnection return 0, so that it keeps on reading, check if returning 0
    // doesn't cause any issue
    if (nread == -1)
    {
      nread = 0;
    }
    
    // reading returned 0, and the state is also not active.
    // other side has closed socket and no more data will arrive
    // safe to return -1 on reads
    if(nread == 0 )
    {
    	if (cinfo.getMSocketState() != MSocketConstants.ACTIVE) 
    	{
    		return -1;
    	}
    }
    
    return nread;
  }

  /**
   * @Override
   * @see java.io.InputStream#read()
   */
  public int read() throws IOException
  {
	int numRead = 0;
	do{
		    byte[] oneByte = new byte[1];
		    numRead = read(oneByte);
		    if (numRead == 1)
		    {
		    	int num = (int)oneByte[0] & 0x000000FF;
		    	return num;
		    }
		    else if(numRead == -1) {
		    	return -1;
		    }
	} while(numRead==0);
	assert true;
	return numRead;
  }

  // private methods
  /**
   * Reads either the entire length requested if possible, 
   * else reads up to dataBoundarySeq,
   * which could be 0 if the next byte is dataBoundarySeq. 
   * Returns -1 if EOF encountered.
   * @param b
   * @param offset
   * @param length
   * @return
   * @throws IOException
   */
  private int singleRead(byte[] b, int offset, int length) throws IOException {
	   	  int bytesReadInAppBuffer=0;
	   	  
	      MSocketInstrumenter.updateMaxInbufferSize(cinfo.getInBufferSize());
	   	  
	      long inbrStart = System.currentTimeMillis();
	   	  bytesReadInAppBuffer= cinfo.readInBuffer(b, offset, length);
	   	  long inbrEnd = System.currentTimeMillis();
	   	  
	   	  MSocketInstrumenter.addInbufferReadSample((inbrEnd-inbrStart));
	   	  
	      if(bytesReadInAppBuffer>0)
	       {
	           cinfo.updateDataAckSeq(bytesReadInAppBuffer);   
	       } else 
	       {
		       long msrStart = System.currentTimeMillis();
		       bytesReadInAppBuffer = cinfo.multiSocketRead(b, offset, length);
		       long msrEnd = System.currentTimeMillis();
		       
		       MSocketInstrumenter.addMultiSocketReadSample((msrEnd-msrStart));
		           
		       if(bytesReadInAppBuffer>0)
		       {
		           cinfo.updateDataAckSeq(bytesReadInAppBuffer);
		       }
	       }
	     return bytesReadInAppBuffer;
  }

  // commented code
}