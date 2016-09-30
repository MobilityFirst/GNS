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

package edu.umass.cs.msocket.contextsocket;

import java.io.IOException;
import java.nio.ByteBuffer;

import edu.umass.cs.msocket.MSocket;
import edu.umass.cs.msocket.logger.MSocketLogger;

public class ContextSocketMessage 
{
	public static final int      DATA_MESG     = 1;
	
	public static final String[] Mesg_Type_Str = {"DATA_MESG"};

	public static final int      HEADER_SIZE   = (Integer.SIZE * 2) / 8;
	final int                    type;
	final int                    length;
	  
	final byte[]                 msg;
	  
	private final int arrayCopyOffset;
	
	  /*
	   * If the byte[] argument b is null or longer than the specified length
	   * argument l, then length is set to l; else length is shortened to b.length.
	   * We need to allow length>0 and msg==null in the case of a header-only
	   * DataMessage.
	   */
	  public ContextSocketMessage(int type, int l, byte[] b, int offset)
	  {
	    this.type = type;
	    if (b == null || l <= b.length)
	      length = l;
	    else
	      length = b.length;
	    arrayCopyOffset = offset;
	    msg = b;
	  }
	  
	  public static int sizeofHeader()
	  {
		  return HEADER_SIZE;
	  }
	  
	  public int size()
	  {
		  return sizeofHeader() + length;
	  }
	  
	  public byte[] getBytes()
	  {
	    ByteBuffer buf = ByteBuffer.allocate(ContextSocketMessage.HEADER_SIZE + (msg != null ? length : 0));
	    buf.putInt(type);
	    buf.putInt(length);
	    if (msg != null)
	      {
	    	buf.put(msg, arrayCopyOffset, length);
	    	if(length>0)
	    	{
	    		MSocketLogger.getLogger().fine("DataMessage: msg[0] "+msg[0]);
	    	}
	      }
	    buf.flip();
	    return buf.array();
	  }
	
	  /*
	   * This method assumes that the byte[] argument b exactly contains a
	   * DataMessage object, i.e., there is no excess bytes beyond the header and
	   * the message body. If that is not the case, it will return null.
	   */
	  public static ContextSocketMessage getDataMessage(byte[] b)
	  {
	    if (b == null || b.length < ContextSocketMessage.HEADER_SIZE)
	      return null;
	    ByteBuffer buf = ByteBuffer.wrap(b);
	    // not coying the buffer
	    ContextSocketMessage dm = new ContextSocketMessage(buf.getInt(), buf.getInt(), b, ContextSocketMessage.HEADER_SIZE);
	    //Arrays.copyOfRange(b, GeocastMessage.HEADER_SIZE, b.length), 0);
	    return dm;
	  }
	
	  public static ContextSocketMessage getDataMessageHeader(byte[] b)
	  {
	    if (b == null || b.length < ContextSocketMessage.HEADER_SIZE)
	      return null;
	    ByteBuffer buf = ByteBuffer.wrap(b, 0, ContextSocketMessage.HEADER_SIZE);
	    return new ContextSocketMessage(buf.getInt(), buf.getInt(), null, -1);
	  }
	  
	  public String toString()
	  {
	    String s = "";
	    s += type + length + ", " + (msg != null ? new String(msg) : "");
	    return s;
	  }
	  
	  public static ContextSocketMessage readDataMessageHeader(MSocket readMSocket) throws IOException
	  {
		    int nreadHeader = 0;
		    byte[] readBuf = new byte[ContextSocketMessage.sizeofHeader()];
		    
		    //ByteBuffer buf = ByteBuffer.allocate(GeocastMessage.sizeofHeader());
		    ContextSocketMessage gm = null;
		    
		    do
		    {
		      int cur = 0;
		      cur = readMSocket.getInputStream().read(readBuf, nreadHeader, ContextSocketMessage.sizeofHeader()-nreadHeader);
		      if (cur != -1)
		      {
		    	  nreadHeader += cur;
		      }
		      else
		      {
		    	  break;
		      }
		    }while ((nreadHeader > 0) && (nreadHeader != ContextSocketMessage.sizeofHeader()));
		    
		    if (nreadHeader == ContextSocketMessage.sizeofHeader())
		    {
		    	gm = ContextSocketMessage.getDataMessageHeader(readBuf);
		    }
		    return gm;
	  }
	  
	  public static void main(String[] args)
	  {
	    byte[] b = "Testing the waters to get a feel".getBytes();
	    ContextSocketMessage dm = new ContextSocketMessage(0,b.length, b, 0);
	    byte[] enc = dm.getBytes();
	
	    ContextSocketMessage dec = ContextSocketMessage.getDataMessage(enc);
	    enc[11] = 98;
	    MSocketLogger.getLogger().fine(dec.toString());
	  }
}