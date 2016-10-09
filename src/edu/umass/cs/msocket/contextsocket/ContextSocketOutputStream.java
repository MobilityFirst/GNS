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
import java.io.OutputStream;

import edu.umass.cs.msocket.MSocket;


public class ContextSocketOutputStream extends OutputStream
{
	private final MSocket thisMSocket;

	public ContextSocketOutputStream(MSocket accptMSocket) 
	{
		this.thisMSocket = accptMSocket;
	}

	@Override
	public void write(int arg0) throws IOException 
	{
		byte[] onebyte = new byte[1];
		onebyte[0] = (byte) (onebyte[0] | arg0);
		write(onebyte);
		//Integer argInt = arg0;
	}
	
	@Override
	public void write(byte[] b) throws IOException 
	{
		write(b, 0, b.length);
	}
	
	@Override
	public void write(byte[] b, int offset, int length) throws IOException
	{	
		ContextSocketMessage gmsg = new ContextSocketMessage(ContextSocketMessage.DATA_MESG, length, b, offset);
		
		byte[] sentByteArray = gmsg.getBytes();
		thisMSocket.getOutputStream().write(sentByteArray, 0, sentByteArray.length);
	}
}


/*private class writeTask implements Runnable 
{
	private final MemberMSocketInfo memberMSocketInfo;
	private final byte[] mesgArray;
	private final int offset;
	private final int length;
	//private MSocketGroupMemberInternals msocketGroupMemberInternalsObj = null;
	
	public writeTask( MemberMSocketInfo writeMSocket, byte[] mesgArray, int offset, int length, 
			ContextMemberInternals msocketGroupMemberInternalsObj )
	{	
		this.memberMSocketInfo = writeMSocket;
		this.mesgArray = mesgArray;
		this.offset = offset;
		this.length = length;
		
		//this.msocketGroupMemberInternalsObj = msocketGroupMemberInternalsObj;
	}
	
	@Override
	public void run()
	{
		if(memberMSocketInfo == null)
		{
			MSocketLogger.getLogger().fine("returned socket info null, should not happen");
		}
		
		synchronized(memberMSocketInfo)
		{
			try 
			{
				MSocket retSocket = memberMSocketInfo.getAccptMSocket();
					
				ContextSocketMessage gmsg = new ContextSocketMessage(ContextSocketMessage.DATA_MESG, length, mesgArray, offset);
				
				byte[] sentByteArray = gmsg.getBytes();
				retSocket.getOutputStream().write(sentByteArray, 0, sentByteArray.length);
			} catch (Exception e)
			{
				MSocketLogger.getLogger().fine("write failed");
				memberMSocketInfo.setAccptMSocket(null);
				e.printStackTrace();
			}
		}
	}
}*/