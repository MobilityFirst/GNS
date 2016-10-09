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
import java.io.InputStream;

import edu.umass.cs.msocket.MSocket;

public class ContextSocketInputStream extends InputStream
{
	// maximum size of read chunk
	private static final int READ_CHUNK 						= 1000;
	
	private final MSocket thisMSocket;
	
	// indicates num of bytes remaining to be
	// read form last message.
	// The inputstream reads bytes in chunks
	// dones't read all bytes in chunks.
	private int numBytesRem 									= 0;
	
	public ContextSocketInputStream(MSocket thisMSocket)
	{
		this.thisMSocket = thisMSocket;
	}
	
	@Override
	public int read() throws IOException
	{
		byte[] onebyte = new byte[1];
		int numBytes = readInternal(onebyte, 0, onebyte.length);
		if(numBytes == 1)
		{
			int ret= 0;
			ret = ret | onebyte[0];
			return ret;
		}
		return 0;
	}
	
	@Override
	public int read(byte[] b) throws IOException
	{
		//return read(b, 0, b.length);
		return readInternal(b, 0, b.length);
	}
	
	@Override
	public int read(byte[] b, int off, int len) throws IOException
	{
		return readInternal(b, off, len);
	}
	
	private int readInternal(byte[] userByteArray, int offset, int length)
	{
		int numBytesRead    = 0;
		try
		{
			if( numBytesRem == 0 )
			{
				ContextSocketMessage mesgHeader = ContextSocketMessage.readDataMessageHeader(thisMSocket);
				if( mesgHeader != null)
				{
					numBytesRem = mesgHeader.length;
					if( numBytesRem > 0 )
					{
						int bytesToRead = 0;
						if( numBytesRem > READ_CHUNK )
						{
							bytesToRead = READ_CHUNK;
						}
						else
						{
							bytesToRead = numBytesRem;
						}
						
						if( bytesToRead > length)
						{
							bytesToRead = length;
						}
						
						//byte[] readBuf = new byte[bytesToRead];
						//System.out.println("Reading again "+bytesToRead);
						
						int numRead = thisMSocket.getInputStream().read(userByteArray, offset, bytesToRead);
						numBytesRead = numRead;
						if(numRead > 0)
						{
							//byte[] storeBuf = new byte[numRead];
							//System.arraycopy(readBuf, 0, storeBuf, 0, numRead);
							
							//readQueueOperations(QUEUE_PUSH, storeBuf);
							//System.out.println("setNumBytesRem "+(numBytesRem-numRead));
							numBytesRem = numBytesRem-numRead;
						}
					}
				}
			 }
			else
			{
				int bytesToRead = 0; 
				if(numBytesRem > READ_CHUNK)
				{
					bytesToRead = READ_CHUNK;
				}
				else
				{
					bytesToRead = numBytesRem;
				}
				
				if( bytesToRead > length)
				{
					bytesToRead = length;
				}
				
				int numRead = thisMSocket.getInputStream().read(userByteArray, offset, bytesToRead);
				numBytesRead = numRead;
				if(numRead > 0)
				{
					//byte[] storeBuf = new byte[numRead];
					//System.arraycopy(readBuf, 0, storeBuf, 0, numRead);
					//readQueueOperations(QUEUE_PUSH, storeBuf);
					numBytesRem = numBytesRem-numRead;
				}
			}
		} catch (IOException e)
		{
			e.printStackTrace();
		} catch (Exception e)
		{
			e.printStackTrace();
		}
		return numBytesRead;
	}
	
	
	/*private class ReadTask implements Runnable 
	{
		private final ContextSocket readMSocketInfo;
		public ReadTask( ContextSocket readMSocketInfo )
		{
			this.readMSocketInfo = readMSocketInfo;
		}
		
		@Override
		public void run()
		{	
				MSocket readMSocket = readMSocketInfo.getAccptMSocket();
				
				while(true)
				{
					int numBytesRem = readMSocketInfo.getNumBytesRem();
					try
					{
						if(numBytesRem == 0)
						{
							//System.out.println("Reading again dm header ");
							ContextSocketMessage mesgHeader = ContextSocketMessage.readDataMessageHeader(readMSocket);
							if( mesgHeader != null)
							{
								readMSocketInfo.setNumBytesRem(mesgHeader.length);
								numBytesRem = readMSocketInfo.getNumBytesRem();
								if( numBytesRem > 0 )
								{
									int bytesToRead = 0;
									if(numBytesRem > READ_CHUNK)
									{
										bytesToRead = READ_CHUNK;
									}
									else
									{
										bytesToRead = numBytesRem;
									}
									
									byte[] readBuf = new byte[bytesToRead];
									
									//System.out.println("Reading again "+bytesToRead);
									int numRead = readMSocket.getInputStream().read(readBuf, 0, readBuf.length);
									if(numRead > 0)
									{
										byte[] storeBuf = new byte[numRead];
										System.arraycopy(readBuf, 0, storeBuf, 0, numRead);
										
										readQueueOperations(QUEUE_PUSH, storeBuf);
										//System.out.println("setNumBytesRem "+(numBytesRem-numRead));
										readMSocketInfo.setNumBytesRem(numBytesRem-numRead);
									}
								}
							}
						 }
						else
						{
							int bytesToRead = 0; 
							if(numBytesRem > READ_CHUNK)
							{
								bytesToRead = READ_CHUNK;
							}
							else
							{
								bytesToRead = numBytesRem;
							}
							
							byte[] readBuf = new byte[bytesToRead];
							
							int numRead = readMSocket.getInputStream().read(readBuf, 0, readBuf.length);
							if(numRead > 0)
							{
								byte[] storeBuf = new byte[numRead];
								System.arraycopy(readBuf, 0, storeBuf, 0, numRead);
								
								readQueueOperations(QUEUE_PUSH, storeBuf);
								readMSocketInfo.setNumBytesRem(numBytesRem-numRead);
							}
						}
						
						synchronized(queueMonitor)
						{
							queueMonitor.notifyAll();
						}
					} catch (IOException e) 
					{
						e.printStackTrace();
					} catch (Exception e)
					{
						e.printStackTrace();
					}
			}
		}
	}*/
	
	/**
	 * reads from any of the writer and returns the byte array
	 * @return
	 * @throws IOException
	 */
	/*public byte[] readAny() throws IOException
	{
//		Vector<MSocket> currMSockets = new Vector<MSocket>();
//		currMSockets.addAll( (Collection<? extends MSocket>) msocketGroupMemberInternalsObj.memberConnectionMapOperations(
//				MSocketGroupMemberInternals.GET_ALL, "", null));
//		MSocketLogger.getLogger().fine("currMSockets size "+currMSockets.size());
//		
//		synchronized(msocketGroupMemberInternalsObj.accptMonitor)
//		{
//			while(currMSockets.size() == 0)
//			{
//				try 
//				{
//					msocketGroupMemberInternalsObj.accptMonitor.wait();
//				} catch (InterruptedException e) 
//				{
//					e.printStackTrace();
//				}
//				currMSockets.clear();
//				currMSockets.addAll( (Collection<? extends MSocket>) msocketGroupMemberInternalsObj.memberConnectionMapOperations(
//						MSocketGroupMemberInternals.GET_ALL, "", null));
//				MSocketLogger.getLogger().fine("currMSockets size "+currMSockets.size());
//			}
//		}
		
		synchronized(msocketGroupMemberInternalsObj.queueMonitor)
		{
			while( (Integer)msocketGroupMemberInternalsObj.readQueueOperations(ContextMemberInternals.QUEUE_SIZE, null) == 0 ) 
			{	
				try {
					msocketGroupMemberInternalsObj.queueMonitor.wait();
				} catch (InterruptedException e) {

					e.printStackTrace();
				}
			}
		}
		byte[] retByte = (byte[]) msocketGroupMemberInternalsObj.readQueueOperations(ContextMemberInternals.QUEUE_POP, null);
	 	return retByte;
	}*/
}