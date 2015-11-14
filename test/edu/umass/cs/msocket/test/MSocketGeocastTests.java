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

package edu.umass.cs.msocket.test;

/**
 * Tests basic context based communication primitive.
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class MSocketGeocastTests
{
	/*private static final String MEMBER_NAME 			= "geocastTestMemberLap";
	private static final String WRITER_NAME 			= "geocastTestWriterLap";
	
	//private static final int    LOCAL_PORT 			= 5454;
	//private static final String LOCALHOST  			= "127.0.0.1";
	private ContextMember       mGroupMem;
	private static Logger       log 					= Logger.getLogger("MSocketGeocastTests");
	
	  @Before
	  public void setUp() throws Exception
	  {
	    BasicConfigurator.configure();
	    log.getRootLogger().setLevel(Level.OFF);
	    log.addAppender(new ConsoleAppender());
	    mGroupMem = new ContextMember(MEMBER_NAME);
	    mGroupMem.setAttributes("contextATT0", 50);
	    EchoGroupMemThread accepterThread = new EchoGroupMemThread(mGroupMem);
	    accepterThread.start();
	  }
	  
	 @After
	  public void tearDown() throws Exception
	  {
		  //mss.close();
		  //FIXME: need some way to close group member and group writer
	  }
	  
	  @Test
	  public void geocastTest()
	  {
	    try
	    {
	      long start = System.currentTimeMillis();
	      ContextWriter msGrpWriter = new ContextWriter(WRITER_NAME, "2 <= contextATT0 <= 140");
	      // connect to the group members
	      long end = System.currentTimeMillis();
	      
	      System.out.println("Query time "+(end - start));
	      
	      ContextWriterOutputStream msocketGrpWritOut 
	      						= (ContextWriterOutputStream) msGrpWriter.getOutputStream();
	      
	      ContextWriterInputStream msocketGrpWritIn 
								= (ContextWriterInputStream) msGrpWriter.getInputStream();
	      
	      int count = 0;
	      while(true)
	      {
	    	  String sent = "Message "+count++;
	    	  msocketGrpWritOut.write(sent.getBytes());
	    	  byte[] bytesRead = msocketGrpWritIn.readAny();
	    	  if(bytesRead !=null)
	    	  System.out.println(new String(bytesRead));
	    	  
	    	  Thread.sleep(5000);
	      }
	    }
	    catch (Exception e)
	    {
	      e.printStackTrace();
	      fail("geocastTest failed");
	    }
	  }
	  
	  private void sleepFor(int ms)
	  {
	    try
	    {
	      Thread.sleep(ms);
	    }
	    catch (InterruptedException e)
	    {
	      e.printStackTrace();
	    }
	  }
	  
	  private void echoTest(InputStream is, OutputStream os, int ms) throws IOException
	  {
	    sleepFor(ms);
	    int value = new Random().nextInt(255);
	    log.info("MSocket writing " + value);
	    os.write(value);
	    os.flush();
	    sleepFor(ms);
	    int read = is.read();
	    log.info("MSocket read " + read);
	    if (read != value)
	      fail("Wrong value returned by echo server. Expected " + value + " but got " + read);
	  }
	  
	  private class EchoGroupMemThread extends Thread
	  {
	    private ContextMember mGroupMem;
	    
	    public EchoGroupMemThread(ContextMember mGroupMem)
	    {
	      super("Echo server thread");
	      this.mGroupMem = mGroupMem;
	    }
	    
	    public void run()
	    {
	      try
	      {
	    	ContextSocketInputStream msGrpInputStr 
	    				= (ContextSocketInputStream)mGroupMem.getInputStream();
	    	
	    	ContextSocketOutputStream msGrpOutputStr 
						= (ContextSocketOutputStream)mGroupMem.getOutputStream();
	    	
	    	while(true)
	    	{
	    		byte[] readBytes = msGrpInputStr.readAny();
	    		System.out.println(new String(readBytes));
	    		String echoStr = "echo "+ new String(readBytes);
	    		byte[] echoBytes = echoStr.getBytes();
	    		msGrpOutputStr.write(echoBytes, 0, echoBytes.length);
	    	}
	      }
	      catch (IOException e)
	      {
	    	  log.error("MServerSocket side error", e);
	    	  fail("MServerSocket side error");
	      }
	   }
	 }
	  */
}