package edu.umass.cs.gnsclient.benchmarking;

import edu.umass.cs.gnsclient.client.GuidEntry;


public class InsertClass extends AbstractRequestSendingClass
{
	//private Random updateRand;
	public InsertClass()
	{
		super(SelectCallBenchmarking.INSERT_LOSS_TOLERANCE);
		//updateRand = new Random();
	}
	
	@Override
	public void run()
	{
		try
		{
			this.startExpTime();
			updRateControlledRequestSender();
		} catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	private void updRateControlledRequestSender() throws Exception
	{
		//double reqspms = MySQLThroughputBenchmarking.requestsps/1000.0;
		double reqspms = 100.0/1000.0;
		long currTime  = 0;
		
		// sleep for 100ms
		double numberShouldBeSentPerSleep = reqspms*100.0;
		
		double currUserGuidNum   = 1000000;
		int numGuidsInserted = 0;
		while( ( numGuidsInserted < SelectCallBenchmarking.NUM_GUIDs ) )
//		while( ( (System.currentTimeMillis() - expStartTime)
//				< MySQLThroughputBenchmarking.EXPERIMENT_TIME ) )
		{
			for(int i=0; i<numberShouldBeSentPerSleep; i++ )
			{
				//doUpdate( (int)currUserGuidNum );
				insertGUID((int)currUserGuidNum);
				currUserGuidNum++;
				numGuidsInserted++;
				if(numGuidsInserted >= SelectCallBenchmarking.NUM_GUIDs)
					break;
				//numSent++;
			}
			
			currTime = System.currentTimeMillis();
			
			double timeElapsed = ((currTime- expStartTime)*1.0);
			double numberShouldBeSentByNow = timeElapsed*reqspms;
			double needsToBeSentBeforeSleep = numberShouldBeSentByNow - numSent;
			if(needsToBeSentBeforeSleep > 0)
			{
				needsToBeSentBeforeSleep = Math.ceil(needsToBeSentBeforeSleep);
			}
			
			for(int i=0;i<needsToBeSentBeforeSleep;i++)
			{
				//doUpdate((int)currUserGuidNum);
				insertGUID((int)currUserGuidNum);
				
				currUserGuidNum++;
				numGuidsInserted++;
				if(numGuidsInserted >= SelectCallBenchmarking.NUM_GUIDs)
					break;
				//numSent++;
			}			
			Thread.sleep(100);
		}
		
		long endTime = System.currentTimeMillis();
		double timeInSec = ((double)(endTime - expStartTime))/1000.0;
		double sendingRate = (numSent * 1.0)/(timeInSec);
		System.out.println("Insert eventual sending rate "+sendingRate);
		
		waitForFinish();
		double endTimeReplyRecvd = System.currentTimeMillis();
		double sysThrput= (numRecvd * 1000.0)/(endTimeReplyRecvd - expStartTime);
		
		System.out.println("Insert result:Goodput "+sysThrput);
	}
	
//	public static void insertGUIDs()
//	{
//		for( int i=0; i<NUM_GUIDs; i++ )
//		{
//			String guidAlias = GUID_PREFIX + i;
//			
//			try
//			{
//				double randLat = LATITUDE_MIN + randomGen.nextDouble()*(LATITUDE_MAX - LATITUDE_MIN);
//				double randLong = LONGITUDE_MIN + randomGen.nextDouble()*(LONGITUDE_MAX - LONGITUDE_MIN);
//				JSONObject updateJSON = new JSONObject();
//				updateJSON.put(Latitude_Name, randLat);
//				updateJSON.put(Longitude_Name, randLong);
//				
//				//JSONArray array = new JSONArray(Arrays.asList(randLong, randLat));
//				System.out.println("Creating GUID alias "+guidAlias);
//				GuidEntry guidEntry = client.guidCreate(account_guid, guidAlias);
//				client.setLocation(guidEntry, randLong, randLat);
//			}
//			catch (Exception e)
//			{
//				e.printStackTrace();
//			}
//		}
//		
//		JSONArray wholeRegion = new JSONArray();
//		wholeRegion.put(UPPER_LEFT);
//		wholeRegion.put(LOWER_RIGHT);
//		
//		InsertTask updTask = new InsertTask( guid, attrValJSON, this);
//		MySQLThroughputBenchmarking.taskES.execute(updTask);
//		
//		// a test select
//		try
//		{
//			JSONArray resultArray = client.selectWithin
//						(GNSCommandProtocol.LOCATION_FIELD_NAME , wholeRegion);
//			System.out.println("Total guids returned "+resultArray.length());
//			assert(resultArray.length() == NUM_GUIDs);	
//		} 
//		catch (Exception e)
//		{
//			e.printStackTrace();
//		}
//	}
	
	public static void insertGUID(int guidNum)
	{
		String guidAlias = SelectCallBenchmarking.GUID_PREFIX + guidNum;
		
		try
		{
			double randLat = SelectCallBenchmarking.LATITUDE_MIN 
					+ SelectCallBenchmarking.randomGen.nextDouble()*(SelectCallBenchmarking.LATITUDE_MAX - SelectCallBenchmarking.LATITUDE_MIN);
			double randLong = SelectCallBenchmarking.LONGITUDE_MIN 
					+ SelectCallBenchmarking.randomGen.nextDouble()*(SelectCallBenchmarking.LONGITUDE_MAX - SelectCallBenchmarking.LONGITUDE_MIN);
//			JSONObject updateJSON = new JSONObject();
//			updateJSON.put(Latitude_Name, randLat);
//			updateJSON.put(Longitude_Name, randLong);
			
			//JSONArray array = new JSONArray(Arrays.asList(randLong, randLat));
			System.out.println("Creating GUID alias "+guidAlias);
			GuidEntry guidEntry = SelectCallBenchmarking.client.guidCreate
						(SelectCallBenchmarking.account_guid, guidAlias);
			SelectCallBenchmarking.client.setLocation(guidEntry, randLong, randLat);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	@Override
	public void incrementUpdateNumRecvd(String userGUID, long timeTaken)
	{
		synchronized(waitLock)
		{
			numRecvd++;
			System.out.println("Insert reply recvd "+userGUID+" time taken "+timeTaken+
					" numSent "+numSent+" numRecvd "+numRecvd);
			//if(currNumReplyRecvd == currNumReqSent)
			if( checkForCompletionWithLossTolerance(numSent, numRecvd) )
			{
				waitLock.notify();
			}
		}
	}
	
	@Override
	public void incrementSearchNumRecvd(int resultSize, long timeTaken)
	{
	}
}