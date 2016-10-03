package edu.umass.cs.gnsclient.benchmarking;

import java.util.Timer;
import java.util.TimerTask;


public abstract class AbstractRequestSendingClass implements Runnable
{
	protected long expStartTime;
	protected final Timer waitTimer;
	protected final Object waitLock = new Object();
	protected long numSent;
	protected long numRecvd;
	protected boolean threadFinished;
	protected final Object threadFinishLock = new Object();
	
	// 1% loss tolerance
	private final double lossTolerance;
	
	public AbstractRequestSendingClass( double lossTolerance )
	{
		threadFinished = false;
		this.lossTolerance = lossTolerance;
		numSent = 0;
		numRecvd = 0;
		waitTimer = new Timer();
	}
	
	protected void startExpTime()
	{
		expStartTime = System.currentTimeMillis();
	}
	
	protected void waitForFinish()
	{
		waitTimer.schedule(new WaitTimerTask(), SelectCallBenchmarking.WAIT_TIME);
		
		while( !checkForCompletionWithLossTolerance(numSent, numRecvd) )
		{
			synchronized(waitLock)
			{
				try
				{
					waitLock.wait();
				} catch (InterruptedException e)
				{
					e.printStackTrace();
				}
			}
		}
		//stopThis();	
		waitTimer.cancel();
		
		threadFinished = true;
		synchronized( threadFinishLock )
		{
			threadFinishLock.notify();
		}
		//System.exit(0);
	}
	
	public void waitForThreadFinish()
	{
		while( !threadFinished )
		{
			synchronized( threadFinishLock )
			{
				try 
				{
					threadFinishLock.wait();
				} catch (InterruptedException e) 
				{
					e.printStackTrace();
				}
			}
		}
	}
	
	public class WaitTimerTask extends TimerTask
	{			
		@Override
		public void run()
		{
			// print the remaining update and query times
			// and finish the process, cancel the timer and exit JVM.
			//stopThis();
			
			double endTimeReplyRecvd = System.currentTimeMillis();
			double sysThrput= (numRecvd * 1000.0)/(endTimeReplyRecvd - expStartTime);
			
			System.out.println(this.getClass().getName()+" Result:TimeOutThroughput "+sysThrput);
			
			waitTimer.cancel();
			// just terminate the JVM
			//System.exit(0);
			threadFinished = true;
			synchronized( threadFinishLock )
			{
				threadFinishLock.notify();
			}
		}
	}
	
	protected boolean checkForCompletionWithLossTolerance(double numSent, double numRecvd)
	{
		boolean completion = false;
		
		double withinLoss = (lossTolerance * numSent)/100.0;
		if( (numSent - numRecvd) <= withinLoss )
		{
			completion = true;
		}
		return completion;
	}
	
	public abstract void incrementUpdateNumRecvd(String userGUID, long timeTaken);
	
	public abstract void incrementSearchNumRecvd(int resultSize, long timeTaken);
}