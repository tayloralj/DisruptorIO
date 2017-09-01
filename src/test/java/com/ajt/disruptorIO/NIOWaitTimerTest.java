/**
 * 
 */
package com.ajt.disruptorIO;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ajt.disruptorIO.NIOWaitStrategy;
import com.ajt.disruptorIO.NIOWaitStrategy.NIOClock;
import com.ajt.disruptorIO.NIOWaitStrategy.TimerCallback;
import com.ajt.disruptorIO.NIOWaitStrategy.TimerHandler;

/**
 * @author ajt
 *
 */
public class NIOWaitTimerTest {
	static {
		System.setProperty("org.apache.logging.log4j.simplelog.StatusLogger.level", "TRACE");
		System.setProperty("org.apache.logging.log4j.level", "DEBUG");

	}
	private final Logger logger = LoggerFactory.getLogger(NIOWaitTimerTest.class);

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	private long nanoTime = 0;
	final NIOClock clock = new NIOClock() {
		// test clock which allows time to be controlled.

		@Override
		public long getTimeNanos() {
			// TODO Auto-generated method stub
			return nanoTime;
		}
	};

	@Test
	public void test() {
		final NIOWaitStrategy waitStrat = new NIOWaitStrategy(clock);
		TimerCallbackImpl timer1 = new TimerCallbackImpl("");
		TimerHandler timerHandler1 = waitStrat.createTimer(timer1, timer1.toString());
		waitStrat.setNanoTime();
		waitStrat.checkTimer();
		assertThat(timer1.calledCount, is(0L));
		timerHandler1.fireIn(1000);

		waitStrat.setNanoTime();
		waitStrat.checkTimer();
		assertThat(timer1.calledCount, is(0L));
		nanoTime = 2000;
		waitStrat.setNanoTime();
		waitStrat.checkTimer();
		assertThat(timer1.calledCount, is(1L));
		waitStrat.setNanoTime();
		waitStrat.checkTimer();
		assertThat(timer1.calledCount, is(1L));
		{
			timerHandler1.fireIn(500);
			waitStrat.setNanoTime();
			waitStrat.checkTimer();
			assertThat(timer1.calledCount, is(1L));
			nanoTime = 2499;
			waitStrat.setNanoTime();
			waitStrat.checkTimer();
			assertThat(timer1.calledCount, is(1L));
			assertThat(timerHandler1.isRegistered(), is(true));
			nanoTime = 2500;
			waitStrat.setNanoTime();
			waitStrat.checkTimer();
			assertThat(timer1.calledCount, is(2L));
			assertThat(timerHandler1.isRegistered(), is(false));
		}
		{
			// cancel
			nanoTime = 2500;
			timerHandler1.fireIn(500);
			nanoTime = 2600;
			waitStrat.setNanoTime();
			waitStrat.checkTimer();
			assertThat(timer1.calledCount, is(2L));
			timerHandler1.cancelTimer();
			nanoTime = 3000;
			waitStrat.setNanoTime();
			waitStrat.checkTimer();
			assertThat(timer1.calledCount, is(2L));
		}

	}

	private class TimerCallbackImpl implements TimerCallback {
		long calledAt = -1;
		long calledCount = 0;
		String name = "";

		public TimerCallbackImpl(String nm) {
			this.name = nm;
		}

		@Override
		public void timerCallback(long dueAt, long currentNanoTime) {
			calledAt = currentNanoTime;
			calledCount++;
			logger.info("TimerCallback fired:{} dueAt:{} actualAt:{} name:{}", calledCount,dueAt,  calledAt,name);
		}

	}

	@Test
	public void test2() {
		nanoTime = 0;
		final NIOWaitStrategy waitStrat = new NIOWaitStrategy(clock);
		TimerCallbackImpl tcList[] = new TimerCallbackImpl[50];
		TimerHandler thList[] = new TimerHandler[50];
		for (int a = 0; a < tcList.length; a++) {
			tcList[a] = new TimerCallbackImpl("a:"+a);
			thList[a] = waitStrat.createTimer(tcList[a], "tc:" + a);
			thList[a].fireIn(50 + a);
		}
		nanoTime=1000;
		waitStrat.setNanoTime();
		waitStrat.checkTimer();

	}
}
