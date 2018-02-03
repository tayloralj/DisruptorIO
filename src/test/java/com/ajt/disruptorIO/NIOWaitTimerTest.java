/*******************************************************************************
 * Copyright (c) 2017 IBM Corporation and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *     IBM Corporation - initial API and implementation
 *******************************************************************************/
/**
 * 
 */
package com.ajt.disruptorIO;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;

import org.hamcrest.Matchers;
import org.hamcrest.core.Is;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	public void testTImers() {
		try (final NIOWaitStrategy waitStrat = new NIOWaitStrategy(clock);) {
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
		} finally {
		}
	}

	private class TimerCallbackImpl implements TimerCallback {
		long calledAt = -1;
		long calledCount = 0;
		final String name;

		public TimerCallbackImpl(final String nm) {
			this.name = nm;
		}

		@Override
		public void timerCallback(final long dueAt, final long currentNanoTime) {
			calledAt = currentNanoTime;
			calledCount++;
			logger.info("TimerCallback fired:{} dueAt:{} actualAt:{} name:{}", calledCount, dueAt, calledAt, name);
			assertThat(currentNanoTime, Matchers.greaterThanOrEqualTo(dueAt));
		}

	}

	@Test
	public void testManyTimers() {
		nanoTime = 0;
		try (final NIOWaitStrategy waitStrat = new NIOWaitStrategy(clock);) {
			final TimerCallbackImpl tcList[] = new TimerCallbackImpl[50];
			final TimerHandler thList[] = new TimerHandler[tcList.length];
			for (int a = 0; a < tcList.length; a++) {
				tcList[a] = new TimerCallbackImpl("a:" + a);
				thList[a] = waitStrat.createTimer(tcList[a], "tc:" + a);
				thList[a].fireIn(50 + a);
			}
			nanoTime = 1000 + thList.length;
			waitStrat.setNanoTime();
			// fire the timers.
			for (int a = 0; a < thList.length; a++) {
				waitStrat.checkTimer();
			}
			for (int a = 0; a < tcList.length; a++) {
				assertThat(tcList[a].calledAt, Is.is(nanoTime));
				assertThat(tcList[a].calledCount, Is.is(1L));
			}
			// fire again
			for (int a = 0; a < tcList.length; a++) {

				thList[a].fireIn(50 + a);
			}
			nanoTime += 1000 + thList.length;
			waitStrat.setNanoTime();
			for (int a = 0; a < thList.length; a++) {
				waitStrat.checkTimer();
			}
			for (int a = 0; a < tcList.length; a++) {
				assertThat(tcList[a].calledAt, Is.is(nanoTime));
				assertThat(tcList[a].calledCount, Is.is(2L));
			}

		} finally {
		}
	}

	@Test
	public void testBadThreadTimers() throws Exception {
		nanoTime = 0;
		try (final NIOWaitStrategy waitStrat = new NIOWaitStrategy(clock, "TimersThread", false, false, true)) {
			final TimerCallbackImpl tcList[] = new TimerCallbackImpl[50];
			final TimerHandler thList[] = new TimerHandler[tcList.length];
			for (int a = 0; a < tcList.length; a++) {
				tcList[a] = new TimerCallbackImpl("a:" + a);
				thList[a] = waitStrat.createTimer(tcList[a], "tc:" + a);
				thList[a].fireIn(50);
			}
			final Thread t = new Thread(() -> {
				// expect timer to throw an exception as the cancel was not on the reactor
				// thread
				thList[0].cancelTimer();
				Assert.fail("Didnt get exception when cancelling on wrong thread");
			});
			final AtomicBoolean hasThrown = new AtomicBoolean(false);
			t.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {

				@Override
				public void uncaughtException(Thread t, Throwable e) {
					hasThrown.set(true);
				}
			});

			t.start();
			t.join(100);

			assertTrue("Error did not throw exception", hasThrown.get());

		}
	}

	@Test
	public void testCorrectTimers() {
		nanoTime = 0;
		try (final NIOWaitStrategy waitStrat = new NIOWaitStrategy(clock);) {
			final TimerCallbackImpl tcList[] = new TimerCallbackImpl[50];
			final TimerHandler thList[] = new TimerHandler[tcList.length];
			for (int a = 0; a < tcList.length; a++) {
				tcList[a] = new TimerCallbackImpl("a:" + a);
				thList[a] = waitStrat.createTimer(tcList[a], "tc:" + a);
				thList[a].fireIn(50);
			}
			nanoTime = 49;
			waitStrat.setNanoTime();
			// fire the timers.
			for (int a = 0; a < thList.length; a++) {
				waitStrat.checkTimer();
			}
			for (int a = 0; a < tcList.length; a++) {
				assertThat(tcList[a].calledAt, Is.is(-1L));
				assertThat(tcList[a].calledCount, Is.is(0L));
			}

			nanoTime = 50;
			waitStrat.setNanoTime();
			for (int a = 0; a < thList.length; a++) {
				waitStrat.checkTimer();
			}
			for (int a = 0; a < tcList.length; a++) {
				assertThat(tcList[a].calledAt, Is.is(nanoTime));
				assertThat(tcList[a].calledCount, Is.is(1L));
			}
			// increment time,. but no more timers.
			nanoTime += 50;
			waitStrat.setNanoTime();
			for (int a = 0; a < thList.length; a++) {
				waitStrat.checkTimer();
			}
			for (int a = 0; a < tcList.length; a++) {
				assertThat(tcList[a].calledAt, Is.is(50L));
				assertThat(tcList[a].calledCount, Is.is(1L));
			}
		} finally {
		}
	}

	@Test
	public void testCancelTimers() {
		nanoTime = 0;
		try (final NIOWaitStrategy waitStrat = new NIOWaitStrategy(clock);) {
			final TimerCallbackImpl tcList[] = new TimerCallbackImpl[50];
			final TimerHandler thList[] = new TimerHandler[tcList.length];
			for (int a = 0; a < tcList.length; a++) {
				tcList[a] = new TimerCallbackImpl("a:" + a);
				thList[a] = waitStrat.createTimer(tcList[a], "tc:" + a);
				thList[a].fireAt(50);
				assertThat(thList[a].isRegistered(), Is.is(true));
			}
			nanoTime = 49;
			waitStrat.setNanoTime();
			{
				// set timers. cancel. move clock on. check again.
				{
					for (int a = 0; a < thList.length; a++) {
						waitStrat.checkTimer();
					}
					for (int a = 0; a < tcList.length; a++) {
						assertThat(tcList[a].calledAt, Is.is(-1L));
						assertThat(tcList[a].calledCount, Is.is(0L));
					}
					for (int a = 0; a < thList.length; a++) {
						thList[a].cancelTimer();
						assertThat("" + thList[a], thList[a].isRegistered(), Is.is(false));
					}
				}
				nanoTime = 51;
				waitStrat.setNanoTime();
				{
					for (int a = 0; a < thList.length; a++) {
						waitStrat.checkTimer();
					}
					for (int a = 0; a < tcList.length; a++) {
						assertThat(tcList[a].calledAt, Is.is(-1L));
						assertThat(tcList[a].calledCount, Is.is(0L));
					}
				}
			}
			{
				{// set timers. cancel. check
					for (int a = 0; a < tcList.length; a++) {
						thList[a].fireAt(nanoTime + 1);
						assertThat(thList[a].isRegistered(), Is.is(true));
					}
					for (int a = 0; a < thList.length; a++) {
						waitStrat.checkTimer();
					}
					for (int a = 0; a < tcList.length; a++) {
						assertThat(tcList[a].calledAt, Is.is(-1L));
						assertThat(tcList[a].calledCount, Is.is(0L));
					}
					for (int a = 0; a < thList.length; a++) {
						thList[a].cancelTimer();
					}
				}
				nanoTime = 51;
				waitStrat.setNanoTime();
				{
					for (int a = 0; a < thList.length; a++) {
						waitStrat.checkTimer();
					}
					for (int a = 0; a < tcList.length; a++) {
						assertThat(tcList[a].calledAt, Is.is(-1L));
						assertThat(tcList[a].calledCount, Is.is(0L));
					}
				}
			}
		} finally {

		}
	}

	@Test
	public void benchmmarkCancelTimers() {
		nanoTime = 0;
		try (final NIOWaitStrategy waitStrat = new NIOWaitStrategy(clock);) {
			final TimerCallbackImpl tcList[] = new TimerCallbackImpl[500];
			final TimerHandler thList[] = new TimerHandler[tcList.length];
			for (int a = 0; a < tcList.length; a++) {
				tcList[a] = new TimerCallbackImpl("a:" + a);
				thList[a] = waitStrat.createTimer(tcList[a], "tc:" + a);
				thList[a].fireAt(50);
				assertThat(thList[a].isRegistered(), Is.is(true));
			}
			nanoTime = 30;
			waitStrat.setNanoTime();
			for (int a = 0; a < thList.length / NIOWaitStrategy.MAX_TIMER_BATCH_SIZE; a++) {
				waitStrat.checkTimer();
			}
			for (int a = 0; a < tcList.length; a++) {
				assertThat(thList[a].isRegistered(), Is.is(true));
			}
			final long startAt = System.currentTimeMillis();
			final long finishAt = startAt + 5000;
			long counter = 0;
			while (System.currentTimeMillis() < finishAt) {
				for (int a = 0; a < tcList.length; a++) {
					assertThat(thList[a].cancelTimer(), Is.is(true));
					assertThat(thList[a].isRegistered(), Is.is(false));
					counter++;
				}
				nanoTime += 10;
				waitStrat.setNanoTime();
				for (int a = 0; a < thList.length / NIOWaitStrategy.MAX_TIMER_BATCH_SIZE; a++) {
					waitStrat.checkTimer();
				}
				for (int a = 0; a < tcList.length; a++) {
					assertThat(thList[a].isRegistered(), Is.is(false));
					assertThat(thList[a].fireIn(10), Is.is(true));
					assertThat(thList[a].isRegistered(), Is.is(true));
				}
				for (int a = 0; a < thList.length / NIOWaitStrategy.MAX_TIMER_BATCH_SIZE; a++) {
					waitStrat.checkTimer();
				}
			}
			logger.info("Timers canceled rate(s):{}", counter, counter * 1000 / (System.currentTimeMillis() - startAt));

		} finally {

		}
	}

	private class QuietTimerCallbackImpl implements TimerCallback {
		long calledAt = -1;
		long calledCount = 0;
		String name = "";

		public QuietTimerCallbackImpl(final String nm) {
			this.name = nm;
		}

		@Override
		public void timerCallback(final long dueAt, final long currentNanoTime) {
			calledAt = currentNanoTime;
			calledCount++;
			assertThat(currentNanoTime, Matchers.greaterThanOrEqualTo(dueAt));
			// logger.info("TimerCallback fired:{} dueAt:{} actualAt:{} name:{}",
			// calledCount, dueAt, calledAt, name);
		}

	}

	@Test
	public void benchmmarkFireTimers() {
		nanoTime = 0;
		try (final NIOWaitStrategy waitStrat = new NIOWaitStrategy(clock);) {
			final QuietTimerCallbackImpl tcList[] = new QuietTimerCallbackImpl[50];
			final TimerHandler thList[] = new TimerHandler[tcList.length];
			for (int a = 0; a < tcList.length; a++) {
				tcList[a] = new QuietTimerCallbackImpl("a:" + a);
				thList[a] = waitStrat.createTimer(tcList[a], "tc:" + a);
				assertThat(thList[a].isRegistered(), Is.is(false));
			}
			nanoTime = 51;
			waitStrat.setNanoTime();
			for (int a = 0; a < thList.length / NIOWaitStrategy.MAX_TIMER_BATCH_SIZE; a++) {
				waitStrat.checkTimer();
			}
			for (int a = 0; a < tcList.length; a++) {
				assertThat(thList[a].isRegistered(), Is.is(false));
			}
			final long startAt = System.currentTimeMillis();
			final long finishAt = startAt + 5000;
			long counter = 0;
			while (System.currentTimeMillis() < finishAt) {
				for (int a = 0; a < thList.length; a++) {
					assertThat(thList[a].fireIn(10), Is.is(true));
					assertThat(thList[a].isRegistered(), Is.is(true));
					counter++;
				}
				nanoTime += 100;
				waitStrat.setNanoTime();
				for (int a = 0; a < thList.length / NIOWaitStrategy.MAX_TIMER_BATCH_SIZE; a++) {
					waitStrat.checkTimer();
				}
				for (int a = 0; a < thList.length; a++) {
					assertThat(thList[a].isRegistered(), Is.is(false));
				}
				for (int a = 0; a < thList.length / NIOWaitStrategy.MAX_TIMER_BATCH_SIZE; a++) {
					waitStrat.checkTimer();
				}
				for (int a = 0; a < thList.length; a++) {
					assertThat(thList[a].isRegistered(), Is.is(false));
				}
			}
			logger.info("Timers FIRED rate(s):{}", counter, counter * 1000 / (System.currentTimeMillis() - startAt));

		} finally {

		}
	}
}
