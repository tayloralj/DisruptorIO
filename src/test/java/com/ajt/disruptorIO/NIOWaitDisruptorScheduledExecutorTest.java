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
package com.ajt.disruptorIO;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.hamcrest.number.OrderingComparison;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

public class NIOWaitDisruptorScheduledExecutorTest {
	static {
		System.setProperty("org.apache.logging.log4j.simplelog.StatusLogger.level", "TRACE");
		System.setProperty("org.apache.logging.log4j.level", "DEBUG");

	}
	private final Logger logger = LoggerFactory.getLogger(NIOWaitDisruptorScheduledExecutorTest.class);
//	private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool();
	NIOWaitStrategy.NIOClock clock;
	NIOWaitStrategy nioWaitStrategy;
	Disruptor<TestEvent> disruptor;
	RingBuffer<TestEvent> rb;
	long toSend = 10_000_000L;
	LatencyTimer lt;

	TestEventHandler[] handlers;

	@Before
	public void setup() {
		nioWaitStrategy = new NIOWaitStrategy(NIOWaitStrategy.getDefaultClock());
		logger.trace("[{}] AsyncLoggerDisruptor creating new disruptor for this context.", "test");
		int ringBufferSize = 2048;
		handlers = new TestEventHandler[] { new TestEventHandler() };
		ThreadFactory threadFactory = new ThreadFactory() {

			@Override
			public Thread newThread(Runnable r) {
				final Thread th = new Thread(r, "WaStratThread");

				return th;
			}
		};
		disruptor = new Disruptor<>(TestEvent.EVENT_FACTORY, ringBufferSize, threadFactory, ProducerType.SINGLE,
				nioWaitStrategy);

		disruptor.handleEventsWith(handlers);

		logger.debug(
				"Starting AsyncLogger disruptor for this context with ringbufferSize={}, waitStrategy={}, "
						+ "exceptionHandler={}...",
				disruptor.getRingBuffer().getBufferSize(), nioWaitStrategy.getClass().getSimpleName());
		disruptor.start();
		lt = new LatencyTimer();
		lt.register(nioWaitStrategy);
		rb = disruptor.getRingBuffer();

	}

	@After
	public void tearDown() {
		try {
			disruptor.shutdown();
			nioWaitStrategy.close();
		} catch (Exception e) {
		}

	}

	public void testSent() {
		long endTime = System.currentTimeMillis() + 10000L;
		while (System.currentTimeMillis() < endTime) {
			if (handlers[0].counter.get() == toSend) {
				logger.info("completed :{}", toSend);
				lt.stop();
				try {
					Thread.sleep(5);
				} catch (Exception e) {
				}

				break;
			}
		}
	}

	@Test
	public void shouldWaitForValue() throws Exception {

		ScheduledExecutorService ses = nioWaitStrategy.getScheduledExecutor();
		final AtomicBoolean isRun = new AtomicBoolean(false);
		Runnable r = new Runnable() {
			@Override
			public void run() {
				logger.info("shouldWaitForValue Fired");
				isRun.set(true);
			}
		};
		ses.execute(r);
		long endSequence = 0;
		long endTime = System.currentTimeMillis() + toSend * 1000L / 100000L;
		for (int a = 0; a < toSend;) {
			if (rb.hasAvailableCapacity(1)) {
				endSequence = rb.next();
				// Thread.sleep(20);
				TestEvent te = rb.get(endSequence);
				te.seqNum = a++;
				rb.publish(endSequence);
			} else {
				assertThat(endTime, is(OrderingComparison.greaterThan(System.currentTimeMillis())));
			}
		}
		testSent();

		assertThat(handlers[0].counter.get(), is(toSend));
		assertThat(isRun.get(), is(true));

	}

	@Test
	public void shouldNotFire() throws Exception {

		ScheduledExecutorService ses = nioWaitStrategy.getScheduledExecutor();
		final AtomicBoolean isRun = new AtomicBoolean(false);
		Runnable r = new Runnable() {
			@Override
			public void run() {
				logger.info("shouldNotFire Fired");
				isRun.set(true);
			}
		};
		ses.schedule(r, 1, TimeUnit.DAYS);
		long endSequence = 0;

		long endTime = System.currentTimeMillis() + toSend * 1000L / 100000L;
		for (int a = 0; a < toSend;) {
			if (rb.hasAvailableCapacity(1)) {
				endSequence = rb.next();
				// Thread.sleep(20);
				final TestEvent te = rb.get(endSequence);
				te.seqNum = a++;
				rb.publish(endSequence);
			} else {
				assertThat(endTime, is(OrderingComparison.greaterThan(System.currentTimeMillis())));
			}

		}
		testSent();

		assertThat(handlers[0].counter.get(), is(toSend));
		assertThat(isRun.get(), is(false));

	}

	@Test
	public void shouldFire() throws Exception {

		ScheduledExecutorService ses = nioWaitStrategy.getScheduledExecutor();
		final AtomicBoolean isRun = new AtomicBoolean(false);
		Runnable r = new Runnable() {
			@Override
			public void run() {
				logger.info("shouldFire ");
				isRun.set(true);
			}
		};
		ses.schedule(r, 5, TimeUnit.MILLISECONDS);
		long endSequence = 0;
		long endTime = System.currentTimeMillis() + toSend * 1000L / 100000L;
		for (int a = 0; a < toSend;) {
			if (rb.hasAvailableCapacity(1)) {
				endSequence = rb.tryNext();
				final TestEvent te = rb.get(endSequence);
				te.seqNum = a++;
				rb.publish(endSequence);
			} else {
				assertThat(endTime, is(OrderingComparison.greaterThan(System.currentTimeMillis())));
			}

		}
		testSent();

		assertThat(handlers[0].counter.get(), is(toSend));
		assertThat(isRun.get(), is(true));

	}

	@Test
	public void shouldPartRun() throws Exception {

		final ScheduledExecutorService ses = nioWaitStrategy.getScheduledExecutor();
		final AtomicBoolean shouldRunisRun = new AtomicBoolean(false);
		final AtomicBoolean shouldNotRunisRun = new AtomicBoolean(false);
		final Runnable shouldRun = new Runnable() {
			@Override
			public void run() {
				logger.info("shouldFire ");
				shouldRunisRun.set(true);
			}
		};
		final Runnable shouldNotRun = new Runnable() {
			@Override
			public void run() {
				logger.info("shouldNotFire ");
				shouldNotRunisRun.set(true);
			}
		};
		ses.schedule(shouldRun, 5, TimeUnit.MILLISECONDS);
		ses.schedule(shouldNotRun, 5, TimeUnit.DAYS);
		long endSequence = 0;
		long endTime = System.currentTimeMillis() + toSend * 1000L / 100000L;
		for (int a = 0; a < toSend;) {
			if (rb.hasAvailableCapacity(1)) {
				endSequence = rb.tryNext();
				final TestEvent te = rb.get(endSequence);
				te.seqNum = a++;
				rb.publish(endSequence);
			} else {
				assertThat(endTime, is(OrderingComparison.greaterThan(System.currentTimeMillis())));
			}

		}
		testSent();

		assertThat(handlers[0].counter.get(), is(toSend));
		assertThat(shouldRunisRun.get(), is(true));
		assertThat(shouldNotRunisRun.get(), is(false));

	}

	static int count = 0;

	@Test
	public void invokeAll() throws Exception {
		int toRunCount = 1000;
		final ScheduledExecutorService ses = nioWaitStrategy.getScheduledExecutor();
		class CC implements Callable<AtomicBoolean> {
			final AtomicBoolean hasRun = new AtomicBoolean(false);
			final int counter = count++;

			@Override
			public AtomicBoolean call() throws Exception {
				logger.info("running :" + counter);
				hasRun.set(true);
				return hasRun;
			}

		}
		ArrayList<CC> runList = new ArrayList<>();
		List<Future<AtomicBoolean>> list = null;
		for (int a = 0; a < toRunCount; a++) {
			runList.add(new CC());
		}
		long endSequence = 0;
		long endTime = System.currentTimeMillis() + toSend * 1000L / 100000L;
		for (int a = 0; a < toSend;) {
			if (rb.hasAvailableCapacity(1)) {
				endSequence = rb.tryNext();
				final TestEvent te = rb.get(endSequence);
				te.seqNum = a++;
				rb.publish(endSequence);
			} else {
				assertThat(endTime, is(OrderingComparison.greaterThan(System.currentTimeMillis())));
			}
			if (a == 1) {
				list = ses.invokeAll(runList);
			}

		}
		testSent();
		assertThat(handlers[0].counter.get(), is(toSend));
		for (int a = 0; a < toRunCount; a++) {
			assertThat(" counter:" + a, runList.get(a).hasRun.get(), is(true));
			assertThat(list.get(a).isDone(), is(true));
			assertThat("" + list.get(a), list.get(a).isCancelled(), is(false));
			assertThat(" counter:" + a, list.get(a).get().get(), is(true));
		}

	}

	private class TestEventHandler implements EventHandler<TestEvent> {
		private AtomicLong counter = new AtomicLong();

		@Override
		public void onEvent(TestEvent event, long sequence, boolean endOfBatch) throws Exception {
			// logger.debug("Event :{} eob:{} seq:{} thread:{}", event.seqNum, endOfBatch,
			// sequence,
			// Thread.currentThread());
			counter.incrementAndGet();

		}

	}

	private static final class TestEvent {
		private long seqNum = -1;
		public static final EventFactory<TestEvent> EVENT_FACTORY = new EventFactory<TestEvent>() {
			@Override
			public TestEvent newInstance() {
				return new TestEvent();
			}
		};

	}

}