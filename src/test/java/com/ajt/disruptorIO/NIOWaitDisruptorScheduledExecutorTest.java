/*******************************************************************************
 * Copyright (c) 2017 IBM Corporation and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and Is.is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *     IBM Corporation - initial API and implementation
 *******************************************************************************/
package com.ajt.disruptorIO;

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

import org.hamcrest.core.Is;
import org.hamcrest.number.OrderingComparison;
import org.junit.After;
import org.junit.Assert;
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
	private final Logger logger = LoggerFactory.getLogger(NIOWaitDisruptorScheduledExecutorTest.class);
	static {
		// turn up the logging
		System.setProperty("org.apache.logging.log4j.simplelog.StatusLogger.level", "TRACE");
		System.setProperty("org.apache.logging.log4j.level", "DEBUG");

	}

	private NIOWaitStrategy.NIOClock clock;
	private NIOWaitStrategy nioWaitStrategy;
	private Disruptor<TestEvent> disruptor;
	private RingBuffer<TestEvent> rb;
	private long toSend = 10_000_000L;
	private LatencyTimer lt;

	private TestEventHandler[] handlers;

	@Before
	public void setup() {
		clock = NIOWaitStrategy.getDefaultClock();
		nioWaitStrategy = new NIOWaitStrategy(clock, true, true, true);
		logger.trace("[{}] AsyncLoggerDisruptor creating new disruptor for this context.", "test");
		int ringBufferSize = 1024;
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

	public void testEventsDelivered() {
		final long endTime = System.currentTimeMillis() + 10000L;
		while (System.currentTimeMillis() < endTime) {
			if (handlers[0].counter.get() == toSend) {
				logger.info("completed :{}", toSend);
				lt.stop();
				try {
					Thread.sleep(5);
				} catch (Exception e) {
				}
				return;
			}
		}
		logger.error("ERROR didn't complete expected:{} actual:{}", toSend, handlers[0].counter.get());
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
				assertThat(endTime, Is.is(OrderingComparison.greaterThan(System.currentTimeMillis())));
			}
		}
		testEventsDelivered();

		assertThat(handlers[0].counter.get(), Is.is(toSend));
		assertThat(isRun.get(), Is.is(true));

	}

	@Test
	public void shouldNotFire() throws Exception {

		final ScheduledExecutorService ses = nioWaitStrategy.getScheduledExecutor();
		final AtomicBoolean isRun = new AtomicBoolean(false);
		final Runnable r = new Runnable() {
			@Override
			public void run() {
				logger.info("shouldNotFire Fired");
				isRun.set(true);
				Assert.fail("SHould not fire");
			}
		};
		ses.schedule(r, 1, TimeUnit.DAYS);
		long endSequence = 0;

		final long endTime = System.currentTimeMillis() + toSend;
		for (int a = 0; a < toSend;) {
			if (rb.hasAvailableCapacity(1)) {
				endSequence = rb.next();
				// Thread.sleep(20);
				final TestEvent te = rb.get(endSequence);
				te.seqNum = a++;
				rb.publish(endSequence);
			} else {
				if (System.currentTimeMillis() > endTime) {
					Assert.fail("Error did not complete in time");
				}
			}

		}
		testEventsDelivered();

		assertThat(handlers[0].counter.get(), Is.is(toSend));
		assertThat("Error is run is set to true, should not of run", isRun.get(), Is.is(false));

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
				assertThat(endTime, Is.is(OrderingComparison.greaterThan(System.currentTimeMillis())));
			}

		}
		testEventsDelivered();

		assertThat(handlers[0].counter.get(), Is.is(toSend));
		assertThat(isRun.get(), Is.is(true));

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
				assertThat(endTime, Is.is(OrderingComparison.greaterThan(System.currentTimeMillis())));
			}

		}
		testEventsDelivered();

		assertThat(handlers[0].counter.get(), Is.is(toSend));
		assertThat(shouldRunisRun.get(), Is.is(true));
		assertThat(shouldNotRunisRun.get(), Is.is(false));

	}

	@Test
	public void shouldRunAll() throws Exception {
		int toRunCount = 1000;
		final AtomicLong count = new AtomicLong();
		final ScheduledExecutorService ses = nioWaitStrategy.getScheduledExecutor();
		final class CC implements Callable<AtomicBoolean> {

			final AtomicBoolean hasRun = new AtomicBoolean(false);
			final long counter = count.getAndIncrement();

			@Override
			public AtomicBoolean call() throws Exception {
				logger.info("running :" + counter);
				hasRun.set(true);
				return hasRun;
			}

		}
		final ArrayList<CC> runList = new ArrayList<>();
		List<Future<AtomicBoolean>> list = null;
		for (int a = 0; a < toRunCount; a++) {
			runList.add(new CC());
		}
		long endSequence = 0;
		final long endTime = System.currentTimeMillis() + toSend * 1000L / 100000L;
		for (int a = 0; a < toSend;) {
			if (rb.hasAvailableCapacity(1)) {
				endSequence = rb.tryNext();
				final TestEvent te = rb.get(endSequence);
				te.seqNum = a++;
				rb.publish(endSequence);
			} else {
				assertThat(endTime, Is.is(OrderingComparison.greaterThan(System.currentTimeMillis())));
			}
			if (a == 1) {
				// run all the tests in the list after the first event which Is.is sent.
				list = ses.invokeAll(runList);
			}

		}
		testEventsDelivered();
		assertThat(handlers[0].counter.get(), Is.is(toSend));
		for (int a = 0; a < toRunCount; a++) {
			assertThat(" counter:" + a, runList.get(a).hasRun.get(), Is.is(true));
			assertThat(list.get(a).isDone(), Is.is(true));
			if (list.get(a).isCancelled() == true) {
				Assert.fail("isCancelled:" + list.get(a));
			}
			assertThat(" counter:" + a, list.get(a).get().get(), Is.is(true));
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
		@SuppressWarnings("unused")
		private long seqNum = -1;
		public static final EventFactory<TestEvent> EVENT_FACTORY = new EventFactory<TestEvent>() {
			@Override
			public TestEvent newInstance() {
				return new TestEvent();
			}
		};

	}

}