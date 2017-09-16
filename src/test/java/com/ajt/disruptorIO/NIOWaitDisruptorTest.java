package com.ajt.disruptorIO;

import static org.junit.Assert.assertThat;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.support.DummySequenceBarrier;
import com.lmax.disruptor.support.SequenceUpdater;

public class NIOWaitDisruptorTest {
	static {
		System.setProperty("org.apache.logging.log4j.simplelog.StatusLogger.level", "TRACE");
		System.setProperty("org.apache.logging.log4j.level", "DEBUG");

	}
	private final Logger logger = LoggerFactory.getLogger(NIOWaitDisruptorTest.class);
	private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool();
	NIOWaitStrategy.NIOClock clock;
	ExceptionHandler<TestEvent> errorHandler;

	@Before
	public void setup() {
		clock = NIOWaitStrategy.getDefaultClock();
		errorHandler = new ExceptionHandler<NIOWaitDisruptorTest.TestEvent>() {

			@Override
			public void handleOnStartException(Throwable ex) {
				// TODO Auto-generated method stub

			}

			@Override
			public void handleOnShutdownException(Throwable ex) {
				// TODO Auto-generated method stub

			}

			@Override
			public void handleEventException(Throwable ex, long sequence, TestEvent event) {
				// TODO Auto-generated method stub

			}
		};
	}

	@Test
	public void shouldWaitForValue() throws Exception {

		NIOWaitStrategy nioWaitStrategy = new NIOWaitStrategy(clock);
		logger.trace("[{}] AsyncLoggerDisruptor creating new disruptor for this context.", "test");
		int ringBufferSize = 2048;

		ThreadFactory threadFactory = new ThreadFactory() {

			@Override
			public Thread newThread(Runnable r) {
				final Thread th = new Thread(r, "WaStratThread");

				return th;
			}
		};
		Disruptor<TestEvent> disruptor = new Disruptor<>(TestEvent.EVENT_FACTORY, ringBufferSize, threadFactory,
				ProducerType.SINGLE, nioWaitStrategy);

		disruptor.setDefaultExceptionHandler(errorHandler);

		final TestEventHandler[] handlers = { new TestEventHandler() };
		disruptor.handleEventsWith(handlers);

		logger.debug(
				"Starting AsyncLogger disruptor for this context with ringbufferSize={}, waitStrategy={}, "
						+ "exceptionHandler={}...",
				disruptor.getRingBuffer().getBufferSize(), nioWaitStrategy.getClass().getSimpleName(), errorHandler);
		disruptor.start();
		LatencyTimer lt = new LatencyTimer();
		lt.register(nioWaitStrategy);

		RingBuffer<TestEvent> rb = disruptor.getRingBuffer();
		long lastSequence = 0;
		long endSequence = 0;
		long toSend = 200000;
		for (int a = 0; a < toSend; a++) {
			if (lastSequence == endSequence) {
				endSequence = rb.next(1024);
			}
			// Thread.sleep(20);
			TestEvent te = rb.get(lastSequence);
			te.seqNum = a;
			rb.publish(lastSequence);
			lastSequence++;
			if ((lastSequence & 127) == 0) {
				Thread.sleep(1);
			}
		}
		long endTime = System.currentTimeMillis() + 10000L;
		while (System.currentTimeMillis() < endTime) {
			if (handlers[0].counter.get() == toSend) {
				logger.info("completed :{}", toSend);
				lt.stop();
				Thread.sleep(5);

				break;
			}
		}
		assertThat(handlers[0].counter.get(), Matchers.is(toSend));
	disruptor.shutdown();

	}

	@Test
	public void shouldNot() throws Exception {
		NIOWaitStrategy nioWaitStrategy = new NIOWaitStrategy(NIOWaitStrategy.getDefaultClock());
		SequenceUpdater sequenceUpdater = new SequenceUpdater(120, nioWaitStrategy);
		EXECUTOR.execute(sequenceUpdater);
		sequenceUpdater.waitForStartup();
		Sequence cursor = new Sequence(0);
		long sequence = nioWaitStrategy.waitFor(0, cursor, sequenceUpdater.sequence, new DummySequenceBarrier());

		assertThat(sequence, Matchers.is(0L));
		nioWaitStrategy.close();
	}

	private class TestEventHandler implements EventHandler<TestEvent> {
		private final Logger logger = LoggerFactory.getLogger(TestEventHandler.class);
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

	/**
	 * Returns the thread ID of the background appender thread. This allows us to
	 * detect Logger.log() calls initiated from the appender thread, which may cause
	 * deadlock when the RingBuffer is full. (LOG4J2-471)
	 *
	 * @param executor
	 *            runs the appender thread
	 * @return the thread ID of the background appender thread
	 */
	public static long getExecutorThreadId(final ExecutorService executor) {
		final Future<Long> result = executor.submit(new Callable<Long>() {
			@Override
			public Long call() {
				return Thread.currentThread().getId();
			}
		});
		try {
			return result.get();
		} catch (final Exception ex) {
			final String msg = "Could not obtain executor thread Id. "
					+ "Giving up to avoid the risk of application deadlock.";
			throw new IllegalStateException(msg, ex);
		}
	}
}