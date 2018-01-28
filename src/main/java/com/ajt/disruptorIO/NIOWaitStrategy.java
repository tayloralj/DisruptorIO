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

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Comparator;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import org.agrona.LangUtil;
import org.agrona.nio.NioSelectedKeySet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.collections.Histogram;

/**
 * wait strategy which is busy spin loop. but when its nothing is available it
 * will check if any NIO events are ready to go or to fire a timer timer
 * callbacks and nio events are then all on the same thread as the disruptor
 * events.
 * 
 * @author alist
 *
 */
public class NIOWaitStrategy implements WaitStrategy, AutoCloseable {

	static final int MAX_TIMER_BATCH_SIZE = 5;
	static final Field SELECTED_KEYS_FIELD;
	static final Field PUBLIC_SELECTED_KEYS_FIELD;
	final long SLOW_TIMER_WARN = 500_00L;
	final boolean debugTimes;
	final boolean assertThread;
	private Thread threadCheck = null;

	private final Logger logger = LoggerFactory.getLogger(NIOWaitStrategy.class);
	final NioSelectedKeySet selectedKeySet;
	final AtomicBoolean selectCurrentBlocked = new AtomicBoolean(false);
	final static long MIN_THREAD_BLOCK_NS = TimeUnit.MILLISECONDS.toNanos(2);

	/** temp until better struct */
	final PriorityQueue<MyTimerHandler> timerHeap;
	final TimerLatencyReport timerLatencyReport;
	final NIOWaitStrategyExecutor strategyExecutor;
	final NIOClock clock;
	final boolean timerStats;
	final Selector selector;

	private final HashMap<SelectorCallback, Histogram> timerCallback;
	private final Histogram waitHisto;
	private long startNIOTime = 0;
	private long startWaitTime = 0;
	private long endWaitTime = 0;
	private long endNIOTime = 0;
	private long selectCount = 0;
	boolean isStarted = false;
	boolean isClosed = false;
	long currentTimeNanos = 0;

	// use funky code from aeron to install object free code
	static {
		Field selectKeysField = null;
		Field publicSelectKeysField = null;

		try {
			final Class<?> clazz = Class.forName("sun.nio.ch.SelectorImpl", false, ClassLoader.getSystemClassLoader());

			if (clazz.isAssignableFrom(Selector.open().getClass())) {
				selectKeysField = clazz.getDeclaredField("selectedKeys");
				selectKeysField.setAccessible(true);

				publicSelectKeysField = clazz.getDeclaredField("publicSelectedKeys");
				publicSelectKeysField.setAccessible(true);
			}
		} catch (final Exception ignore) {
		}

		SELECTED_KEYS_FIELD = selectKeysField;
		PUBLIC_SELECTED_KEYS_FIELD = publicSelectKeysField;
	}

	public NIOWaitStrategy(final NIOClock clock) {
		this(clock, false, false, true);
	}

	public NIOWaitStrategy(//
			final NIOClock clock, //
			final boolean timerStats, //
			final boolean debugTimes, //
			final boolean assertThread) {
		this.clock = clock;
		this.timerStats = timerStats;
		this.debugTimes = debugTimes;
		this.assertThread = assertThread;
		//
		logger.info("Created NIOWait with clock:{}", clock);
		timerHeap = new PriorityQueue<>(255);
		if (timerStats) {
			timerLatencyReport = new TimerLatencyReport(this);
			timerLatencyReport.timerHandler = createTimer(timerLatencyReport.callback, "TimerLatencyReport");
			timerLatencyReport.timerHandler.fireIn(timerLatencyReport.timerReportInterval);
		} else {
			timerLatencyReport = null;
		}

		strategyExecutor = new NIOWaitStrategyExecutor(this);
		setNanoTime();

		// use own selector keyset to remove obj creation.
		try {
			selector = Selector.open();
			selectedKeySet = new NioSelectedKeySet();
			SELECTED_KEYS_FIELD.set(selector, selectedKeySet);
			PUBLIC_SELECTED_KEYS_FIELD.set(selector, selectedKeySet);
		} catch (final Exception ex) {
			throw new RuntimeException(ex);
		}
		if (debugTimes) {
			timerCallback = new HashMap<>();
			waitHisto = getHisto();
			logger.info("DEBUGTIME ENABLED");
		} else {
			logger.info("DEBUGTIME CANCELLED");
			waitHisto = null;
			timerCallback = null;
		}
	}

	public ScheduledExecutorService getScheduledExecutor() {
		return strategyExecutor.getExecutorService();
	}

	/**
	 * first check for data from ringbuffer then from nio selectable channel then
	 * from timer.
	 */
	@Override
	public long waitFor(final long sequence, //
			final Sequence cursor, //
			final Sequence dependentSequence, //
			final SequenceBarrier barrier) throws AlertException, InterruptedException, TimeoutException {
		if (debugTimes) {
			startWaitTime = System.nanoTime();
		}
		long availableSequence;
		final long lastClock = currentTimeNanos;
		setNanoTime();
		if (currentTimeNanos - lastClock > 1_000_000L) {
			checkNIO();
			if (currentTimeNanos - lastClock > 10_000_000L) {
				checkTimer();
			}
		}
		while ((availableSequence = dependentSequence.get()) < sequence) {
			barrier.checkAlert();
			setNanoTime();
			if (checkNIO()) {
				continue;
			}

			checkTimer();
		}
		if (debugTimes) {
			endWaitTime = System.nanoTime();
			waitHisto.addObservation((endWaitTime - startWaitTime));
		}

		return availableSequence;

	}

	/**
	 * throw an exception if the thread is messed with. handy for debugging.
	 */
	private final void checkThreadIfStarted() {
		if (isStarted == true) {
			checkThread();
		}
	}

	private final void checkThread() {
		if (assertThread) {

			final Thread runningThread = Thread.currentThread();
			if (runningThread.equals(threadCheck)) {

			} else {
				if (threadCheck == null) {
					threadCheck = Thread.currentThread();
					isStarted = true;
				} else {
					if (isClosed) {

					} else {
						throw new RuntimeException("Error thread check. thread problem changed. started on:"
								+ threadCheck + " current:" + runningThread);
					}
				}
			}
		}

	}

	/** set the time currently */
	void setNanoTime() {
		currentTimeNanos = clock.getTimeNanos();
	}

	@Override
	public void signalAllWhenBlocking() {
		// wake up
		if (selectCurrentBlocked.get() == true) {
			selector.wakeup();
		}
	}

	private Histogram getHisto() {
		final Histogram histo = new Histogram(
				new long[] { 5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 140, 160, 180, 200, 300, 400, 600,
						800, 1000, 2000, 4000, 8000, 16000, 32000, 64000, 128000, 256000, 512000, Long.MAX_VALUE });
		return histo;
	}

	public static String toStringHisto(Histogram h) {
		final StringBuilder sb = new StringBuilder();
		sb.append("Mean: " + h.getMean());
		sb.append(" Count:" + h.getCount());
		final int size = h.getSize();
		for (int a = 0; a < size; a++) {
			if (h.getCountAt(a) > 0) {
				sb.append(" ").append(h.getUpperBoundAt(a)).append(":").append(h.getCountAt(a));
			}
		}
		return sb.toString();
	}

	NIOClock getClock() {
		return clock;
	}

	boolean checkNIO() {
		final long timeUntilTimer = timeUntilNextTimerNS();
		if (timeUntilTimer > MIN_THREAD_BLOCK_NS) {
			return selectBlock(timeUntilTimer);
		} else {
			return selectNow();
		}
	}

	/**
	 * select and block for some time.
	 * 
	 * @param maxBlockTimeNS
	 * @return
	 */
	boolean selectBlock(final long maxBlockTimeNS) {
		try {
			selectCurrentBlocked.set(true);
			final long maxBlockMillis = TimeUnit.NANOSECONDS.toMillis(maxBlockTimeNS) - 1;
			logger.trace("CheckNIO");
			assert (maxBlockMillis > 0);
			final int keyCount = selector.select(maxBlockMillis);
			if (keyCount == 0) {
				selectCount++;
				return false;
			}
			return checkSelectOutput();
		} catch (final IOException ex) {
			LangUtil.rethrowUnchecked(ex);
		} finally {
			selectCurrentBlocked.set(false);
		}
		return false;
	}

	/**
	 * select and fire immediately
	 * 
	 * @return
	 */
	boolean selectNow() {
		try {
			logger.trace("CheckNIO");
			final int keyCount = selector.selectNow();
			if (keyCount == 0) {
				selectCount++;
				return false;
			}
			return checkSelectOutput();
		} catch (final IOException ex) {
			LangUtil.rethrowUnchecked(ex);

		}
		return false;
	}

	boolean checkSelectOutput() {
		for (int aaa = 0; aaa < selectedKeySet.size(); aaa++) {
			final SelectionKey key = selectedKeySet.keys()[aaa];
			if (key.isValid()) {
				final int readyOps = key.readyOps();
				final SelectorCallback callback = (SelectorCallback) key.attachment();
				final SelectableChannel channel = key.channel();
				switch (readyOps) {
				case SelectionKey.OP_ACCEPT:
					if (debugTimes) {
						startNIOTime = System.nanoTime();
					}
					callback.opAccept(channel, currentTimeNanos);
					if (debugTimes) {
						endNIOTime = System.nanoTime();
						final Histogram hg = timerCallback.get(callback);
						if (hg == null) {
							final Histogram h = getHisto();
							h.addObservation(TimeUnit.NANOSECONDS.toMicros(endNIOTime - startNIOTime));
							timerCallback.put(callback, h);
						} else {
							hg.addObservation(TimeUnit.NANOSECONDS.toMicros(endNIOTime - startNIOTime));
						}
					}
					break;
				case SelectionKey.OP_CONNECT:
					if (debugTimes) {
						startNIOTime = System.nanoTime();
					}
					callback.opConnect(channel, currentTimeNanos);
					if (debugTimes) {
						endNIOTime = System.nanoTime();
						final Histogram hg = timerCallback.get(callback);
						if (hg == null) {
							final Histogram h = getHisto();
							h.addObservation(TimeUnit.NANOSECONDS.toMicros(endNIOTime - startNIOTime));
							timerCallback.put(callback, h);
						} else {
							hg.addObservation(TimeUnit.NANOSECONDS.toMicros(endNIOTime - startNIOTime));
						}
					}
					break;
				case SelectionKey.OP_READ:
					if (debugTimes) {
						startNIOTime = System.nanoTime();
					}
					callback.opRead(channel, currentTimeNanos);
					if (debugTimes) {
						endNIOTime = System.nanoTime();
						final Histogram hg = timerCallback.get(callback);
						if (hg == null) {
							final Histogram h = getHisto();
							h.addObservation(TimeUnit.NANOSECONDS.toMicros(endNIOTime - startNIOTime));
							timerCallback.put(callback, h);
						} else {
							hg.addObservation(TimeUnit.NANOSECONDS.toMicros(endNIOTime - startNIOTime));
						}
					}
					break;
				case SelectionKey.OP_WRITE:
					if (debugTimes) {
						startNIOTime = System.nanoTime();
					}
					callback.opWrite(channel, currentTimeNanos);
					if (debugTimes) {
						endNIOTime = System.nanoTime();
						final Histogram hg = timerCallback.get(callback);
						if (hg == null) {
							final Histogram h = getHisto();
							h.addObservation(TimeUnit.NANOSECONDS.toMicros(endNIOTime - startNIOTime));
							timerCallback.put(callback, h);
						} else {
							hg.addObservation(TimeUnit.NANOSECONDS.toMicros(endNIOTime - startNIOTime));
						}
					}
					break;

				case SelectionKey.OP_WRITE + SelectionKey.OP_READ:
					callback.opWrite(channel, currentTimeNanos);
					callback.opRead(channel, currentTimeNanos);
					if (debugTimes) {
						endNIOTime = System.nanoTime();
						final Histogram hg = timerCallback.get(callback);
						if (hg == null) {
							final Histogram h = getHisto();
							h.addObservation(TimeUnit.NANOSECONDS.toMicros(endNIOTime - startNIOTime));
							timerCallback.put(callback, h);
						} else {
							hg.addObservation(TimeUnit.NANOSECONDS.toMicros(endNIOTime - startNIOTime));
						}
					}
					break;

				default:
					throw new RuntimeException("Error unknown ready" + readyOps);
				}

			} else {

				logger.info("Invalid key in selection key:{} ", key);
				try {
					key.cancel();
				} catch (Exception e) {

				}
			}

		}
		selectedKeySet.reset();
		return true;
	}

	/**
	 * implement this interface and pass with registration and get callbacks on the
	 * channel.
	 * 
	 * @author ajt
	 *
	 */
	public interface SelectorCallback {
		/** readable */
		public void opRead(final SelectableChannel channel, final long currentTimeNanos);

		/** acceptable */
		public void opAccept(final SelectableChannel channel, final long currentTimeNanos);

		/** can connect */
		public void opConnect(final SelectableChannel channel, final long currentTimeNanos);

		public void opWrite(final SelectableChannel channel, final long currentTimeNanos);

	}

	long timeUntilNextTimerNS() {
		final MyTimerHandler mth = timerHeap.peek();
		if (mth == null) {
			return Long.MAX_VALUE;
		}
		return mth.nanoTimeWillFireAfter - currentTimeNanos;

	}

	/**
	 * will only fire a small number of timers to prevent blocking too long.
	 */
	void checkTimer() {
		checkThread();

		for (int a = MAX_TIMER_BATCH_SIZE; timerHeap.size() > 0 && a > 0; a--) {
			final MyTimerHandler handler = timerHeap.peek();
			if (currentTimeNanos < handler.nanoTimeWillFireAfter) {
				// first element in the heap is not ready to fire yet.
				break;
			}
			final MyTimerHandler removed = timerHeap.remove();
			assert (removed == handler);

			handler.isRegistered = false; // unregister. timer objects can register themselves again. eg a hb
			final long timerLateBy = currentTimeNanos - handler.nanoTimeWillFireAfter;
			final long startTimerAt = System.nanoTime(); // dont need to use custom clock for an absolute timing.
			handler.timerCallback.timerCallback(handler.nanoTimeWillFireAfter, currentTimeNanos);
			final long finishTimerAt = System.nanoTime(); // replace with rtdsc counter.
			final long tookToRun = finishTimerAt - startTimerAt;
			if (tookToRun > SLOW_TIMER_WARN) {
				logger.warn("Slow timerCallback runtime took(us):{} name:{} class:{}",
						TimeUnit.NANOSECONDS.toMicros(tookToRun), handler.timerName, handler.timerCallback);
			}
			if (timerStats) {
				handler.timerHistogram.addObservation(tookToRun);
				handler.lateBy.addObservation(timerLateBy);
			}
		}
	}

	/**
	 * create a timer handler which is used to control the registration of the
	 * callback
	 */
	public TimerHandler createTimer(final TimerCallback callback, final String timerName) {
		final MyTimerHandler th = new MyTimerHandler(callback, timerName);
		if (timerLatencyReport != null) {
			timerLatencyReport.addTimerHandler(th);
		}
		return th;
	}

	/**
	 * logic to manipulate the timer heap
	 * 
	 * @author ajt
	 *
	 */
	final class MyTimerHandler implements TimerHandler, Comparable<MyTimerHandler>, Comparator<MyTimerHandler> {

		final TimerCallback timerCallback;
		final String timerName;
		final Histogram timerHistogram;
		final Histogram lateBy;
		boolean isRegistered = false;
		long cancelCount;
		long registerCount;
		long nanoTimeWillFireAfter;

		public MyTimerHandler(final TimerCallback callback, final String name) {
			this.timerCallback = callback;
			this.timerName = name;
			timerHistogram = getHisto();
			lateBy = getHisto();
		}

		@Override
		public int compareTo(final MyTimerHandler o) {
			if (o == null) {
				return 1;
			}
			if (o.nanoTimeWillFireAfter < nanoTimeWillFireAfter) {
				return 1;
			}
			if (o.nanoTimeWillFireAfter > nanoTimeWillFireAfter) {
				return -1;
			}
			return 0;
		}

		@Override
		public boolean isRegistered() {
			return isRegistered;
		}

		@Override
		public boolean cancelTimer() {
			checkThreadIfStarted();
			cancelCount++;
			if (isRegistered) {
				final boolean removed = timerHeap.remove(this);
				if (removed == false) {
					throw new RuntimeException("Error timer was not removed from heap:" + timerName + " size:timerHeap:"
							+ timerHeap.size() + " cancelCount:" + cancelCount);
				}
				isRegistered = false;
				return true;
			}
			return false;
		}

		@Override
		public boolean fireAt(final long absTimeNano) {
			checkThreadIfStarted();
			if (isRegistered) {
				final boolean removed = timerHeap.remove(this);
				if (removed == false) {
					throw new RuntimeException("Error timer was not removed from heap:" + timerName + " count:"
							+ timerHistogram.getCount() + " cancelCount:" + cancelCount);
				}
			}
			nanoTimeWillFireAfter = absTimeNano;
			final boolean added = timerHeap.add(this);
			if (added == false) {
				throw new RuntimeException("Error timer was not added to heap:" + timerName);
			}
			isRegistered = true;
			registerCount++;
			return true;
		}

		@Override
		public boolean fireIn(final long relTimeNano) {

			return fireAt(currentTimeNanos + relTimeNano);
		}

		@Override
		public int compare(final MyTimerHandler o1, final MyTimerHandler o2) {
			if (o1.nanoTimeWillFireAfter < o2.nanoTimeWillFireAfter) {
				return -1;
			}
			if (o1.nanoTimeWillFireAfter > o2.nanoTimeWillFireAfter) {
				return 1;
			}
			return 0;
		}

		@Override
		public long currentNanoTime() {
			return currentTimeNanos;
		}

		Histogram getTimerHistogram() {
			return timerHistogram;
		}

		public String toString() {
			return "Timer:" + timerName + " canceled:" + cancelCount + " fired:" + timerHistogram.getCount()
					+ " registerCount:" + registerCount + " isRegistered:" + isRegistered;
		}

		String getTimerName() {
			return timerName;
		}

		Histogram getLateByHistogram() {
			return lateBy;
		}

	}

	/**
	 * register a timer with the selector. use the key to determin the callbacks
	 * required
	 * 
	 * @param channel
	 * @param callback
	 * @return selectionKey
	 * @throws ClosedChannelException
	 */
	public SelectionKey registerSelectableChannel(final SelectableChannel channel, final SelectorCallback callback)
			throws ClosedChannelException {
		final SelectionKey key = channel.register(selector, 0, callback);
		@SuppressWarnings("unused")
		final int validOps = channel.validOps();
		return key;
	}

	/**
	 * for interacting with and setting the timer. can be reused to schedule the
	 * timer callback many times, but each timer can only be set once. if the time
	 * is set when already registered this will adjust the existing timercallback
	 * rather than create a new one
	 * 
	 * @author ajt
	 *
	 */
	public interface TimerHandler {
		/**
		 * check if already registered, true if so
		 * 
		 * @return boolean
		 */
		public boolean isRegistered();

		/** cancel registration of timer */
		public boolean cancelTimer();

		/** set to fire at an absolute time */
		public boolean fireAt(final long absTimeNano);

		/**
		 * fire at some time in the future
		 * 
		 * @param relTimeNano
		 * @return
		 */
		public boolean fireIn(final long relTimeNano);

		/**
		 * as per set when clock most recently read
		 * 
		 * @return
		 */
		public long currentNanoTime();

	}

	/**
	 * implement for timer callbacks
	 * 
	 * @author ajt
	 *
	 */
	public interface TimerCallback {
		/** handle the callback */
		public void timerCallback(final long dueAt, final long currentNanoTime);
	}

	@Override
	public void close() {
		logger.info("Closing selector timerDepth:{}", timerHeap.size());
		if (isClosed) {
			logger.info("Already closed");
			return;
		}
		isClosed = true;

		strategyExecutor.close();
		selector.wakeup();

		try {
			selector.close();
		} catch (final IOException ex) {
			logger.info("Error closing selector", ex);
		}

		int counter = 256;
		if (timerStats) {
			logger.info("timerStats currentTime:{}", currentTimeNanos);
			timerLatencyReport.callback.timerCallback(0, 0);
		}
		while (timerHeap.size() > 0 && counter-- > 0) {
			MyTimerHandler timer = timerHeap.poll();
			logger.info("outstanding timer:{} cancelCount:{} fireCount:{} at:{} in(us):{}", timer.timerName,
					timer.cancelCount, timer.timerHistogram.getCount(), timer.nanoTimeWillFireAfter,
					TimeUnit.NANOSECONDS.toMicros(timer.nanoTimeWillFireAfter - timer.currentNanoTime()));
		}
		if (debugTimes) {
			logger.info("DebugStats CALLBACK TIME DEBUG");
			logger.info("WaitCallbacks selectCount:{} {}", selectCount, toStringHisto(waitHisto));
			timerCallback.forEach(new BiConsumer<NIOWaitStrategy.SelectorCallback, Histogram>() {

				@Override
				public void accept(final NIOWaitStrategy.SelectorCallback t, final Histogram u) {
					logger.info("DumpStatsStats:{} histo:{}", t, toStringHisto(u));
				}
			});
		}

		logger.info("Close complete");
	}

	/**
	 * very simple clock interface which returns the time since 1970/01/1 in
	 * nanoseconds rather than milliseconds can be used to allow time based testing
	 * 
	 * @author ajt
	 *
	 */
	public interface NIOClock {
		public long getTimeNanos();

	}

	/**
	 * simple clock to be a little more precise than milliseconds Calibrates itself
	 * by looking at the nanosecond time when the millisecond rolls over and using
	 * this to convert the
	 */

	public static NIOClock getDefaultClock() {
		return new NIOClock() {
			private long nanoOffset = 0;
			{
				final long endMillis = System.currentTimeMillis() + 2;
				long currentMillis = System.currentTimeMillis();
				long currentNanos = System.nanoTime();
				while (currentMillis < endMillis) {
					currentMillis = System.currentTimeMillis();
					currentNanos = System.nanoTime();
				}

				nanoOffset = TimeUnit.MILLISECONDS.toNanos(currentMillis) - currentNanos;
			}

			@Override
			public long getTimeNanos() {
				return nanoOffset + System.nanoTime();
			}
		};
	}

}
