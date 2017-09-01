package com.ajt.disruptorIO;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
class NIOWaitStrategy implements WaitStrategy, AutoCloseable {
	private final Logger logger = LoggerFactory.getLogger(NIOWaitStrategy.class);
	final NIOClock clock;

	final Selector selector;
	final NioSelectedKeySet selectedKeySet;
	/** temp until better struct */
	final PriorityQueue<MyTimerHandler> timerHeap;
	long currentTimeNanos = 0;
	final long SLOW_TIMER_WARN;
	final TimerLatencyReport timerLatencyReport;
	final NIOWaitStrategyExecutor strategyExecutor;

	static final Field SELECTED_KEYS_FIELD;
	static final Field PUBLIC_SELECTED_KEYS_FIELD;

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
		this.clock = clock;

		logger.info("Created NIOWait with clock:{}", clock);
		timerHeap = new PriorityQueue<>(255);
		timerLatencyReport = new TimerLatencyReport(this);
		timerLatencyReport.timerHandler = createTimer(timerLatencyReport.callback, "TimerLatencyReport");
		timerLatencyReport.timerHandler.fireIn(timerLatencyReport.timerReportInterval);

		strategyExecutor = new NIOWaitStrategyExecutor(this);
		setNanoTime();

		SLOW_TIMER_WARN = 500_000L; // 500us
		// use own selector keyset to remove obj creation.
		try {
			selector = Selector.open();
			selectedKeySet = new NioSelectedKeySet();
			SELECTED_KEYS_FIELD.set(selector, selectedKeySet);
			PUBLIC_SELECTED_KEYS_FIELD.set(selector, selectedKeySet);
		} catch (final Exception ex) {
			throw new RuntimeException(ex);
		}

	}

	public ScheduledExecutorService getScheduledExecutor() {
		return strategyExecutor.executorService;
	}

	/**
	 * first check for data from ringbuffer then from nio selectable channel then
	 * from timer.
	 */
	@Override
	public long waitFor(final long sequence, final Sequence cursor, final Sequence dependentSequence,
			final SequenceBarrier barrier) throws AlertException, InterruptedException, TimeoutException {

		long availableSequence;

		while ((availableSequence = dependentSequence.get()) < sequence) {
			barrier.checkAlert();

			setNanoTime();
			if (checkNIO()) {
				continue;
			}
			checkTimer();
		}

		return availableSequence;

	}

	/** set the time currently */
	void setNanoTime() {
		currentTimeNanos = clock.getTimeNanos();
	}

	@Override
	public void signalAllWhenBlocking() {
		// TODO Auto-generated method stub

	}

	boolean checkNIO() {
		try {

			final int keyCount = selector.selectNow();
			if (keyCount == 0) {
				return false;
			}

			for (int aaa = 0; aaa < selectedKeySet.size(); aaa++) {
				final SelectionKey key = selectedKeySet.keys()[aaa];
				if (key.isValid()) {
					final int readyOps = key.readyOps();
					final SelectorCallback callback = (SelectorCallback) key.attachment();
					final SelectableChannel channel = key.channel();
					switch (readyOps) {
					case SelectionKey.OP_ACCEPT:
						callback.opAccept(channel, currentTimeNanos);
						break;
					case SelectionKey.OP_CONNECT:
						callback.opConnect(channel, currentTimeNanos);
						break;
					case SelectionKey.OP_READ:
						callback.opRead(channel, currentTimeNanos);
						break;
					case SelectionKey.OP_WRITE:
						callback.opWrite(channel, currentTimeNanos);
						break;

					case SelectionKey.OP_WRITE + SelectionKey.OP_READ:
						callback.opWrite(channel, currentTimeNanos);
						callback.opRead(channel, currentTimeNanos);
						break;

					default:
					}

				} else {
					logger.info("Invalid key in selection key:{} interest:{} Objec:{}", key, key.interestOps(),
							key.attachment());
					try {
						key.cancel();
					} catch (Exception e) {

					}
				}

			}
			selectedKeySet.reset();
			return true;
		} catch (final IOException ex) {
			LangUtil.rethrowUnchecked(ex);

		}
		return false;
	}

	/**
	 * implement this interface and pass with registration and get callbacks on the
	 * channel.
	 * 
	 * @author ajt
	 *
	 */
	public interface SelectorCallback {
		public void opRead(final SelectableChannel channel, final long currentTimeNanos);

		public void opAccept(final SelectableChannel channel, final long currentTimeNanos);

		public void opConnect(final SelectableChannel channel, final long currentTimeNanos);

		public void opWrite(final SelectableChannel channel, final long currentTimeNanos);

	}

	/**
	 * will only fire a small number of timers to prevent blocking too long.
	 */
	void checkTimer() {
		for (int a = 5; timerHeap.size() > 0 && a > 0; a--) {
			final MyTimerHandler handler = timerHeap.peek();
			if (currentTimeNanos < handler.nanoTimeWillFireAfter) {
				// first element in the heap is not ready to fire yet.
				break;
			}
			timerHeap.poll(); // remove from timer heap
			handler.isRegistered = false; // unregister. timer objects can register themselves again. eg a hb
			final long timerLateBy = currentTimeNanos - handler.nanoTimeWillFireAfter;
			final long startTimerAt = System.nanoTime();
			handler.timerCallback.timerCallback(handler.nanoTimeWillFireAfter, currentTimeNanos);
			final long finishTimerAt = System.nanoTime(); // replace with rtdsc counter.
			final long tookToRun = finishTimerAt - startTimerAt;
			if (tookToRun > SLOW_TIMER_WARN) {
				logger.warn("Slow timer took:{} name:{}", tookToRun, handler.timerName);
			}

			handler.timerHistogram.addObservation(tookToRun);
			handler.lateBy.addObservation(timerLateBy);
		}
	}

	public TimerHandler createTimer(final TimerCallback callback, final String timerName) {
		final MyTimerHandler th = new MyTimerHandler(callback, timerName);
		logger.debug("Created new timer:{}", timerName);
		timerLatencyReport.addTimerHandler(th);

		return th;
	}

	/**
	 * logic to manipulate the timer heap
	 * 
	 * @author ajt
	 *
	 */
	class MyTimerHandler implements TimerHandler, Comparable<MyTimerHandler>, Comparator<MyTimerHandler> {
		final long[] histogramBin = new long[] { 50, 100, 200, 400, 600, 800, 1000, 2000, 3000, 4000, 6000, 12000,
				16000, 32000, 64000, 128000, 256000, 512000, 1024000, 2048000, 4096000, 32768000 };

		private final TimerCallback timerCallback;
		private boolean isRegistered = false;
		private long nanoTimeWillFireAfter;
		final String timerName;
		final Histogram timerHistogram;
		final Histogram lateBy;

		public MyTimerHandler(final TimerCallback callback, final String name) {
			this.timerCallback = callback;
			this.timerName = name;
			timerHistogram = new Histogram(histogramBin);
			lateBy = new Histogram(histogramBin);
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

			if (isRegistered) {
				final boolean removed = timerHeap.remove(this);
				if (removed == false) {
					throw new RuntimeException("Error timer was not removed from heap:" + timerName);
				}
				return true;
			}
			return false;
		}

		@Override
		public boolean fireAt(final long absTimeNano) {
			if (isRegistered) {
				final boolean removed = timerHeap.remove(this);
				if (removed == false) {
					throw new RuntimeException("Error timer was not removed from heap:" + timerName);
				}
			}
			nanoTimeWillFireAfter = absTimeNano;
			final boolean added = timerHeap.add(this);
			if (added == false) {
				throw new RuntimeException("Error timer was not added to heap:" + timerName);
			}
			isRegistered = true;
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
		return key;
	}

	/**
	 * for interacting with and setting the timer
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
	public void close() throws Exception {
		logger.info("Closing selector");
		timerLatencyReport.callback.timerCallback(0, 0);
		strategyExecutor.close();
		selector.wakeup();
		try {
			selector.close();
		} catch (final IOException ex) {
			LangUtil.rethrowUnchecked(ex);

		}

	}

	public interface NIOClock {
		public long getTimeNanos();

	}

	static NIOClock getDefaultClock() {
		return new NIOClock() {
			long nanoOffset = 0;
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
