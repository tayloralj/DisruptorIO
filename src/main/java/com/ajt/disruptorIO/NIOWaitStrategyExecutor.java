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

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ajt.disruptorIO.NIOWaitStrategy.TimerCallback;
import com.ajt.disruptorIO.NIOWaitStrategy.TimerHandler;

/**
 * is used to schedule tasks via the executor api onto the waitstrategy thread
 * via a timer
 */
class NIOWaitStrategyExecutor implements Closeable {

	private final Logger logger = LoggerFactory.getLogger(NIOWaitStrategyExecutor.class);
	private final TimerHandler timerHandler;
	private final StrategyExecutorTimerCallback callback;
	private final TimerExecutorService executorService;
	private final long timerCheckInterval = 50_000L;
	private final long timerBusyInterval = 5_000L;
	private final ConcurrentLinkedQueue<TimerScheduledFuture<?>> queue;
	private final ArrayList<CallableDelayedTask> delayedTaskTimerCache;
	private volatile boolean isShutdown = false;
	private volatile long mostRecentTime = 0;
	private final NIOWaitStrategy wait;
	private static int counter = 0;

	public NIOWaitStrategyExecutor(NIOWaitStrategy waiter) {
		wait = waiter;
		callback = new StrategyExecutorTimerCallback();
		timerHandler = waiter.createTimer(callback, "StrategyExecutorTimer");
		timerHandler.fireIn(timerBusyInterval);
		executorService = new TimerExecutorService();
		queue = new ConcurrentLinkedQueue<>();
		delayedTaskTimerCache = new ArrayList<>();

		// simple cache
		for (int a = 0; a < 20; a++) {
			delayedTaskTimerCache.add(new CallableDelayedTask());
		}
		logger.info("Created pollInterval:{} busyInterval:{}", timerCheckInterval, timerBusyInterval);
	}

	public void close() {
		logger.info("Exector Close unscheduled Jobs:{}", queue.size());
		int maxDump = 256; // protection
		while (queue.size() > 0 && maxDump-- > 0) {
			final TimerScheduledFuture<?> future = queue.poll();
			logger.info("job not scheduled timer:{}", future);
		}
		isShutdown = true;
	}

	ScheduledExecutorService getExecutorService() {
		return executorService;
	}

	private class CallableDelayedTask implements TimerCallback {
		private final TimerHandler th;
		private TimerScheduledFuture<?> callable;

		public CallableDelayedTask() {
			th = wait.createTimer(this, "DelayedTask :" + (counter++));
		}

		@Override
		public void timerCallback(final long dueAt, final long currentNanoTime) {
			mostRecentTime = currentNanoTime;

			if (callable == null) {
				logger.error("Error callable was null " + dueAt, currentNanoTime);
				return;
			}

			if (callable.isCancelled()) {
				return;
			}
			if (callable.isDone()) {
				return;
			}
			callable.run();
			delayedTaskTimerCache.add(this);
			callable = null;
		}

	}

	class StrategyExecutorTimerCallback implements TimerCallback {

		@Override
		public void timerCallback(final long dueAt, final long currentNanoTime) {
			mostRecentTime = currentNanoTime;
			if (queue.size() == 0) {
				timerHandler.fireIn(timerCheckInterval);
				return;
			}
			try {
				final TimerScheduledFuture<?> taskWrapper = queue.poll();
				if (taskWrapper.runAfterNS < currentNanoTime) {
					taskWrapper.run();
				} else {
					final CallableDelayedTask delayedTask;
					if (delayedTaskTimerCache.size() == 0) {
						delayedTask = new CallableDelayedTask();
					} else {
						delayedTask = delayedTaskTimerCache.remove(delayedTaskTimerCache.size() - 1);
					}
					delayedTask.callable = taskWrapper;
					delayedTask.th.fireAt(taskWrapper.runAfterNS);
				}
			} catch (Exception e) {
				logger.error("Error running or scheduling task", e);
			}
			// reschedule
			if (queue.size() == 0) {
				timerHandler.fireIn(timerCheckInterval);
			} else {
				timerHandler.fireIn(timerBusyInterval);
			}

		}

	}

	private class TimerScheduledFuture<V> extends FutureTask<V> implements ScheduledFuture<V> {
		final long runAfterNS;

		public TimerScheduledFuture(Callable<V> callable, final long ns) {
			super(callable);
			runAfterNS = ns;
		}

		@Override
		public long getDelay(TimeUnit unit) {
			return unit.convert(runAfterNS - mostRecentTime, TimeUnit.NANOSECONDS);
		}

		@Override
		public int compareTo(Delayed o) {
			final long oNano = o.getDelay(TimeUnit.NANOSECONDS);
			if (runAfterNS < oNano) {
				return -1;
			}
			if (runAfterNS > oNano) {
				return 1;
			}
			return 0;
		}

	}

	private class TimerExecutorService implements ScheduledExecutorService {

		@Override
		public void shutdown() {
			isShutdown = true;

		}

		@Override
		public List<Runnable> shutdownNow() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public boolean isShutdown() {
			return isShutdown;
		}

		@Override
		public boolean isTerminated() {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public <T> Future<T> submit(final Callable<T> task) {
			if (isShutdown) {
				throw new RuntimeException("Error already shutdown");
			}
			final TimerScheduledFuture<T> ft = new TimerScheduledFuture<>(task, -1);
			queue.add(ft);

			return ft;
		}

		@Override
		public <T> Future<T> submit(final Runnable task, final T result) {
			if (isShutdown) {
				throw new RuntimeException("Error already shutdown");
			}
			final Callable<T> r = new Callable<T>() {

				@Override
				public T call() throws Exception {
					task.run();
					return result;
				}
			};
			final TimerScheduledFuture<T> ft = new TimerScheduledFuture<>(r, -1);
			queue.add(ft);
			return ft;
		}

		@Override
		public Future<?> submit(final Runnable task) {
			if (isShutdown) {
				throw new RuntimeException("Error already shutdown");
			}
			final Callable<Runnable> r = new Callable<Runnable>() {

				@Override
				public Runnable call() throws Exception {
					task.run();
					return task;
				}
			};
			final TimerScheduledFuture<Runnable> ft = new TimerScheduledFuture<>(r, -1);
			queue.add(ft);

			return ft;
		}

		@Override
		public <T> List<Future<T>> invokeAll(final Collection<? extends Callable<T>> tasks)
				throws InterruptedException {
			return invokeAll(tasks, 100, TimeUnit.DAYS);
		}

		@Override
		public <T> List<Future<T>> invokeAll(final Collection<? extends Callable<T>> tasks, final long timeout,
				final TimeUnit unit) throws InterruptedException {

			final Iterator<? extends Callable<T>> i = tasks.iterator();
			final ArrayList<Future<T>> results = new ArrayList<>();
			final long completeByNS = mostRecentTime + unit.toNanos(timeout);
			// put all onto thread
			while (i.hasNext()) {
				final Callable<T> callable = i.next();
				final Future<T> future = submit(callable);
				results.add(future);
			}
			// wait for execution to complete
			for (int a = 0; a < results.size() && mostRecentTime < completeByNS; a++) {
				try {
					while (!results.get(a).isDone()) {
						try {
							results.get(a).get(1000, TimeUnit.NANOSECONDS);
						} catch (TimeoutException te) {
						}

					}
				} catch (ExecutionException ee) {
					logger.error("error in invoke", ee);
				}
			}
			// cancel anything not done by timeout
			for (int a = 0; a < results.size() && mostRecentTime > completeByNS; a++) {
				if (results.get(a).isDone()) {

				} else {
					results.get(a).cancel(true);
				}
			}
			return results;
		}

		@Override
		public <T> T invokeAny(final Collection<? extends Callable<T>> tasks)
				throws InterruptedException, ExecutionException {
			try {
				return invokeAny(tasks, 100, TimeUnit.DAYS);
			} catch (TimeoutException te) {
				throw new ExecutionException(te);
			}
		}

		@Override
		public <T> T invokeAny(final Collection<? extends Callable<T>> tasks, final long timeout, final TimeUnit unit)
				throws InterruptedException, ExecutionException, TimeoutException {
			final Iterator<? extends Callable<T>> i = tasks.iterator();
			final ArrayList<Future<T>> results = new ArrayList<>();
			final long completeByNS = mostRecentTime + unit.toNanos(timeout);
			// put all onto thread
			while (i.hasNext()) {
				final Callable<T> callable = i.next();
				final Future<T> future = submit(callable);
				results.add(future);
			}
			T result = null;
			while (result != null) {
				for (int a = 0; a < results.size(); a++) {
					if (results.get(a).isDone()) {
						result = results.remove(a).get();
						break;
					}
				}
				if (mostRecentTime > completeByNS) {
					throw new TimeoutException("Error no task completed within:" + timeout + " " + unit);
				}
			}
			for (int a = 0; a < results.size(); a++) {
				results.get(a).cancel(true);
			}
			return result;
		}

		@Override
		public void execute(final Runnable command) {
			submit(command);

		}

		@Override
		public ScheduledFuture<?> schedule(final Runnable command, final long delay, final TimeUnit unit) {
			final Callable<Runnable> r = new Callable<Runnable>() {

				@Override
				public Runnable call() throws Exception {
					command.run();
					return command;
				}
			};
			return schedule(r, delay, unit);
		}

		@Override
		public <V> ScheduledFuture<V> schedule(final Callable<V> callable, final long delay, final TimeUnit unit) {
			final long nsDelay = unit.toNanos(delay);
			final TimerScheduledFuture<V> t = new TimerScheduledFuture<>(callable, mostRecentTime + nsDelay);
			queue.add(t);
			return t;
		}

		@Override
		public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay,
				TimeUnit unit) {
			// TODO Auto-generated method stub
			return null;
		}

	}

}
