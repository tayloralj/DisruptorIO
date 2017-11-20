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

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ajt.disruptorIO.NIOWaitStrategy.TimerCallback;
import com.ajt.disruptorIO.NIOWaitStrategy.TimerHandler;
import com.lmax.disruptor.collections.Histogram;

/**
 * simple timer callback class which attempts to record how long it takes to get
 * called after scheduling itself
 */
final public class LatencyTimer {
	private final Logger logger = LoggerFactory.getLogger(LatencyTimer.class);
	private TimerHandler handler;
	private TimerCallbackImpl callback;
	private Histogram histo;

	public LatencyTimer() {
		callback = new TimerCallbackImpl();
		histo = new Histogram(
				new long[] { 5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 140, 160, 180, 200, 300, 400, 600,
						800, 1000, 2000, 4000, 8000, 16000, 32000, 64000, 128000, 256000, 512000, Long.MAX_VALUE });

	}

	public void register(final NIOWaitStrategy strategy) {
		handler = strategy.createTimer(callback, callback.name);
		handler.fireIn(1000);
	}

	private AtomicBoolean isStopped = new AtomicBoolean(false);

	/** call from any thread to request a shutdown on the next callback */
	public void stop() {
		isStopped.set(true);
	}

	private class TimerCallbackImpl implements TimerCallback {
		private final String name = "timerLatencyMeasure";

		private long interval = 1;
		private long lastFiredAtActual = System.nanoTime();
		private long count = 0;

		@Override
		public void timerCallback(final long dueAt, final long currentNanoTime) {
			if (isStopped.get() == true) {
				logger.info(" histo size:{} histo:{} ", histo.getCount(), histo.toString());
				for (int a = 0; a < histo.getSize(); a++) {
					logger.info("Upper:{} count:{}", histo.getUpperBoundAt(a), histo.getCountAt(a));
				}
				return;
			}

			count++;
			final long actualNanoTime = System.nanoTime();
			
			histo.addObservation(actualNanoTime - lastFiredAtActual);
			if (count < 11000) {
				histo.clear();
			}

			lastFiredAtActual = actualNanoTime;
			handler.fireIn(interval);

		}

	}

}
