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

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ajt.disruptorIO.NIOWaitStrategy.MyTimerHandler;
import com.ajt.disruptorIO.NIOWaitStrategy.TimerCallback;
import com.ajt.disruptorIO.NIOWaitStrategy.TimerHandler;

/**
 * prints out histogram stored within each timer. this is also fired on a timer.
 * is registered as part of the wait strategy creat.
 * 
 * @author ajt
 *
 */
final class TimerLatencyReport {
	private final Logger logger = LoggerFactory.getLogger(TimerLatencyReport.class);

	private final ArrayList<MyTimerHandler> timerList;
	final TimerLatencyCallbackImpl callback;
	final long timerReportInterval;
	TimerHandler timerHandler;

	public TimerLatencyReport(final NIOWaitStrategy was) {
		timerReportInterval = TimeUnit.SECONDS.toNanos(60);
		callback = new TimerLatencyCallbackImpl();
		timerList = new ArrayList<>(32);
	}

	void addTimerHandler(final MyTimerHandler th) {
		timerList.add(th);
	}

	final class TimerLatencyCallbackImpl implements TimerCallback {

		@Override
		public void timerCallback(final long dueAt, final long currentNanoTime) {
			for (int a = 0; a < timerList.size(); a++) {
				final MyTimerHandler mth = timerList.get(a);
				if (mth.getTimerHistogram().getCount() > 0) {
					logger.info("TimerStats timer:{}  runTime:{} lateBy:{} ", //
							mth.getTimerName(), NIOWaitStrategy.toStringHisto(mth.getTimerHistogram()),
							NIOWaitStrategy.toStringHisto(mth.getLateByHistogram()));

					mth.getTimerHistogram().clear();
					mth.getLateByHistogram().clear();
				}
			}
			timerHandler.fireIn(timerReportInterval);
		}

	}
}
