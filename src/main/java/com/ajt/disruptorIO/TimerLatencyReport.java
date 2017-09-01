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
class TimerLatencyReport {
	private final Logger logger = LoggerFactory.getLogger(TimerLatencyReport.class);

	final TimerLatencyCallbackImpl callback;
	TimerHandler timerHandler;
	final ArrayList<MyTimerHandler> timerList;
	final long timerReportInterval;

	public TimerLatencyReport(NIOWaitStrategy was) {
		timerReportInterval = TimeUnit.SECONDS.toNanos(5);
		callback = new TimerLatencyCallbackImpl();
		timerList = new ArrayList<>(32);
	}

	void addTimerHandler(MyTimerHandler th) {
		timerList.add(th);
	}

	class TimerLatencyCallbackImpl implements TimerCallback {

		@Override
		public void timerCallback(long dueAt, long currentNanoTime) {
			for (int a = 0; a < timerList.size(); a++) {
				final MyTimerHandler mth = timerList.get(a);
				if (mth.timerHistogram.getCount() > 0) {
					logger.info("TimerStats timer:{} count:{} runTime:{} lateBy:{} ", mth.timerName,
							mth.timerHistogram.getCount(), mth.timerHistogram.toString(), mth.lateBy.toString());

					mth.timerHistogram.clear();
					mth.lateBy.clear();
				}
			}
			timerHandler.fireIn(timerReportInterval);
		}

	}
}
