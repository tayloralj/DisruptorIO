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
package com.ajt.disruptorIO.helper;

import static org.junit.Assert.assertThat;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ajt.disruptorIO.ConnectionHelper;
import com.ajt.disruptorIO.NIOWaitStrategy;
import com.ajt.disruptorIO.TestEvent;
import com.ajt.disruptorIO.ConnectionHelper.SenderCallback;
import com.ajt.disruptorIO.NIOWaitStrategy.TimerCallback;
import com.ajt.disruptorIO.NIOWaitStrategy.TimerHandler;
import com.lmax.disruptor.collections.Histogram;

/** simple class to handle the connection lifecycle in tests */
class ClientConnectionHelper implements SenderCallback {
	/**
	 * 
	 */

	private final Logger logger = LoggerFactory.getLogger(ClientConnectionHelper.class);
	// private final NIOWaitSelector2NIO2.TestEventClient testEventClient;
	boolean isClosed = false;
	long readShouldBeAtLeast = 0;
	long writeShouldBeAtLeast = 0;
	long bytesRead = 0;
	long bytesReadCallback = 0;
	long messageReadCount = 0;
	long rttReadCount = 0;
	long bytesReadCount = 0;
	long bytesWritten = 0;
	private long writeSignalCount = 0;
	private long writeCount = 0;

	long recvMsgCount = 0;

	final byte[] readBytes;
	final byte[] writeBytes;
	long messageCounter = 0;
	final ByteBuffer writeBuffer;
	final ByteBuffer readBuffer;
	final TimerHandler timerHandler;
	final TimerSenderCallback callback;
	long startTimeNano = 0;
	long closeTimeNano = 0;
	boolean writeBlocked = false;
	long slowTimer = 100 * 1000000;
	long fastTimer = 500;
	final int id;
	final Histogram propHisto = TestEvent.getHisto();
	final Histogram rttHisto = TestEvent.getHisto();
	ConnectionHelper.SenderCallin callin;
	private long startBlockAt = 0;
	private final long writeRatePerSecond;

	boolean isConnected = false;

	public ClientConnectionHelper(//
			final long writeRatePerSecond, //
			final int id, //
			final SocketAddress sa, //
			final NIOWaitStrategy nioWait) {
		this.writeRatePerSecond = writeRatePerSecond;
		this.id = id;

		writeBytes = new byte[256];
		for (int a = 0; a < writeBytes.length; a++) {
			writeBytes[a] = (byte) a;
		}
		writeBuffer = ByteBuffer.wrap(writeBytes);
		writeBuffer.order(ByteOrder.LITTLE_ENDIAN);
		readBytes = new byte[1024 * 1024];
		readBuffer = ByteBuffer.wrap(readBytes);
		readBuffer.order(ByteOrder.LITTLE_ENDIAN);
		callback = new TimerSenderCallback();

		timerHandler = nioWait.createTimer(callback, "NIO connection-" + id);

	}

	public boolean isConnected() {
		return isConnected;
	}

	@Override
	public void connected(final ConnectionHelper.SenderCallin callin) {

		this.callin = callin;
		isConnected = true;
		// start sending own data to get echo'd back
		timerHandler.fireIn(0);
		startTimeNano = timerHandler.currentNanoTime() - 1;
		logger.info("opConnect Complete :" + callin);

	}

	// flow controol

	@Override
	public void writeNowBlocked(final ConnectionHelper.SenderCallin callin) {
		logger.info(" client blocked write callin:{}", callin.getLocalAddress());
		writeBlocked = true;
		startBlockAt = System.nanoTime();
	}

	@Override
	public void writeUnblocked(final ConnectionHelper.SenderCallin callin) {
		logger.info("client unblocked write for(us):{} callin:{}",
				TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - startBlockAt), callin.getLocalAddress());
		writeBlocked = false;
	}

	@Override
	public void readData(final ConnectionHelper.SenderCallin callin, final ByteBuffer buffer) {
		bytesReadCount++;
		bytesRead += buffer.remaining();
		readBuffer.put(buffer);
		int bufferPosition = readBuffer.position(); // point in buffer of last read byte
		int startPosition = 0;

		readBuffer.position(startPosition);
		readBuffer.limit(bufferPosition);
		while (true) {
			if (readBuffer.limit() - readBuffer.position() < 8) {
				if (logger.isTraceEnabled()) {
					logger.trace("SHort Buff COMPACT :{} :{}", readBuffer.limit(), readBuffer.position());
				}
				readBuffer.compact();
				break;
			}
			final int length = TestEvent.getIntFromArray(readBytes, readBuffer.position() + TestEvent.Offsets.length);
			assertThat("failed - too short", //
					length, Matchers.greaterThan(16));
			//
			if (startPosition + length <= readBuffer.limit()) {
				// got a full message
				final int type = TestEvent.getIntFromArray(readBytes, readBuffer.position() + 4);
				switch (type) {
				case TestEvent.MessageType.clientRequestMessage:
					logger.debug("unknown type");
					break;
				case TestEvent.MessageType.dataFromServerEvent: {
					final long sendTime = TestEvent.getLongFromArray(readBytes,
							readBuffer.position() + TestEvent.Offsets.time);
					final long nowNanoTime = System.nanoTime();
					propHisto.addObservation(nowNanoTime - sendTime);
					// occasional logging
					if ((messageReadCount & ((256 * 1024) - 1)) == 0) {
						logger.info("id:{} Prop Histo:{} {}", id, propHisto.getCount(),
								TestEvent.toStringHisto(propHisto));

						logger.debug(
								"Took to recv:{} len:{} posn:{} lim:{} readCount:{} bytesRead:{} bytesReadCount:{}",
								(nowNanoTime - sendTime), length, startPosition, readBuffer.limit(), bytesRead,
								bytesRead, bytesReadCount);

					}
					messageReadCount++;
				}
					break;
				case TestEvent.MessageType.serverResponseMessage: {
					// logger.debug("ServerRTTMessage");
					final long sendTime = TestEvent.getLongFromArray(readBytes,
							readBuffer.position() + TestEvent.Offsets.time);
					final long nowNanoTime = System.nanoTime();
					rttHisto.addObservation(nowNanoTime - sendTime);
					rttReadCount++;
				}
					break;
				default:
					Assert.fail("Error unknown message type:" + type + " len:" + length);
				}
				startPosition += length;
				if (startPosition == readBuffer.limit()) {
					readBuffer.clear();
					break;
				} else {
					assertThat(startPosition, Matchers.lessThan(readBuffer.limit()));
					readBuffer.position(startPosition);
				}
			} else {
				if (logger.isTraceEnabled()) {
					logger.info("Client COMPACT posn:{} lim:{}", readBuffer.position(), readBuffer.limit());
				}
				readBuffer.compact();
				break;
			}
		}

	}

	@Override
	public void closed(final ConnectionHelper.SenderCallin callin) {
		logger.info("Close from callin:{}", callin);
		close();

	}

	public void close() {
		if (isClosed) {
			return;
		}
		closeTimeNano = timerHandler.currentNanoTime();
		isConnected = false;
		isClosed = true;
		if (timerHandler.isRegistered()) {
			timerHandler.cancelTimer();
		}
		logger.info(
				"\tClientClosing id:{} \n"
						+ "\n\tREAD_CLIENT:bytesRead:{} messageSentCounter:{} serverMessageRead:{} serverRttRead:{}"
						+ "\n\tWRITE_CLIENT: totalWrite:{} writeSignalCount:{} writeCount:{}" //
						+ "\n\t Propogate time Histo:{} {}"//
						+ "\n\t RTT time Histo:{} {}",
				id, bytesRead, messageCounter, messageReadCount, rttReadCount, //
				bytesWritten, writeSignalCount, writeCount, propHisto.getCount(), //
				TestEvent.toStringHisto(propHisto), rttHisto.getCount(), TestEvent.toStringHisto(rttHisto));
		logger.info("THROUGHPUT ID:{} Read(mbit):{} Write(mbit):{} connectedFor(ms):{}", //
				id, //
				bytesRead * 8000 / (closeTimeNano - startTimeNano), //
				bytesWritten * 8000 / (closeTimeNano - startTimeNano), //
				TimeUnit.NANOSECONDS.toMillis(closeTimeNano - startTimeNano));

		propHisto.clear();
		rttHisto.clear();
	}

	/** sent out messages to the client periodically */
	private class TimerSenderCallback implements TimerCallback {
		long writeAttemptCounter;

		@Override
		public void timerCallback(final long dueAt, final long currentNanoTime) {
			try {
				if (isClosed) {
					logger.info("already closed");
					return;
				}
				if (writeBlocked) {
					logger.debug("Write blocked, could not send client timer");
					timerHandler.fireIn(slowTimer);
					return;
				}

				final long elapsed = currentNanoTime - startTimeNano;
				boolean somethingWritten = false;
				while (elapsed * writeRatePerSecond > bytesWritten * 1000_000_000L && !writeBlocked) {

					TestEvent.putIntToArray(writeBytes, TestEvent.Offsets.type,
							TestEvent.MessageType.clientRequestMessage);
					TestEvent.putIntToArray(writeBytes, TestEvent.Offsets.length, writeBytes.length);
					TestEvent.putLongToArray(writeBytes, TestEvent.Offsets.time, System.nanoTime());
					TestEvent.putLongToArray(writeBytes, TestEvent.Offsets.seqnum, messageCounter++);

					writeBuffer.position(0);
					writeBuffer.limit(writeBytes.length);
					final long couldSend = callin.sendMessage(writeBytes, 0, writeBytes.length);
					if (couldSend == -1) {
						messageCounter--;
						writeAttemptCounter++;
						if (writeAttemptCounter % 16384 == 0) {
							logger.debug(
									"got blocked trying to write using timer, buffer full :-( writeBlocked:{} send:{} counter:{} bytesWritten:{} somethingWritten:{}",
									writeBlocked, messageCounter, writeAttemptCounter, bytesWritten, somethingWritten);
						}
						break;
					}
					somethingWritten = true;
					bytesWritten += couldSend;
					writeCount++;

				}
				if (somethingWritten) {
					callin.flush();
					writeSignalCount++;
					timerHandler.fireIn(fastTimer);
				} else {
					timerHandler.fireIn(slowTimer);
				}

			} catch (final Exception e) {
				logger.error("Errro in callback", e);
				close();
			}
		}

	}

}