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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ajt.disruptorIO.ConnectionHelper;
import com.ajt.disruptorIO.ConnectionHelper.SenderCallback;
import com.ajt.disruptorIO.TestEvent;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.collections.Histogram;

public class ServerConnectionHelper implements EventHandler<TestEvent>, AutoCloseable {
	private final Logger logger = LoggerFactory.getLogger(ServerConnectionHelper.class);
	final AtomicLong counter = new AtomicLong();
	final AtomicBoolean isClosed = new AtomicBoolean(false);
	private final boolean coalsce;
	private final Histogram elapsedHisto = TestEvent.getHisto();
	private final Histogram delayHisto = TestEvent.getHisto();
	private final ServerConnectionHelper.EstablishedServerConnectionCallback[] ecc;
	private final ConnectionHelper serverSenderHelper;

	public volatile SocketAddress remoteAddress = null;
	private ConnectionHelper.SenderCallin serverCallin;

	public ServerConnectionHelper(final ConnectionHelper serverSenderHelper, final boolean compact,
			final InetSocketAddress address, final int maxClientCount) {
		coalsce = compact;
		this.serverSenderHelper = serverSenderHelper;
		ecc = new ServerConnectionHelper.EstablishedServerConnectionCallback[maxClientCount];
		try {
			serverCallin = serverSenderHelper.bindTo(address, new NIOServerCallback());

			remoteAddress = serverCallin.getRemoteAddress();

		} catch (Exception e) {
			logger.error("Error", e);
			Assert.fail("Fail due to exception" + e);
		}
	}

	@Override
	public void onEvent(final TestEvent event, final long sequence, final boolean endOfBatch) throws Exception {
		final long now = System.nanoTime();
		switch (event.type) {
		case data:
			if (ecc[event.targetID] == null) {
				if (endOfBatch) {
					logger.error("Erro no connection with targetID:{})",
							event.targetID + " " + serverCallin.toString());
				}
				return;
			}
			try {
				ecc[event.targetID].handleData(event, sequence, endOfBatch);
				// flush anything without a batch
				if (endOfBatch) {
					for (int a = 0; a < ecc.length; a++) {
						if (ecc[a] != null) {
							try {
								ecc[a].flush();
							} catch (Exception e) {
								logger.error("Error during flush id:" + a + " " + ecc[a], e);
								ecc[a] = null;
							}
						}
					}
				}
			} catch (Exception e) {
				close();
				Assert.fail("Error in test" + e);

			}
			break;

		default:
			logger.error("Unknown sNum:{} type:{} seqNum:{} eob:{}", event.seqNum, event.type, sequence, endOfBatch);
		}
		counter.incrementAndGet();
		event.type = TestEvent.EventType.UNKNOWN;
		final long end = System.nanoTime();
		delayHisto.addObservation(now - event.nanoSendTime);
		elapsedHisto.addObservation(end - now);
	}

	public boolean isClosed() {
		return isClosed.get();
	}

	public void close() {
		logger.info("CLOSESERVER EventServer" //
				+ "\n\tElapsedCallbackTime counter:{} elapsedHistoTime:{}"//
				+ "\n\tDelayDataTime count:{} delayfromClientHisto:{}", //
				elapsedHisto.getCount(), TestEvent.toStringHisto(elapsedHisto), //
				delayHisto.getCount(), TestEvent.toStringHisto(delayHisto));

		for (int a = 0; a < ecc.length; a++) {
			if (ecc[a] != null) {
				logger.info("closeServer id:{} ", a);
				ecc[a].close();
				ecc[a] = null;
			}
		}
		isClosed.set(true);
	}

	class NIOServerCallback implements SenderCallback {
		private final Logger logger = LoggerFactory.getLogger(NIOServerCallback.class);

		@Override
		public void connected(final ConnectionHelper.SenderCallin callin) {
			logger.info("Connected callin:{}", callin);
			remoteAddress = callin.getRemoteAddress();
			boolean matched = false;
			for (int a = 0; a < ecc.length && !matched; a++) {
				if (ecc[a] == null) {

					@SuppressWarnings("resource")
					final ServerConnectionHelper.EstablishedServerConnectionCallback escc = new EstablishedServerConnectionCallback(
							callin, a, coalsce);
					callin.setId(a);
					ecc[a] = escc;
					ecc[a].startTimeNano = System.nanoTime();

					matched = true;
					break;
				}
			}
			if (!matched) {
				try {
					logger.error("Error could not find empty slot. closing");
					callin.close();
				} catch (Exception e) {

				}
			}

		}

		@Override
		public void writeNowBlocked(final ConnectionHelper.SenderCallin callin) {
			logger.info("blocked Server write callin:{}", callin);
			callin.blockRead();
		}

		@Override
		public void writeUnblocked(final ConnectionHelper.SenderCallin callin) {
			// logger.info("unblocked Server write callin:{}", callin);
			serverCallin.unblockRead();
		}

		@Override
		public void closed(final ConnectionHelper.SenderCallin callin) {

			logger.info("closed callin:{}", callin);
			if (ecc[callin.getId()] != null) {

				ecc[callin.getId()].close();
			}
		}

		@Override
		public void readData(final ConnectionHelper.SenderCallin callin, final ByteBuffer buffer) {
			try {
				// logger.info("readData callin:{} buffer:{}", callin, buffer);
				ecc[callin.getId()].read(buffer);
			} catch (Exception e) {
				logger.error("Error reading buffer", e);
				ecc[callin.getId()].close();
			}
		}

	}

	/**
	 * callback to interact with an established client session batches data into
	 * upto 4kb chunks before flushing to socket at the end of the batch or when the
	 * buffer is full.
	 * 
	 * @author ajt
	 *
	 */
	private static class EstablishedServerConnectionCallback implements AutoCloseable {
		private final Logger logger = LoggerFactory
				.getLogger(ServerConnectionHelper.EstablishedServerConnectionCallback.class);

		private boolean closed = false;

		private final ConnectionHelper.SenderCallin serverCallin;
		private final int id;

		private long totalBytesRead = 0;

		private long messageReadCount = 0;
		private long readSignalCount = 0;
		private long totalBytesWritten = 0;
		private long writeSignalCount = 0;
		private long writeCount = 0;

		private long endOfBatchCount = 0;
		private long messageCount = 0;

		private final ByteBuffer readBuffer;
		private final byte[] readBytes;
		private long respSeqNum = 0;
		private final ByteBuffer writeBuffer;

		private final Histogram propHisto = TestEvent.getHisto();
		private final boolean coalsce;
		private long dropped = 0;
		private long startTimeNano = 0;
		private long closeTimeNano = 0;

		EstablishedServerConnectionCallback(final ConnectionHelper.SenderCallin sc, //
				final int id, //
				final boolean coalsce) {
			this.serverCallin = sc;
			this.id = id;
			this.coalsce = coalsce;
			readBytes = new byte[sc.maxBuffer() * 2];
			readBuffer = ByteBuffer.wrap(readBytes);
			readBuffer.order(ByteOrder.LITTLE_ENDIAN);
			writeBuffer = ByteBuffer.allocate(sc.maxBuffer());
			writeBuffer.order(ByteOrder.LITTLE_ENDIAN);
		}

		public void flush() throws IOException {
			try {
				if (serverCallin.bytesInBuffer() > 0) {
					final long bytesWritten = serverCallin.flush();
					if (bytesWritten > 0) {
						totalBytesWritten += bytesWritten;
					}
					writeSignalCount++;

				}
			} catch (IOException e) {
				close();
				throw e;
			}
		}

		public void handleData(final TestEvent event, final long sequence, final boolean endOfBatch) throws Exception {
			messageCount++;
			long flushed = 0;
			if (serverCallin.bufferRemaining() < event.getLength()) {
				if (coalsce) {
					// drop data
					if (logger.isTraceEnabled()) {
						logger.trace("Coalse: drop data size:{}", event.data.length);
					}
					dropped++;
				} else {
					long errorAt = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(5);
					// block writing until data is sent to client
					while (serverCallin.bufferRemaining() < event.getLength()) {
						final long written = serverCallin.flush();
						flushed += written;
						if (System.nanoTime() > errorAt) {
							logger.error("Error timed out. dropping flushed:{}", flushed);
							dropped++;
							break;
						}
					}

				}
			} else {
				totalBytesWritten += serverCallin.sendMessage(event.data, 0, event.getLength());
				writeSignalCount++;
			}
			if (endOfBatch) {
				// change to only flush on end of batch.
				endOfBatchCount++;
				totalBytesWritten += serverCallin.flush();
				writeCount++;
			}
		}

		public void close() {
			if (closed) {
				logger.warn("Already closed:{}", id);
				return;
			}
			closed = true;
			closeTimeNano = System.nanoTime();
			logger.info("id:{} Closing Server dropped:{}"//
					+ "\n\tCallin:{}"//
					+ "\n\tREAD_SERVER: totalBytesRead:{} messageReadCount:{} readSignalCount:{}"//
					+ "\n\tWRITE_SERVER: totalWrite:{} writeSignalCount:{} writeCount:{}" //
					+ "\n\tBATCH: BatchCound:{} EndOfBatch:{} "//
					+ "\n\tHISTO: count:{} hist:{}", //
					id, dropped, serverCallin, //
					totalBytesRead, messageReadCount, readSignalCount, //
					totalBytesWritten, writeSignalCount, writeCount, //
					messageCount, endOfBatchCount, //
					propHisto.getCount(), TestEvent.toStringHisto(propHisto));

			logger.info("SERVER THROUGHPUT ID:{} Read(mbit):{} Write(mbit):{} connectedFor(ms):{}", //
					id, //
					totalBytesRead * 8000 / (closeTimeNano - startTimeNano), //
					totalBytesWritten * 8000 / (closeTimeNano - startTimeNano), //
					TimeUnit.NANOSECONDS.toMillis(closeTimeNano - startTimeNano));

			try {
				serverCallin.close();
			} catch (Exception e) {
				logger.error("Error closing socket", e);
			}

		}

		public void read(final ByteBuffer readData) throws IOException {
			// logger.debug("SERVERREAD {} {} {} {}", readBuffer.position(),
			// readBuffer.limit() + readData.position(),
			// readData.limit());
			readSignalCount++;
			final int bytesRead = readData.remaining();
			totalBytesRead += bytesRead;
			// logger.info("Read:" + read);
			if (readData.position() > readBuffer.remaining()) {
				logger.error("Error not enough space {} {} {} {}", readBuffer.position(), readBuffer.limit(),
						readData.position(), readData.limit());
				Assert.fail();
			}

			try {
				readBuffer.put(readData);
			} catch (BufferOverflowException boe) {
				logger.error("Error not enough space {} {} {} {}", readBuffer.position(), readBuffer.limit(),
						readData.position(), readData.limit());
				Assert.fail();
			}

			int bufferPosition = readBuffer.position(); // point in buffer of last read byte
			int startPosition = 0;

			readBuffer.position(startPosition);
			readBuffer.limit(bufferPosition);

			while (readBuffer.limit() - readBuffer.position() > 4) {
				final int length = TestEvent.getIntFromArray(readBytes,
						readBuffer.position() + TestEvent.Offsets.length);
				assertThat(length, Matchers.greaterThan(16));
				if (startPosition + length <= readBuffer.limit()) {
					// got a full message
					final int type = TestEvent.getIntFromArray(readBytes,
							readBuffer.position() + TestEvent.Offsets.type);
					switch (type) {
					case TestEvent.MessageType.clientRequestMessage:

						messageReadCount++;
						final long sendTime = TestEvent.getLongFromArray(readBytes,
								readBuffer.position() + TestEvent.Offsets.time);
						final long nowNanoTime = System.nanoTime();
						propHisto.addObservation(nowNanoTime - sendTime);
						// occasional logging
						/*
						 * if ((messageReadCount & ((256 * 1024) - 1)) == 0) {
						 * logger.info("id:{} Prop Histo:{} {}", id, propHisto.getCount(),
						 * propHisto.toString()); propHisto.clear();
						 * 
						 * logger.debug(
						 * "Took to recv:{} len:{} posn:{} lim:{} readCount:{} bytesRead:{} bytesReadCount:{}"
						 * , (nowNanoTime - sendTime), length, startPosition, readBuffer.limit(),
						 * readCount, bytesRead, totalRead);
						 * 
						 * }
						 */
						// return some data
						if (serverCallin.isWriteBlocked() >= 0) {
							if (serverCallin.bufferRemaining() < length) {
								serverCallin.flush();
								writeCount++;
								if (serverCallin.bufferRemaining() < length) {
									logger.info("blocked write - some data remaining callin:{}", serverCallin);
									serverCallin.blockRead();
									readBuffer.compact();
									return;
								}
							}
						}
						TestEvent.putIntToArray(readBytes, startPosition + TestEvent.Offsets.type,
								TestEvent.MessageType.serverResponseMessage);

						TestEvent.putLongToArray(readBytes, startPosition + TestEvent.Offsets.responsTime,
								System.nanoTime());
						TestEvent.putLongToArray(readBytes, startPosition + TestEvent.Offsets.respSeqNum, respSeqNum++);
						if (serverCallin.sendMessage(readBytes, startPosition, length) >= 0) {
							writeSignalCount++;
						} else {
							// didnt send.
						}
						break;
					case TestEvent.MessageType.dataFromServerEvent:
						close();
						Assert.fail("unexpected dataEvent");
					case TestEvent.MessageType.serverResponseMessage:
						close();
						Assert.fail("unexpected serverMessage");
					default:
						close();
						Assert.fail("Error unknown message type:" + type + " len:" + length);
					}
					startPosition += length;
					if (startPosition == readBuffer.limit()) {
						readBuffer.clear();
						return;
					} else {
						assertThat(startPosition, Matchers.lessThan(readBuffer.limit()));
						readBuffer.position(startPosition);
					}
				} else {
					logger.info("COMPACT posn:{} Lim:{}", readBuffer.position(), readBuffer.limit());
					readBuffer.compact();
					logger.info("COMPACT posn:{} Lim:{}", readBuffer.position(), readBuffer.limit());
					return;
				}

			}
			logger.info("out of loop");
			logger.error("Error not enough space {} {} {} {}", readBuffer.position(), readBuffer.limit(),
					readData.position(), readData.limit());
		}
	}

}