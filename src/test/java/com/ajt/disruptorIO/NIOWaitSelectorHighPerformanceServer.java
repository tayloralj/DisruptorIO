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

import static org.junit.Assert.assertThat;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.hamcrest.core.Is;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.collections.Histogram;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

public class NIOWaitSelectorHighPerformanceServer {
	static {
		System.setProperty("org.apache.logging.log4j.simplelog.StatusLogger.level", "TRACE");
		System.setProperty("org.apache.logging.log4j.level", "DEBUG");

	}
	private final Logger logger = LoggerFactory.getLogger(NIOWaitSelectorHighPerformanceServer.class);
	private NIOWaitStrategy.NIOClock clock;
	private ExceptionHandler<TestEvent> errorHandler;
	private long sequenceNum = 0;
	private Disruptor<TestEvent> disruptor;
	private LatencyTimer lt;
	private ThreadFactory threadFactory;

	@Before
	public void setup() {
		clock = NIOWaitStrategy.getDefaultClock();
		threadFactory = new ThreadFactory() {

			@Override
			public Thread newThread(Runnable r) {
				final Thread th = new Thread(r, "WaStratThread");

				return th;
			}
		};
		errorHandler = new ExceptionHandler<NIOWaitSelectorHighPerformanceServer.TestEvent>() {

			@Override
			public void handleOnStartException(Throwable ex) {
				logger.error("handleOnStartException", ex);
			}

			@Override
			public void handleOnShutdownException(Throwable ex) {
				logger.error("handleOnShutdownException", ex);
			}

			@Override
			public void handleEventException(Throwable ex, long sequence, TestEvent event) {
				logger.error("handleEventException event:{} seqnum:{} last:{}", event, sequence, sequenceNum, ex);

			}
		};
		nioWaitStrategy = new NIOWaitStrategy(clock);
		int ringBufferSize = 2048;
		disruptor = new Disruptor<>(TestEvent.EVENT_FACTORY, ringBufferSize, threadFactory, ProducerType.SINGLE,
				nioWaitStrategy);
		disruptor.setDefaultExceptionHandler(errorHandler);

		lt = new LatencyTimer();
	}

	@After
	public void teardown() {
		disruptor.shutdown();
		try {
			nioWaitStrategy.close();
		} catch (Exception e) {
			logger.info("Error closing nioWait", e);
		}
		handlers = null;
		threadFactory = null;
	}

	private NIOWaitStrategy nioWaitStrategy;

	private TestEventServer[] handlers;

	@Test
	public void testServerConnection() throws Exception {
		final long toSend = 100_000L;
		final long messageratePerSecond = 10_000L;
		final long readRatePerSecond = 1000_000_000L;
		final long writeRatePerSecond = 1000_000L;
		handlers = new TestEventServer[] { new TestEventServer(nioWaitStrategy, false) };
		disruptor.handleEventsWith(handlers);
		testFastServer(toSend, messageratePerSecond, readRatePerSecond, writeRatePerSecond, 4, false);
	}

	private TestEventClient[] tc = new TestEventClient[128];

	// runs on the main thread
	private void testFastServer(final long toSend, final long messageratePerSecond, final long readRatePerSecond,
			final long writeRatePerSecond, int clients, boolean lossy) throws Exception {

		try (final ServerSocketChannel socketChannel = ServerSocketChannel.open()) {
			logger.info("Disruptor creating new disruptor for this context. toSend:{} rateAt:{}", toSend,
					messageratePerSecond);
			byte[] dataToSendToClient = "ddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
					.getBytes();

			logger.debug(
					"Starting AsyncLogger disruptor for this context with ringbufferSize={}, waitStrategy={}, "
							+ "exceptionHandler={}...",
					disruptor.getRingBuffer().getBufferSize(), nioWaitStrategy.getClass().getSimpleName(),
					errorHandler);
			nioWaitStrategy.getScheduledExecutor().execute(() -> {
				lt.register(nioWaitStrategy);

			});

			disruptor.start();

			final RingBuffer<TestEvent> rb = disruptor.getRingBuffer();
			socketChannel.configureBlocking(false);
			socketChannel.bind(null, 0);

			// pass to the disruptor thread.
			nioWaitStrategy.getScheduledExecutor().execute(() -> {
				try {
					handlers[0].newServer(socketChannel);
				} catch (Exception e) {
					logger.error("Error starting server", e);
				}
			});

			tc = new TestEventClient[clients];
			for (int b = 0; b < clients; b++) {
				tc[b] = new TestEventClient(b, writeRatePerSecond, readRatePerSecond, socketChannel.getLocalAddress());
			}
			Thread.sleep(10);
			long a = 0;
			final long startTimeNanos = System.nanoTime();
			int b = 0;
			while (a < toSend) {
				final long currentTimeNanos = System.nanoTime();
				final long timeShouldBeAtLeast = (a * 1000000000) / messageratePerSecond + startTimeNanos;
				if (currentTimeNanos > timeShouldBeAtLeast) {

					try {
						if (lossy) {
							sequenceNum = rb.tryNext(1);
						} else {
							sequenceNum = rb.next(1);
						}

						final TestEvent te = rb.get(sequenceNum);
						te.type = TestEvent.EventType.data;
						te.targetID = b;
						te.data = dataToSendToClient;
						te.nanoSendTime = System.nanoTime();
						rb.publish(sequenceNum);
						a++;
					} catch (InsufficientCapacityException ice) {
						// land here if a lossy client
					} finally {
						// move onto next client
						if (++b >= clients) {
							b = 0;
						}
					}
				} else {
					Thread.sleep(1);
				}

				if ((a & 131071) == 0) {
					logger.debug("total sent:{} sendRate:{} runTimeMS:{} ", a,
							a * 1000000000 / (currentTimeNanos - startTimeNanos),
							(currentTimeNanos - startTimeNanos) / 1000000L);
					for (int c = 0; c < clients; c++) {
						logger.debug(
								"{}:   runningFor(ms):{} MsgCount:{}  ReadBytes:{} readByteRate:{} writeAtLeast:{} writtenBytes:{} ",
								c, // c
								(currentTimeNanos - startTimeNanos) / 1000000, // runfor
								tc[c].recvMsgCount.get(), // count
								tc[c].bytesRead.get(), // bytes
								tc[c].bytesRead.get() * 1000000000L / (currentTimeNanos - startTimeNanos), // rate
								tc[c].writeShouldBeAtLeast.get(), // write at lease
								tc[c].bytesWritten.get()); // written
					}
				}

			}
			logger.info("Finished sending");
			final long endTime = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(10);
			while (System.nanoTime() < endTime) {
				if (handlers[0].counter.get() == toSend) {
					logger.info("completed :{}", toSend);
					break;
				}
			}
			lt.stop();
			assertThat(handlers[0].counter.get(), Is.is(toSend));
			long end = System.nanoTime();
			for (int c = 0; c < clients; c++) {
				logger.info("Complete Took(ms):{} DataSend:{} rate:{} MBwritten:{} rate:{}",
						TimeUnit.NANOSECONDS.toMillis(end - startTimeNanos), handlers[0].counter.get(),
						handlers[0].counter.get() * 1000 / (end - startTimeNanos), //
						tc[c].bytesWritten.get() / 1000000, //
						tc[c].bytesWritten.get() / (1000 * end - 1000 * startTimeNanos));
				logger.debug(
						"Complete client:{}:  sentMsg:{}  recvMsgCount:{} readAtLeastByte:{} bytesRead:{} writeAtLeast:{} bytesWritten:{} ", //
						c, a, tc[c].recvMsgCount.get(), tc[c].readShouldBeAtLeast.get(), tc[c].bytesRead.get(),
						tc[c].writeShouldBeAtLeast.get(), tc[c].bytesWritten.get());
				tc[c].close();
			}
			// pass to the disruptor thread.
			nioWaitStrategy.getScheduledExecutor().execute(() -> {
				try {
					handlers[0].close();
				} catch (Exception e) {
					logger.error("Error starting server", e);
				}

			});
			Thread.sleep(10);
		} finally {
		}
	}

	/** simple test client. socket listener per client */
	class TestEventClient implements Closeable {
		// atomic as updated/accessed from read/write seperate threads
		final AtomicBoolean isRunning = new AtomicBoolean(true);
		final AtomicLong readShouldBeAtLeast = new AtomicLong(0);
		final AtomicLong writeShouldBeAtLeast = new AtomicLong(0);
		final AtomicLong bytesRead = new AtomicLong(0);
		final AtomicLong bytesWritten = new AtomicLong(0);
		final AtomicLong recvMsgCount = new AtomicLong(0);

		final Socket s;
		// thread for each as uses blocking IO
		final Thread readThread;
		final Thread writeThread;
		final int id;

		TestEventClient(int count, final long writeRatePerSecond, final long readRatePerSecond, SocketAddress sa)
				throws Exception {
			id = count;

			s = new Socket();
			// pick a random address

			// call method on correct thread.

			logger.info("local connection to :{}", sa);
			s.connect(sa, 1000);
			s.setTcpNoDelay(false);

			final InputStream is = s.getInputStream();
			final OutputStream os = s.getOutputStream();
			final long startTimeNanos = System.nanoTime();
			// blocking - forces clients to keep running.
			readThread = new Thread(() -> {
				try {
					final byte[] readArray = new byte[1024];
					while (isRunning.get()) {
						final long currentTimeNanos = System.nanoTime();
						readShouldBeAtLeast
								.set(readRatePerSecond * ((currentTimeNanos - startTimeNanos) / 1000_000_000L));
						if (bytesRead.get() < readShouldBeAtLeast.get()) {

							final int bytesReadCycle = is.read(readArray);
							if (bytesReadCycle == -1) {
								logger.error("Socket closed - exit");
								break;
							}
							bytesRead.addAndGet(bytesReadCycle);
						} else {
							Thread.sleep(1);
						}
					}
				} catch (SocketException se) {
					logger.info("SocketClosed");
				} catch (Exception e) {
					logger.error("Error in read:", e);
				}
				try {
					is.close();
				} catch (Exception e) {
					logger.error("erro during is close", e);
				}

			});
			readThread.setName("testClientReadThread-" + count);
			readThread.start();
			writeThread = new Thread(() -> {
				try {
					byte[] writeBytes = "Hello worldHello worldHello worldHello worldHello worldHello worldHelloworldHello worldHello worldHello world"
							.getBytes();
					while (isRunning.get()) {
						final long currentTimeNanos = System.nanoTime();
						writeShouldBeAtLeast
								.set(writeRatePerSecond * ((currentTimeNanos - startTimeNanos) / 1_000_000_000L));
						if (bytesWritten.get() < writeShouldBeAtLeast.get()) {

							os.write(writeBytes);
							bytesWritten.addAndGet(writeBytes.length);
						} else {
							Thread.sleep(1);
						}
					}
				} catch (SocketException se) {
					logger.info("SocketClosed");
				} catch (Exception e) {
					logger.error("Error in read:", e);
				}
				try {
					os.close();
				} catch (Exception e) {
					logger.error("erro during os close", e);
				}

			});
			writeThread.setName("testClientWriteThread-" + count);

			writeThread.start();

		}

		public void close() {
			isRunning.set(false);
			readThread.interrupt();
			writeThread.interrupt();
			try {
				s.close();
			} catch (Exception e) {
				logger.info("Error closing socket", e);
			}
		}
	}

	public class TestEventServer implements EventHandler<TestEvent>, Closeable {
		private final Logger logger = LoggerFactory.getLogger(TestEventServer.class);
		private final AtomicLong counter = new AtomicLong();
		private final NIOWaitStrategy waitStrat;

		private final boolean coalsce;
		private final Histogram elapsedHisto = new Histogram(new long[] { 100, 200, 400, 800, 1600, 3200, 6400, 12800,
				25600, 51200, 102400, 204800, 409600, 819200, 1638400, 3276800L, 6553600L, 13107200L, 13107200L * 2L,
				13107200L * 4L, 13107200L * 8L, 13107200L * 16L, 13107200L * 10000L });
		private final Histogram delayHisto = new Histogram(new long[] { 100, 200, 400, 800, 1600, 3200, 6400, 12800,
				25600, 51200, 102400, 204800, 409600, 819200, 1638400, 3276800L, 6553600L, 13107200L, 13107200L * 2L,
				13107200L * 4L, 13107200L * 8L, 13107200L * 16L, 13107200L * 10000L });

		private NIOServerAcceptor callback;

		public TestEventServer(NIOWaitStrategy waiter, boolean compact) {
			coalsce = compact;
			waitStrat = waiter;
		}

		@Override
		public void onEvent(final TestEvent event, final long sequence, final boolean endOfBatch) throws Exception {
			final long now = System.nanoTime();
			switch (event.type) {
			case data:
				handleData(event, sequence, endOfBatch);
				break;

			default:
				logger.error("Unknown sNum:{} type:{} seqNum:{} eob:{}", event.seqNum, event.type, sequence,
						endOfBatch);
			}
			counter.incrementAndGet();
			event.type = TestEvent.EventType.UNKNOWN;
			final long end = System.nanoTime();
			delayHisto.addObservation(now - event.nanoSendTime);
			elapsedHisto.addObservation(end - now);
		}

		void handleData(final TestEvent event, final long sequence, final boolean endOfBatch) throws Exception {
			tc[event.targetID].recvMsgCount.incrementAndGet();
			if (callback.ecc[event.targetID] == null) {
				logger.error("Erro no connection with targetID:{})", event.targetID);
				return;
			}
			callback.ecc[event.targetID].handleData(event, sequence, endOfBatch);
			// flush anything without a batch
			if (endOfBatch) {
				for (int a = 0; a < callback.ecc.length; a++) {
					if (callback.ecc[a] != null && callback.ecc[a].isDirty) {
						callback.ecc[a].flush();
					}
				}
			}
		}

		public void close() throws IOException {
			logger.info("CLOSESERVER");
			logger.info("ElapsedHisto:" + elapsedHisto.getCount() + " " + elapsedHisto.toString());
			logger.info("DelayHisto:" + delayHisto.getCount() + " " + delayHisto.toString());

			for (int a = 0; a < callback.ecc.length; a++) {
				if (callback.ecc[a] != null) {
					logger.info("closeServer id:{} ", a);
					callback.ecc[a].close();
				}
			}
		}

		void newServer(final SelectableChannel channel) throws Exception {
			callback = new NIOServerAcceptor();
			callback.key = waitStrat.registerSelectableChannel(channel, callback);
			callback.key.interestOps(SelectionKey.OP_ACCEPT);
			logger.info("Registered for opAccept " + callback.key.interestOps() + " chnnal:" + channel + " reg:"
					+ channel.isRegistered() + " blocking:" + channel.isBlocking() + "  open:" + channel.isOpen());

		}

		/**
		 * callback to accept a new connection on the server socket
		 * 
		 * @author ajt
		 *
		 */
		class NIOServerAcceptor implements NIOWaitStrategy.SelectorCallback {
			private EstablishedServerConnectionCallback[] ecc = new EstablishedServerConnectionCallback[128];

			SelectionKey key;

			NIOServerAcceptor() {

			}

			@Override
			public void opRead(final SelectableChannel channel, final long currentTimeNanos) {
				logger.info("OPRead");

			}

			@Override
			public void opAccept(final SelectableChannel channel, final long currentTimeNanos) {
				logger.info("opAccept");
				ServerSocketChannel ssc = (ServerSocketChannel) channel;
				try {
					final SocketChannel socketChannel = ssc.accept();
					final boolean finishConnect = socketChannel.finishConnect();
					socketChannel.configureBlocking(false);

					int connectionID = 0;
					for (connectionID = 0; connectionID < ecc.length; connectionID++) {
						if (ecc[connectionID] == null)
							break;
					}
					if (connectionID == ecc.length) {
						logger.error("ERROR no remaining slots for connections all in use");
						channel.close();
						return;
					}
					ecc[connectionID] = new EstablishedServerConnectionCallback(socketChannel, connectionID);
					ecc[connectionID].key = waitStrat.registerSelectableChannel(socketChannel, ecc[connectionID]);
					ecc[connectionID].key.interestOps(SelectionKey.OP_READ);
					connectionID++;
					logger.info("opAcccept completed:{} registered:{} finishConnect:{}", socketChannel,
							socketChannel.isRegistered(), finishConnect);
				} catch (Exception e) {
					logger.info("Error failed handshake", e);
				}

			}

			@Override
			public void opConnect(final SelectableChannel channel, final long currentTimeNanos) {
				logger.info("opConnect");

			}

			@Override
			public void opWrite(final SelectableChannel channel, final long currentTimeNanos) {
				logger.info("NIOAcceptorCallback- opWrite");

			}

			/**
			 * callback to interact with an established client session batches data into
			 * upto 4kb chunks before flushing to socket at the end of the batch or when the
			 * buffer is full.
			 * 
			 * @author ajt
			 *
			 */
			class EstablishedServerConnectionCallback implements NIOWaitStrategy.SelectorCallback, Closeable {
				int maxQueueSize = 65536;
				final LinkedList<byte[]> queue = new LinkedList<>();
				boolean currentlyBlocked = false;
				boolean closed = false;
				boolean isDirty = false;
				private final SocketChannel socketChannel;
				private final int id;
				private SelectionKey key;
				private long totalRead = 0;
				private long readCount = 0;
				private long readSignalCount = 0;
				private long totalWriteSocket = 0;
				private long writeSignalCount = 0;
				private long writeCount = 0;
				private long dataLossCount = 0;
				private long endOfBatchCount = 0;

				private final ByteBuffer readBuffer;
				private final ByteBuffer writeBuffer;
				long lastLogErrorNano = System.nanoTime();

				EstablishedServerConnectionCallback(final SocketChannel sc, int id) {
					this.socketChannel = sc;
					this.id = id;
					readBuffer = ByteBuffer.allocate(4096);
					readBuffer.order(ByteOrder.LITTLE_ENDIAN);
					writeBuffer = ByteBuffer.allocate(1024 * 1024);
					writeBuffer.order(ByteOrder.LITTLE_ENDIAN);
				}

				public void close() {
					logger.info(
							"id:{} Closing socket:{} totalRead:{} readSignalCount:{} readiteration:{} totalWrite:{} writeSignalCount:{} writeCount:{}", //
							id, socketChannel, totalRead, readSignalCount, readCount, totalWriteSocket,
							writeSignalCount, writeCount);
					logger.info("EndOfBatch:{} dataLoss:{}", endOfBatchCount, dataLossCount);
					if (closed) {
						logger.warn("Already closed:{}", id);
					}

					try {
						socketChannel.close();
					} catch (Exception e) {
						logger.info("Error closing connnection " + socketChannel + " key:" + key, e);
					}
					if (key.isValid()) {
						key.cancel();
					}
					ecc[id] = null;
					closed = true;

				}

				/**
				 * return true if all good. and writebuffer will be clear; false if partial
				 * write. now will be registered writeable to complete.
				 * 
				 * @return
				 */
				boolean flush() throws IOException {
					if (currentlyBlocked) {
						return false;
					}
					writeBuffer.flip();
					final int remaining = writeBuffer.remaining();
					final int bytesWritten = socketChannel.write(writeBuffer);
					if (bytesWritten == -1) {
						close();
						return false;
					} else {
						if (bytesWritten < remaining) {
							// didnt write everything
							logger.info(
									"Didnt write all - blocking:{} remaining:{} bytesWritten:{} totalWriteSocket:{}",
									id, remaining, bytesWritten, totalWriteSocket);
							currentlyBlocked = true;
							key.interestOps(SelectionKey.OP_READ + SelectionKey.OP_WRITE);
							writeBuffer.compact();
							writeBuffer.flip();
							return false;
						}
						writeCount++;
						totalWriteSocket += bytesWritten;
						writeBuffer.clear();
						isDirty = false;
						return true;
					}
				}

				public void handleData(final TestEvent event, final long sequence, final boolean endOfBatch)
						throws Exception {

					if (currentlyBlocked) {
						if (coalsce) {
							// drop data
							logger.trace("Coalse: drop data size:{}", event.data.length);
						} else {
							if (queue.size() > maxQueueSize) {
								dataLossCount++;
								if (System.nanoTime() - lastLogErrorNano > TimeUnit.MILLISECONDS.toNanos(100)) {
									logger.error("Data lost: backlog exeeded count:{}", dataLossCount);
									lastLogErrorNano = System.nanoTime();
								}
							} else {
								final byte[] dataCopy = new byte[event.data.length];
								System.arraycopy(event.data, 0, dataCopy, 0, event.data.length);
								queue.add(dataCopy);
							}
						}
					} else {
						if (endOfBatch) {
							// change to only flush on end of batch.
							endOfBatchCount++;
						}
						if (writeBuffer.remaining() < event.data.length) {
							if (flush()) {

							} else {
								return;
							}
						}
						writeBuffer.put(event.data);
						isDirty = true;

					}

				}

				@Override
				public void opRead(final SelectableChannel channel, final long currentTimeNanos) {
					try {
						readSignalCount++;
						int read = 0;
						do {
							readBuffer.clear();
							read = socketChannel.read(readBuffer);
							// logger.info("Read:" + read);
							if (read == -1) {
								close();
								return;
							} else {
								totalRead += read;
								readCount++;
							}
						} while (read > 0);
					} catch (Exception e) {
						close();
					}
				}

				@Override
				public void opWrite(final SelectableChannel channel, final long currentTimeNanos) {
					long start = System.nanoTime();
					long thisWrite = 0;
					try {
						writeSignalCount++;
						int bytesRemaining = writeBuffer.remaining();
						int bytesWritten = socketChannel.write(writeBuffer);
						logger.debug(
								" EstablishedConnectionCallback - OPWrite called  - sup size:{} wc:{} sdc:{} writ:{}",
								queue.size(), writeCount, writeSignalCount, bytesWritten);

						writeCount++;
						if (bytesWritten == -1) {
							// closed.
							logger.info("closing server in opWrite -1 ret");
							close();
						} else {
							totalWriteSocket += bytesWritten;
							if (bytesRemaining == bytesWritten) // wrote everything. lets get some more{
							{
								writeBuffer.clear();
								while (queue.size() > 0) {
									// pack multiple messages into one write.
									while (queue.size() > 0 && queue.peek().length < writeBuffer.remaining()) {
										final byte[] data = queue.poll();
										writeBuffer.put(data);
									}

									writeBuffer.flip();

									bytesRemaining = writeBuffer.remaining();
									bytesWritten = socketChannel.write(writeBuffer);
									writeCount++;
									if (bytesWritten == -1) {
										// closed.
										logger.info("closing server in opWrite flush -1 ret");
										close();
									} else {
										thisWrite += bytesWritten;
										totalWriteSocket += bytesWritten;
										if (bytesWritten < bytesRemaining) {
											writeBuffer.compact();
											logger.debug(
													"{} baglog incomplete - socket not caught up size:{} writeCount:{} writeSignalCOunt:{} writtenBytes:{} totalWriteSocket:{} thisWrte:{} took:(us):{}", //
													id, queue.size(), writeCount, writeSignalCount, bytesWritten,
													totalWriteSocket, thisWrite, (System.nanoTime() - start) / 1000L);
											return;
										}
									}
								}
								writeBuffer.clear();

								logger.debug("Write to socket - caught up - completed");
								// back to read - finished backlog
								key.interestOps(SelectionKey.OP_READ);
								currentlyBlocked = false;

							} else {
								// still some data left
								writeBuffer.compact();
								writeBuffer.flip();
							}
						}

					} catch (

					Exception e) {
						logger.error("Error in opWrite", e);
					}
				}

				@Override
				public void opAccept(SelectableChannel channel, long currentTimeNanos) {
					throw new RuntimeException("ERROR acc");

				}

				@Override
				public void opConnect(SelectableChannel channel, long currentTimeNanos) {
					throw new RuntimeException("ERROR connect");

				}
			}
		}

	}

	private static final class TestEvent {
		enum EventType {
			data, newServer, close, UNKNOWN;
		}

		private EventType type = EventType.UNKNOWN;
		private byte[] data = null;
		private int targetID = 0;
		private final long seqNum;
		private long nanoSendTime = 0;
		private static long counter = 0;

		private TestEvent() {
			seqNum = counter++;
		}

		public static final EventFactory<TestEvent> EVENT_FACTORY = new EventFactory<TestEvent>() {
			@Override
			public TestEvent newInstance() {
				return new TestEvent();
			}
		};

	}

}