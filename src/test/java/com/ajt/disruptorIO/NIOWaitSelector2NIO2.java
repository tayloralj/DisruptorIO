package com.ajt.disruptorIO;

import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.LinkedList;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ajt.disruptorIO.NIOWaitStrategy.TimerCallback;
import com.ajt.disruptorIO.NIOWaitStrategy.TimerHandler;
import com.ajt.disruptorIO.SenderHelper.SenderCallin;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.collections.Histogram;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

public class NIOWaitSelector2NIO2 {
	static {
		System.setProperty("org.apache.logging.log4j.simplelog.StatusLogger.level", "TRACE");
		System.setProperty("org.apache.logging.log4j.level", "DEBUG");

	}
	private final Logger logger = LoggerFactory.getLogger(NIOWaitSelector2NIO2.class);
	private ExceptionHandler<TestEvent> errorHandler;
	private long sequenceNum = 0;
	private Disruptor<TestEvent> disruptorServer;
	private Disruptor<TestEvent> disruptorClient;
	private NIOWaitStrategy nioWaitStrategyClient;
	private NIOWaitStrategy nioWaitStrategyServer;

	private TestEventServer[] handlers;
	private ThreadFactory threadFactoryServer;
	private ThreadFactory threadFactoryClient;

	static Histogram getHisto() {
		return new Histogram(new long[] { 100, 200, 400, 1000, 4000, 8000, 20000, 50000, 100000, 200000, 500000,
				2000000, 5000000, 5000000 * 4, 5000000 * 10, 5000000 * 20, 5000000 * 50 });
	}

	@Before
	public void setup() {

		threadFactoryServer = new ThreadFactory() {
			int count = 0;

			@Override
			public Thread newThread(Runnable r) {
				final Thread th = new Thread(r, "WAServer-" + (count++));

				return th;
			}
		};
		threadFactoryClient = new ThreadFactory() {
			int count = 0;

			@Override
			public Thread newThread(Runnable r) {
				final Thread th = new Thread(r, "WAClient-" + (count++));

				return th;
			}
		};
		errorHandler = new ExceptionHandler<TestEvent>() {

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
		nioWaitStrategyClient = new NIOWaitStrategy(NIOWaitStrategy.getDefaultClock(), false);
		nioWaitStrategyServer = new NIOWaitStrategy(NIOWaitStrategy.getDefaultClock(), false);
		int ringBufferSize = 2048;
		disruptorServer = new Disruptor<>(TestEvent.EVENT_FACTORY, ringBufferSize, threadFactoryServer,
				ProducerType.SINGLE, nioWaitStrategyServer);
		disruptorServer.setDefaultExceptionHandler(errorHandler);

		disruptorClient = new Disruptor<>(TestEvent.EVENT_FACTORY, ringBufferSize, threadFactoryClient,
				ProducerType.SINGLE, nioWaitStrategyClient);
		disruptorClient.setDefaultExceptionHandler(errorHandler);

	}

	@After
	public void teardown() {
		logger.info("Teardown");
		disruptorServer.shutdown();
		try {
			nioWaitStrategyServer.close();
		} catch (Exception e) {
			logger.info("Error closing nioWait", e);
		}
		for (int a = 0; a < handlers.length; a++) {
			handlers[a].close();
			handlers[a] = null;
		}
		handlers = null;

		threadFactoryServer = null;

		disruptorClient.shutdown();
		try {
			nioWaitStrategyClient.close();
		} catch (Exception e) {
			logger.info("Error closing nioWait", e);
		}
		threadFactoryClient = null;

	}

	@Test
	public void testServerConnection() throws Exception {
		final long toSend = 1000_000_000L;
		final long messageratePerSecond = 1_00_000L;
		final long readRatePerSecond = 1_000_000L;
		final long writeRatePerSecond = 1_000_000L;
		final int clientCount = 2;
		handlers = new TestEventServer[] { new TestEventServer(nioWaitStrategyServer, false, null) };
		disruptorServer.handleEventsWith(handlers);
		testFastServer(toSend, messageratePerSecond, readRatePerSecond, writeRatePerSecond, clientCount, false);
	}

	// runs on the main thread
	private void testFastServer(final long toSend, final long messageratePerSecond, final long readRatePerSecond,
			final long writeRatePerSecond, int clients, boolean lossy) throws Exception {
		try {
			logger.info("Disruptor creating new disruptor for this context. toSend:{} rateAt:{}", toSend,
					messageratePerSecond);

			disruptorServer.start();

			final long maxWaitForBind = System.currentTimeMillis() + 10000;
			while (System.currentTimeMillis() < maxWaitForBind && handlers[0].remoteAddress == null) {
				Thread.sleep(1);
			}
			if (handlers[0].remoteAddress == null) {
				Assert.fail("Error remote address not set for connect to");
			}

			// create a client set using the client disruptor
			final TestEventClient tc = new TestEventClient(clients, writeRatePerSecond, readRatePerSecond,
					(InetSocketAddress) handlers[0].remoteAddress, nioWaitStrategyClient);
			disruptorClient.handleEventsWith(new TestEventClient[] { tc });
			disruptorClient.start();

			// pass to the disruptor thread - start command on correct thread callback
			nioWaitStrategyClient.getScheduledExecutor().execute(() -> {
				tc.start();
			});

			// wait to connect before sending
			Thread.sleep(50);
			final byte[] byteDataToSend = "MyMessageToTheClientMyMessageToTheClientMyMessageToTheClientMyMessageToTheClientMyMessageToTheClientMyMessageToTheClientMyMessageToTheClientMyMessageToTheClientMyMessageToTheClient"
					.getBytes();
			long actualMessageSendCount = 0;
			final long startTimeNanos = System.nanoTime();
			int b = 0;
			final RingBuffer<TestEvent> rb = disruptorServer.getRingBuffer();
			while (actualMessageSendCount < toSend) {
				final long currentTimeNanos = System.nanoTime();
				final long elapsed = currentTimeNanos - startTimeNanos;
				// control send rate
				if (elapsed * messageratePerSecond > actualMessageSendCount * 1000000000L) {

					try {
						if (lossy) {
							sequenceNum = rb.tryNext(1);
						} else {
							sequenceNum = rb.next(1);
						}

						final TestEvent te = rb.get(sequenceNum);
						te.type = TestEvent.EventType.data;
						te.targetID = b;
						TestEvent.putLongToArray(byteDataToSend, TestEvent.Offsets.time, System.nanoTime());
						TestEvent.putLongToArray(byteDataToSend, TestEvent.Offsets.seqnum, sequenceNum);

						te.setData(byteDataToSend, 0, byteDataToSend.length, TestEvent.MessageType.dataEvent);
						te.nanoSendTime = System.nanoTime();
						rb.publish(sequenceNum);
						actualMessageSendCount++;
					} catch (InsufficientCapacityException ice) {
						// land here if a lossy client
					} finally {
						// move onto next client
						if (++b >= clients) {
							b = 0;
						}
					}
				} else {
					LockSupport.parkNanos(2000);
				}

				if ((actualMessageSendCount & (1024 * 1024 - 1)) == 0) {
					logger.debug("Intermediate total sent:{} sendRate:{} runTimeMS:{} ", actualMessageSendCount,
							actualMessageSendCount * 1000000000 / (currentTimeNanos - startTimeNanos),
							(currentTimeNanos - startTimeNanos) / 1000000L);
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
			assertThat(handlers[0].counter.get(), Matchers.is(toSend));
			nioWaitStrategyClient.getScheduledExecutor().execute(() -> {
				tc.close();
			});
			// pass to the disruptor thread.
			nioWaitStrategyServer.getScheduledExecutor().execute(() -> {
				logger.info("Closing server");
				handlers[0].close();
			});
			Thread.sleep(50);
		} finally {

		}

	}

	public static class TestEventClient implements EventHandler<TestEvent> {
		private final Logger logger = LoggerFactory.getLogger(TestEventClient.class);
		final int count;
		final long writeRatePerSecond;
		final long readRatePerSecond;
		final InetSocketAddress sa;
		final NIOClientConnection[] clients;
		final SenderHelper helper;

		boolean isRunning = false;
		long _startTimeNanos = 0;
		long _currentTimeNanos = 0;

		public TestEventClient(final int count, //
				final long writeRatePerSecond, //
				final long readRatePerSecond, //
				final InetSocketAddress sa, //
				final NIOWaitStrategy nioWait) {
			this.sa = sa;
			this.count = count;
			this.writeRatePerSecond = writeRatePerSecond;
			this.readRatePerSecond = readRatePerSecond;
			helper = new SenderHelper(nioWait);
			clients = new NIOClientConnection[count];
			for (int a = 0; a < count; a++) {
				clients[a] = new NIOClientConnection(a, sa, nioWait);
			}
		}

		@Override
		public void onEvent(TestEvent event, long sequence, boolean endOfBatch) throws Exception {
			logger.warn("Not exepecting callback");
		}

		void start() {
			try {
				logger.info("Start");
				for (int a = 0; a < count; a++) {

					helper.connectTo(sa, clients[a]);

				}
			} catch (Exception e) {
				logger.error("Error during start", e);
				Assert.fail("fail during start;" + e);
			}
		}

		/** print out some stats */
		public void close() {
			isRunning = false;
			for (int c = 0; c < clients.length; c++) {
				if (clients[c] != null) {

					logger.debug(
							"{}:  Client  runningFor(ms):{} isRunning:{} MsgCount:{} ReadMBYTE:{} readByteRate(mbit):{} readCycle:{} readCount:{} writeAtLeast:{} writtenBytes:{} ",
							c, // c
							(_currentTimeNanos - _startTimeNanos) / 1000000, // runfor
							clients[c].isRunning, //
							clients[c].recvMsgCount, // count
							clients[c].bytesRead / 1024L / 1024L, // bytes
							clients[c].bytesRead * (1000000000L * 8L / 1024L / 1024L)
									/ (_currentTimeNanos - _startTimeNanos), // rate
							clients[c].bytesReadCallback, //
							clients[c].bytesReadCount, //
							clients[c].writeShouldBeAtLeast, // write
							clients[c].bytesWritten); // written
					clients[c].close();
					clients[c] = null;
				}
			}

		}

		class NIOClientConnection implements SenderHelper.SenderCallback {
			boolean isRunning = true;
			long readShouldBeAtLeast = 0;
			long writeShouldBeAtLeast = 0;
			long bytesRead = 0;
			long bytesReadCallback = 0;
			long messageReadCount = 0;
			long bytesReadCount = 0;
			long bytesWritten = 0;
			long recvMsgCount = 0;

			final byte[] readBytes;
			final byte[] writeBytes;
			long messageCounter = 0;
			final ByteBuffer writeBuffer;
			final ByteBuffer readBuffer;
			final TimerHandler timerHandler;
			final TimerSenderCallback callback;
			long startTimeNano = 0;
			boolean blocked = false;
			long slowTimer = 100 * 1000000;
			long fastTimer = 500;
			final int id;
			final Histogram propHisto = getHisto();
			final Histogram rttHisto = getHisto();
			SenderCallin callin;
			final SocketAddress sa;

			public NIOClientConnection(int id, SocketAddress sa, NIOWaitStrategy nioWait) {
				this.id = id;
				this.sa = sa;
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

			@Override
			public void connected(final SenderCallin callin) {

				timerHandler.fireIn(0);
				startTimeNano = timerHandler.currentNanoTime();
				this.callin = callin;
				logger.info("opConnect Complete :" + callin.getLocalAddress() + " " + callin.getRemoteAddress() + " ");

			}

			@Override
			public void blocked(final SenderCallin callin) {
				logger.info("blocked write callin:{}", callin);
				blocked = true;
			}

			@Override
			public void unblocked(final SenderCallin callin) {
				logger.info("unblocked write callin:{}", callin);
				blocked = false;
			}

			@Override
			public void readData(final SenderCallin callin, final ByteBuffer buffer) {
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
					final int length = TestEvent.getIntFromArray(readBytes,
							readBuffer.position() + TestEvent.Offsets.length);
					assertThat(" posn:" + readBuffer.position() + " lim:" + readBuffer.limit(), //
							length, Matchers.greaterThan(16));
					//
					if (startPosition + length <= readBuffer.limit()) {
						// got a full message
						final int type = TestEvent.getIntFromArray(readBytes, readBuffer.position() + 4);
						switch (type) {
						case TestEvent.MessageType.clientMessage:
							logger.debug("unknown type");
							break;
						case TestEvent.MessageType.dataEvent:
							final long sendTime = TestEvent.getLongFromArray(readBytes,
									readBuffer.position() + TestEvent.Offsets.time);
							final long nowNanoTime = System.nanoTime();
							propHisto.addObservation(nowNanoTime - sendTime);
							// occasional logging
							if ((messageReadCount & ((256 * 1024) - 1)) == 0) {
								logger.info("id:{} Prop Histo:{} {}", id, propHisto.getCount(), propHisto.toString());
								propHisto.clear();

								logger.debug(
										"Took to recv:{} len:{} posn:{} lim:{} readCount:{} bytesRead:{} bytesReadCount:{}",
										(nowNanoTime - sendTime), length, startPosition, readBuffer.limit(), bytesRead,
										bytesRead, bytesReadCount);

							}
							messageReadCount++;
							break;
						case TestEvent.MessageType.serverMessage:
							// logger.debug("ServerRTTMessage");
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
			public void closed(final SenderCallin callin) {
				logger.info("Close from callin:{}", callin);
				close();

			}

			public void close() {
				if (isRunning == false) {
					return;
				}
				logger.info("ClientClosing:" + bytesRead);
				logger.info("Propogate time Histo:{} {}", propHisto.getCount(), propHisto.toString());
				logger.info("RTT time Histo:{} {}", rttHisto.getCount(), rttHisto.toString());
				propHisto.clear();
				isRunning = false;

			}

			class TimerSenderCallback implements TimerCallback {

				@Override
				public void timerCallback(final long dueAt, final long currentNanoTime) {
					try {
						if (blocked) {
							timerHandler.fireIn(slowTimer);
							return;
						}

						final long elapsed = currentNanoTime - startTimeNano;

						while (elapsed * writeRatePerSecond > bytesWritten * 1000000000L) {

							TestEvent.putIntToArray(writeBytes, TestEvent.Offsets.type,
									TestEvent.MessageType.clientMessage);
							TestEvent.putIntToArray(writeBytes, TestEvent.Offsets.length, writeBytes.length);
							TestEvent.putLongToArray(writeBytes, TestEvent.Offsets.time, System.nanoTime());
							TestEvent.putLongToArray(writeBytes, TestEvent.Offsets.seqnum, messageCounter++);

							writeBuffer.position(0);
							writeBuffer.limit(writeBytes.length);
							callin.sendMessage(writeBytes, 0, writeBytes.length);
						}
						callin.flush();
						timerHandler.fireIn(fastTimer);

					} catch (final Exception e) {
						logger.error("Errro in callback", e);
						close();
					}
				}

			}

		}

	}

	public static class TestEventServer implements EventHandler<TestEvent>, AutoCloseable {
		private final Logger logger = LoggerFactory.getLogger(TestEventServer.class);
		private final AtomicLong counter = new AtomicLong();
		private final boolean coalsce;
		private final Histogram elapsedHisto = getHisto();
		private final Histogram delayHisto = getHisto();
		private final EstablishedServerConnectionCallback[] ecc = new EstablishedServerConnectionCallback[128];
		private final SenderHelper serverSenderHelper;

		public volatile SocketAddress remoteAddress = null;
		private SenderCallin serverCallin;

		public TestEventServer(final NIOWaitStrategy waiter, final boolean compact, final InetSocketAddress address) {
			coalsce = compact;
			serverSenderHelper = new SenderHelper(waiter);
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
						logger.error("Erro no connection with targetID:{})", event.targetID);
					}
					return;
				}
				ecc[event.targetID].handleData(event, sequence, endOfBatch);
				// flush anything without a batch
				if (endOfBatch) {
					for (int a = 0; a < ecc.length; a++) {
						if (ecc[a] != null && ecc[a].serverCallin.byteInBuffer() > 0) {
							ecc[a].serverCallin.flush();
						}
					}
				}
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

		public void close() {
			logger.info("CLOSESERVER EventServer");
			logger.info("ElapsedCallbackTime:" + elapsedHisto.getCount() + " " + elapsedHisto.toString());
			logger.info("DelayDataTime:" + delayHisto.getCount() + " " + delayHisto.toString());

			for (int a = 0; a < ecc.length; a++) {
				if (ecc[a] != null) {
					logger.info("closeServer id:{} ", a);
					ecc[a].close();
					ecc[a] = null;
				}
			}
		}

		class NIOServerCallback implements SenderHelper.SenderCallback {
			private final Logger logger = LoggerFactory.getLogger(NIOServerCallback.class);

			@Override
			public void connected(final SenderCallin callin) {
				logger.info("Connected callin:{}", callin);
				remoteAddress = callin.getRemoteAddress();
				for (int a = 0; a < ecc.length; a++) {
					if (ecc[a] == null) {

						final EstablishedServerConnectionCallback escc = new EstablishedServerConnectionCallback(callin,
								a, coalsce);
						callin.setId(a);
						ecc[a] = escc;
						break;
					}
					if (a == ecc.length - 1) {
						try {
							logger.error("Error could not find empty slot. closing");
							callin.close();
						} catch (Exception e) {

						}
					}
				}

			}

			@Override
			public void blocked(final SenderCallin callin) {
				logger.info("blocked Server write callin:{}", callin);
			}

			@Override
			public void unblocked(final SenderCallin callin) {
				logger.info("unblocked Server write  callin:{}", callin);
				serverCallin.unblockRead();
			}

			@Override
			public void closed(final SenderCallin callin) {
				logger.info("closed callin:{}", callin);
				ecc[callin.getId()].close();
			}

			@Override
			public void readData(final SenderCallin callin, final ByteBuffer buffer) {
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
			private final Logger logger = LoggerFactory.getLogger(EstablishedServerConnectionCallback.class);
			private final int maxQueueSize = 65536;
			private final LinkedList<byte[]> queue = new LinkedList<>();

			private boolean closed = false;

			private final SenderCallin serverCallin;
			private final int id;

			private long totalRead = 0;

			private long messageReadCount = 0;
			private long readSignalCount = 0;
			private long totalWriteSocket = 0;
			private long writeSignalCount = 0;
			private long writeCount = 0;
			private long dataLossCount = 0;
			private long endOfBatchCount = 0;
			private long messageCount = 0;

			private final ByteBuffer readBuffer;
			private final byte[] readBytes;
			private long respSeqNum = 0;
			private final ByteBuffer writeBuffer;
			long lastLogErrorNano = System.nanoTime();
			private final Histogram propHisto = getHisto();
			private final boolean coalsce;

			EstablishedServerConnectionCallback(final SenderCallin sc, final int id, final boolean coalsce) {
				this.serverCallin = sc;
				this.id = id;
				this.coalsce = coalsce;
				readBytes = new byte[sc.maxBuffer() * 2];
				readBuffer = ByteBuffer.wrap(readBytes);
				readBuffer.order(ByteOrder.LITTLE_ENDIAN);
				writeBuffer = ByteBuffer.allocate(sc.maxBuffer());
				writeBuffer.order(ByteOrder.LITTLE_ENDIAN);
			}

			public void handleData(final TestEvent event, final long sequence, final boolean endOfBatch)
					throws Exception {
				messageCount++;
				if (serverCallin.isWriteBlocked() >= 0) {
					if (coalsce) {
						// drop data
						logger.trace("Coalse: drop data size:{}", event.data.length);
					} else {
						if (queue.size() > maxQueueSize) {
							dataLossCount++;
							if (System.nanoTime() - lastLogErrorNano > TimeUnit.MILLISECONDS.toNanos(100)) {
								logger.error("Data lost: backlog exeeded count:{} callin:{}", dataLossCount,
										serverCallin);
								lastLogErrorNano = System.nanoTime();
							}
						} else {
							final byte[] dataCopy = new byte[event.getLength()];
							System.arraycopy(event.data, 0, dataCopy, 0, event.getLength());
							queue.add(dataCopy);
						}
					}
				} else {
					serverCallin.sendMessage(event.data, 0, event.getLength());
				}
				if (endOfBatch) {
					// change to only flush on end of batch.
					endOfBatchCount++;
					if (serverCallin.flush() == 0) {
						// wrote it all
					} else {
						// blocked
					}
				}
			}

			public void close() {
				if (closed) {
					logger.warn("Already closed:{}", id);
					return;
				}
				closed = true;
				logger.info("id:{} Closing Server socket:{}"//
						+ "\n\tREAD_SERVER: totalBytesRead:{} messageReadCount:{} readSignalCount:{}"//
						+ "\n\tWRITE_SERVER: totalWrite:{} writeSignalCount:{} writeCount:{}" //
						+ "\n\tBATCH: BatchCound:{} EndOfBatch:{} dataLoss:{}"//
						+ "\n\tHISTO: count:{} hist:{}", //
						id, serverCallin, //
						totalRead, messageReadCount, readSignalCount, //
						totalWriteSocket, writeSignalCount, writeCount, //
						messageCount, endOfBatchCount, dataLossCount, //
						propHisto.getCount(), propHisto.toString());

				try {
					serverCallin.close();
				} catch (Exception e) {
					logger.error("Error closing socket", e);
				}

			}

			public void read(final ByteBuffer readData) throws IOException {
				logger.debug("SERVERREAD {} {} {} {}", readBuffer.position(), readBuffer.limit() + readData.position(),
						readData.limit());
				readSignalCount++;
				final int bytesRead = readData.remaining();
				totalRead += bytesRead;
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
						case TestEvent.MessageType.clientMessage:

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
									if (serverCallin.bufferRemaining() < length) {
										logger.info("blocked write - some data remaining callin:{}", serverCallin);
										serverCallin.blockRead();
										readBuffer.compact();
										return;
									}
								}
								TestEvent.putIntToArray(readBytes, startPosition + TestEvent.Offsets.type,
										TestEvent.MessageType.serverMessage);

								TestEvent.putLongToArray(readBytes, startPosition + TestEvent.Offsets.responsTime,
										System.nanoTime());
								TestEvent.putLongToArray(readBytes, startPosition + TestEvent.Offsets.respSeqNum,
										respSeqNum++);
								serverCallin.sendMessage(readBytes, startPosition, length);
							}
							break;
						case TestEvent.MessageType.dataEvent:
							close();
							Assert.fail("unexpected dataEvent");
						case TestEvent.MessageType.serverMessage:
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

}