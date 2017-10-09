package com.ajt.disruptorIO;

import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
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
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.collections.Histogram;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

public class NIOWaitSelector2NIO {
	static {
		System.setProperty("org.apache.logging.log4j.simplelog.StatusLogger.level", "TRACE");
		System.setProperty("org.apache.logging.log4j.level", "DEBUG");

	}
	private final Logger logger = LoggerFactory.getLogger(NIOWaitSelector2NIO.class);
	private ExceptionHandler<TestEvent> errorHandler;
	private long sequenceNum = 0;
	private Disruptor<TestEvent> disruptorServer;
	private Disruptor<TestEvent> disruptorClient;
	private NIOWaitStrategy nioWaitStrategyClient;
	private NIOWaitStrategy nioWaitStrategyServer;

	private TestEventServer[] handlers;
	private ThreadFactory threadFactoryServer;
	private ThreadFactory threadFactoryClient;

	private TestEventClient tc;

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
		errorHandler = new ExceptionHandler<NIOWaitSelector2NIO.TestEvent>() {

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
		nioWaitStrategyClient = new NIOWaitStrategy(NIOWaitStrategy.getDefaultClock());
		nioWaitStrategyServer = new NIOWaitStrategy(NIOWaitStrategy.getDefaultClock());
		int ringBufferSize = 2048;
		disruptorServer = new Disruptor<>(TestEvent.EVENT_FACTORY, ringBufferSize, threadFactoryServer,
				ProducerType.SINGLE, nioWaitStrategyServer);
		disruptorServer.setDefaultExceptionHandler(errorHandler);

		disruptorClient = new Disruptor<>(TestEvent.EVENT_FACTORY, ringBufferSize, threadFactoryClient,
				ProducerType.SINGLE, nioWaitStrategyClient);
		disruptorClient.setDefaultExceptionHandler(errorHandler);

	}

	static long getLongFromArray(final byte[] data, final int offset) {
		return ((long) (data[offset + 0]) << 56) + //
				(((long) data[offset + 1] & 0xFF) << 48) + //
				(((long) data[offset + 2] & 0xFF) << 40) + //
				(((long) data[offset + 3] & 0xFF) << 32) + //
				(((long) data[offset + 4] & 0xFF) << 24) + //
				(((long) data[offset + 5] & 0xFF) << 16) + //
				(((long) data[offset + 6] & 0xFF) << 8) + //
				(((long) data[offset + 7] & 0xFF));
	}

	static int getIntFromArray(final byte[] data, final int offset) {
		return ((int) (data[offset + 0]) << 24) + //
				(((int) data[offset + 1] & 0xFF) << 16) + //
				(((int) data[offset + 2] & 0xFF) << 8) + //
				(((int) data[offset + 3] & 0xFF));
	}

	static void putLongToArray(final byte[] data, final int offset, final long value) {
		data[offset + 0] = (byte) ((value >> 56) & 0xFF);
		data[offset + 1] = (byte) ((value >> 48) & 0xFF);
		data[offset + 2] = (byte) ((value >> 40) & 0xFF);
		data[offset + 3] = (byte) ((value >> 32) & 0xFF);
		data[offset + 4] = (byte) ((value >> 24) & 0xFF);
		data[offset + 5] = (byte) ((value >> 16) & 0xFF);
		data[offset + 6] = (byte) ((value >> 8) & 0xFF);
		data[offset + 7] = (byte) ((value) & 0xFF);
	}

	static void putIntToArray(final byte[] data, final int offset, final int value) {
		data[offset + 0] = (byte) ((value >> 24) & 0xFF);
		data[offset + 1] = (byte) ((value >> 16) & 0xFF);
		data[offset + 2] = (byte) ((value >> 8) & 0xFF);
		data[offset + 3] = (byte) ((value) & 0xFF);
	}

	@After
	public void teardown() {
		disruptorServer.shutdown();
		try {
			nioWaitStrategyServer.close();
		} catch (Exception e) {
			logger.info("Error closing nioWait", e);
		}
		handlers = null;
		threadFactoryServer = null;
	}

	@Test
	public void testServerConnection() throws Exception {
		final long toSend = 1_00_000L;
		final long messageratePerSecond = 10000L;
		final long readRatePerSecond = 1_000_000L;
		final long writeRatePerSecond = 1_000L;
		final int clientCount = 2;
		handlers = new TestEventServer[] { new TestEventServer(nioWaitStrategyServer, false) };
		disruptorServer.handleEventsWith(handlers);
		testFastServer(toSend, messageratePerSecond, readRatePerSecond, writeRatePerSecond, clientCount, false);
	}

	// runs on the main thread
	private void testFastServer(final long toSend, final long messageratePerSecond, final long readRatePerSecond,
			final long writeRatePerSecond, int clients, boolean lossy) throws Exception {
		try (final ServerSocketChannel socketChannel = ServerSocketChannel.open()) {
			logger.info("Disruptor creating new disruptor for this context. toSend:{} rateAt:{}", toSend,
					messageratePerSecond);

			disruptorClient.handleEventsWith(new TestEventClient[] { tc });
			logger.debug(
					"Starting AsyncLogger disruptor for this context with ringbufferSize={}, waitStrategy={}, "
							+ "exceptionHandler={}...",
					disruptorServer.getRingBuffer().getBufferSize(), nioWaitStrategyServer.getClass().getSimpleName(),
					errorHandler);
			disruptorServer.start();
			disruptorClient.start();

			final RingBuffer<TestEvent> rb = disruptorServer.getRingBuffer();

			socketChannel.configureBlocking(false);
			// SocketAddress sa = new InetSocketAddress("192.168.1.76", 9999);
			socketChannel.bind(null, 0);

			// pass to the disruptor thread.
			nioWaitStrategyServer.getScheduledExecutor().execute(new Runnable() {

				@Override
				public void run() {
					try {
						handlers[0].newServer(socketChannel);
					} catch (Exception e) {
						logger.error("Error starting server", e);
					}
				}
			});

			// create a client set using the client disruptor
			tc = new TestEventClient(clients, writeRatePerSecond, readRatePerSecond, socketChannel.getLocalAddress(),
					nioWaitStrategyClient);

			// pass to the disruptor thread - start command on correct thread callback
			nioWaitStrategyClient.getScheduledExecutor().execute(new Runnable() {

				@Override
				public void run() {
					try {
						tc.start();
					} catch (Exception e) {
						logger.error("Error starting server", e);
					}
				}
			});
			// wait to connect before sending
			Thread.sleep(50);
			final byte[] byteDataToSend = "MyMessageToTheClientMyMessageToTheClientMyMessageToTheClientMyMessageToTheClientMyMessageToTheClientMyMessageToTheClientMyMessageToTheClientMyMessageToTheClientMyMessageToTheClient"
					.getBytes();
			long actualMessageSendCount = 0;
			final long startTimeNanos = System.nanoTime();
			int b = 0;
			while (actualMessageSendCount < toSend) {
				final long currentTimeNanos = System.nanoTime();
				final long elapsed = currentTimeNanos - startTimeNanos;

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
						putLongToArray(byteDataToSend, TestEvent.Offsets.time, System.nanoTime());
						putLongToArray(byteDataToSend, TestEvent.Offsets.seqnum, sequenceNum);

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

					logger.debug("total sent:{} sendRate:{} runTimeMS:{} ", actualMessageSendCount,
							actualMessageSendCount * 1000000000 / (currentTimeNanos - startTimeNanos),
							(currentTimeNanos - startTimeNanos) / 1000000L);

				}

			}
			logger.info("Finished sending");
			final long endTime = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(1000);
			while (System.nanoTime() < endTime) {
				if (handlers[0].counter.get() == toSend) {
					logger.info("completed :{}", toSend);
					break;
				}
			}
			assertThat("Message count did not all get delivered by disruptor to client, slow or blocked client ? ",
					handlers[0].counter.get(), Matchers.is(toSend));
			nioWaitStrategyClient.getScheduledExecutor().execute(new Runnable() {

				@Override
				public void run() {
					try {
						tc.dump();
					} catch (Exception e) {
						logger.error("Error starting server", e);
					}
				}
			});
			// pass to the disruptor thread.
			nioWaitStrategyServer.getScheduledExecutor().execute(new Runnable() {

				@Override
				public void run() {
					try {
						handlers[0].closeServer();
					} catch (Exception e) {
						logger.error("Error starting server", e);
					}
				}
			});
			Thread.sleep(10);
		} finally {

		}

	}

	public static class TestEventClient implements EventHandler<TestEvent> {
		private final Logger logger = LoggerFactory.getLogger(TestEventClient.class);
		final int count;
		final long writeRatePerSecond;
		final long readRatePerSecond;
		final SocketAddress sa;
		final NIOClientConnector[] clients;
		final NIOWaitStrategy nioWait;
		boolean isRunning = false;
		long _startTimeNanos = 0;
		long _currentTimeNanos = 0;

		public TestEventClient(final int count, //
				final long writeRatePerSecond, //
				final long readRatePerSecond, //
				final SocketAddress sa, //
				final NIOWaitStrategy nioWait) {
			this.sa = sa;
			this.count = count;
			this.writeRatePerSecond = writeRatePerSecond;
			this.readRatePerSecond = readRatePerSecond;
			this.nioWait = nioWait;
			clients = new NIOClientConnector[count];
			for (int a = 0; a < count; a++) {
				clients[a] = new NIOClientConnector(a);
			}
		}

		/** print out some stats */
		public void dump() {
			for (int c = 0; c < clients.length; c++) {
				logger.debug(
						"{}:   runningFor(ms):{} isRunning:{} MsgCount:{} ReadMBYTE:{} readByteRate(mbit):{} readCycle:{} readCount:{} writeAtLeast:{} writtenBytes:{} ",
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
															// at
															// lease
						clients[c].bytesWritten); // written
			}

		}

		@Override
		public void onEvent(TestEvent event, long sequence, boolean endOfBatch) throws Exception {
			// ignore all events }
		}

		void start() throws Exception {
			logger.info("Start");
			for (int a = 0; a < count; a++) {
				final SocketChannel sc = SocketChannel.open();
				sc.configureBlocking(false);
				sc.connect(sa);
				logger.info("Start [" + a + "]:" + sa);

				final SelectionKey sk = nioWait.registerSelectableChannel(sc, clients[a]);
				sk.interestOps(SelectionKey.OP_CONNECT);
				clients[a].sk = sk;
				clients[a].sc = sc;
			}
		}

		void closeServer() throws Exception {
			isRunning = false;
		}

		class NIOClientConnector implements NIOWaitStrategy.SelectorCallback {
			boolean isRunning = true;
			long readShouldBeAtLeast = 0;
			long writeShouldBeAtLeast = 0;
			long bytesRead = 0;
			long bytesReadCallback = 0;
			long messageReadCount = 0;
			long bytesReadCount = 0;
			long bytesWritten = 0;
			long recvMsgCount = 0;
			SelectionKey sk;
			SocketChannel sc;
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
			final Histogram propHisto = new Histogram(new long[] { 100, 200, 400, 800, 1600, 3200, 6400, 12800, 25600,
					51200, 102400, 204800, 409600, 819200, 1638400, 3276800L, 6553600L, 13107200L, 13107200L * 2L,
					13107200L * 4L, 13107200L * 8L, 13107200L * 16L, 13107200L * 10000L });

			public NIOClientConnector(final int id) {
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

			public void close() {
				logger.info("CLientClosing:" + bytesRead);
				logger.info("Prop Histo:{} {}", propHisto.getCount(), propHisto.toString());
				propHisto.clear();
				isRunning = false;
				logger.info("Closing:" + sk + " " + sc);
				try {
					sk.cancel();
					sc.close();
				} catch (Exception e) {

				}
			}

			@Override
			public void opRead(final SelectableChannel channel, final long currentTimeNanos) {
				bytesReadCallback++;
				_currentTimeNanos = currentTimeNanos;
				try {
					final SocketChannel sk = (SocketChannel) channel;
					if (sk.isConnected()) {
						for (int a = 0; a < 20; a++) {
							final long readCount = sk.read(readBuffer);
							if (readCount == 0) {
								break;
							} else if (readCount == -1) {
								close();
							} else {
								bytesReadCount++;
								bytesRead += readCount;
								int bufferPosition = readBuffer.position(); // point in buffer of last read byte
								int startPosition = 0;

								readBuffer.position(startPosition);
								readBuffer.limit(bufferPosition);
								while (true) {
									if (readBuffer.limit() - readBuffer.position() < 8) {
										logger.info("SHort Buff COMPACT");
										readBuffer.compact();
										break;
									}
									final int length = getIntFromArray(readBytes,
											readBuffer.position() + TestEvent.Offsets.length);
									assertThat(" posn:" + readBuffer.position() + " lim:" + readBuffer.limit(), //
											length, Matchers.greaterThan(16));
									//
									if (startPosition + length <= readBuffer.limit()) {
										// got a full message
										final int type = getIntFromArray(readBytes, readBuffer.position() + 4);
										switch (type) {
										case TestEvent.MessageType.clientMessage:
											logger.debug("unknown type");
											break;
										case TestEvent.MessageType.dataEvent:
											final long sendTime = getLongFromArray(readBytes,
													readBuffer.position() + TestEvent.Offsets.time);
											final long nowNanoTime = System.nanoTime();
											propHisto.addObservation(nowNanoTime - sendTime);
											// occasional logging
											if ((messageReadCount & ((256 * 1024) - 1)) == 0) {
												logger.info("id:{} Prop Histo:{} {}", id, propHisto.getCount(),
														propHisto.toString());
												propHisto.clear();

												logger.debug(
														"Took to recv:{} len:{} posn:{} lim:{} readCount:{} bytesRead:{} bytesReadCount:{}",
														(nowNanoTime - sendTime), length, startPosition,
														readBuffer.limit(), readCount, bytesRead, bytesReadCount);

											}
											messageReadCount++;
											break;
										case TestEvent.MessageType.serverMessage:
											logger.debug("ServerRTTMessage");
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
										logger.info("Client COMPACT posn:{} lim:{}", readBuffer.position(),
												readBuffer.limit());
										readBuffer.compact();
										break;
									}
								}
							}
						}
					} else {
						close();
					}
				} catch (Exception e) {
					logger.error("Error", e);
				}
			}

			@Override
			public void opAccept(SelectableChannel channel, long currentTimeNanos) {
				logger.error("Error not execpting accept");

			}

			@Override
			public void opConnect(SelectableChannel channel, long currentTimeNanos) {
				_startTimeNanos = currentTimeNanos;
				logger.info("opConnect:" + channel + " " + sk.interestOps());
				try {
					final SocketChannel sc = (SocketChannel) channel;
					sc.configureBlocking(false);
					final boolean finishConnect = sc.finishConnect();
					SelectionKey sk2 = sk.interestOps(SelectionKey.OP_READ);

					timerHandler.fireIn(0);
					startTimeNano = timerHandler.currentNanoTime();

					logger.info("opConnect Complete :" + channel + " " + sk.interestOps() + " " + sk2.interestOps()
							+ " " + finishConnect);
				} catch (Exception e) {
					logger.error("Error connecting", e);
				}
			}

			@Override
			public void opWrite(final SelectableChannel channel, final long currentTimeNanos) {
				if (blocked) {
					try {
						final int remaining = writeBuffer.remaining();
						final int bytesWrittenCycle = sc.write(writeBuffer);
						if (bytesWrittenCycle == -1) {
							close();
							return;
						} else if (bytesWrittenCycle < remaining) {
							// remain written;
						} else {
							sk.interestOps(SelectionKey.OP_READ);
							blocked = false;
							timerHandler.fireIn(0);
						}
						bytesWritten += bytesWrittenCycle;
					} catch (Exception e) {
						logger.info("Error cycle write error", e);
						close();
					}
				} else {

				}
			}

			class TimerSenderCallback implements TimerCallback {

				@Override
				public void timerCallback(final long dueAt, final long currentNanoTime) {
					try {
						if (blocked) {
							timerHandler.fireIn(slowTimer);
							return;
						}
						if (sk == null || sc == null) {
							timerHandler.fireIn(slowTimer);
							return;
						}
						final long elapsed = currentNanoTime - startTimeNano;

						while (elapsed * writeRatePerSecond > bytesWritten * 1000000000L) {

							putIntToArray(writeBytes, TestEvent.Offsets.type, TestEvent.MessageType.clientMessage);
							putIntToArray(writeBytes, TestEvent.Offsets.length, writeBytes.length);
							putLongToArray(writeBytes, TestEvent.Offsets.time, System.nanoTime());
							putLongToArray(writeBytes, TestEvent.Offsets.seqnum, messageCounter++);

							writeBuffer.position(0);
							writeBuffer.limit(writeBytes.length);

							final int bytesWrittenCycle = sc.write(writeBuffer);
							if (bytesWrittenCycle == -1) {
								close();
								return;
							} else if (bytesWrittenCycle < writeBuffer.capacity()) {
								logger.debug("client:{} blocked writing:{} counter:{}", id, bytesWritten,
										messageCounter);
								sk.interestOps(SelectionKey.OP_READ + SelectionKey.OP_WRITE);
								blocked = true;
								writeBuffer.compact();
								bytesWritten += bytesWrittenCycle;
								timerHandler.fireIn(slowTimer);
								break;
							} else {
								bytesWritten += bytesWrittenCycle;
							}
						}
						timerHandler.fireIn(fastTimer);

					} catch (final Exception e) {
						logger.error("Errro in callback", e);
						close();
					}
				}

			}

		}

	}

	public static class TestEventServer implements EventHandler<TestEvent> {
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
			if (callback.ecc[event.targetID] == null) {
				if (endOfBatch) {
					logger.error("Erro no connection with targetID:{})", event.targetID);
				}
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

		void closeServer() throws Exception {
			logger.info("CLOSESERVER");
			logger.info("ElapsedHisto:" + elapsedHisto.getCount() + " " + elapsedHisto.toString());
			logger.info("DelayHisto:" + delayHisto.getCount() + " " + delayHisto.toString());

			for (int a = 0; a < callback.ecc.length; a++) {
				if (callback.ecc[a] != null) {
					logger.info("closeServer id:{} ", a);
					callback.ecc[a].closeConnection();
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
				final ServerSocketChannel ssc = (ServerSocketChannel) channel;
				try {
					final SocketChannel socketChannel = ssc.accept();
					if (socketChannel == null) {
						logger.info("Error no connetion for use accept=null");
						return;
					}
					final boolean finishConnect = socketChannel.finishConnect();
					socketChannel.configureBlocking(false);
					socketChannel.setOption(StandardSocketOptions.SO_SNDBUF, 1024 * 64);
					socketChannel.setOption(StandardSocketOptions.SO_RCVBUF, 1024 * 64);
					socketChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);
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
					logger.info("opAccept completed local:{} remote:{} registered:{} finishConnect:{}",
							socketChannel.getLocalAddress(), socketChannel.getRemoteAddress(),
							socketChannel.isRegistered(), finishConnect);
				} catch (Exception e) {
					logger.info("Error failed handshake", e);
				}

			}

			@Override
			public void opConnect(final SelectableChannel channel, final long currentTimeNanos) {
				logger.info("opConnect");
				Assert.fail("opConnect SHould not be called");

			}

			@Override
			public void opWrite(final SelectableChannel channel, final long currentTimeNanos) {
				logger.info("NIOAcceptorCallback- opWrite");
				Assert.fail("opWrite SHould not be called");

			}

			/**
			 * callback to interact with an established client session batches data into
			 * upto 4kb chunks before flushing to socket at the end of the batch or when the
			 * buffer is full.
			 * 
			 * @author ajt
			 *
			 */
			class EstablishedServerConnectionCallback implements NIOWaitStrategy.SelectorCallback {
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
				private long messageReadCount = 0;
				private long readSignalCount = 0;
				private long totalWriteSocket = 0;
				private long writeSignalCount = 0;
				private long writeCount = 0;
				private long dataLossCount = 0;
				private long endOfBatchCount = 0;

				private final ByteBuffer readBuffer;
				private final byte[] readBytes;
				private long respSeqNum = 0;
				private final ByteBuffer writeBuffer;
				long lastLogErrorNano = System.nanoTime();
				final Histogram propHisto = new Histogram(new long[] { 100, 200, 400, 800, 1600, 3200, 6400, 12800,
						25600, 51200, 102400, 204800, 409600, 819200, 1638400, 3276800L, 6553600L, 13107200L,
						13107200L * 2L, 13107200L * 4L, 13107200L * 8L, 13107200L * 16L, 13107200L * 10000L });

				EstablishedServerConnectionCallback(final SocketChannel sc, int id) {
					this.socketChannel = sc;
					this.id = id;
					readBytes = new byte[4096];
					readBuffer = ByteBuffer.wrap(readBytes);
					readBuffer.order(ByteOrder.LITTLE_ENDIAN);
					writeBuffer = ByteBuffer.allocate(1024 * 1024);
					writeBuffer.order(ByteOrder.LITTLE_ENDIAN);
				}

				private void closeConnection() {
					logger.info(
							"id:{} Closing socket:{} totalRead:{} readSignalCount:{} readiteration:{} totalWrite:{} writeSignalCount:{} writeCount:{}", //
							id, socketChannel, totalRead, readSignalCount, readCount, totalWriteSocket,
							writeSignalCount, writeCount);
					logger.info("EndOfBatch:{} dataLoss:{}", endOfBatchCount, dataLossCount);
					logger.info("id:{} ackCnt:{} Histo:{}", id, propHisto.getCount(), propHisto.toString());
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
					// logger.info("Server flush");
					if (currentlyBlocked) {
						return false;
					}
					writeBuffer.flip();
					final int remaining = writeBuffer.remaining();
					final int bytesWritten = socketChannel.write(writeBuffer);
					if (bytesWritten == -1) {
						closeConnection();
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
								final byte[] dataCopy = new byte[event.getLength()];
								System.arraycopy(event.data, 0, dataCopy, 0, event.getLength());
								queue.add(dataCopy);
							}
						}
					} else {
						if (endOfBatch) {
							// change to only flush on end of batch.
							endOfBatchCount++;
						}
						if (writeBuffer.remaining() < event.getLength()) {
							if (flush()) {

							} else {
								return;
							}
						}
						writeBuffer.put(event.data, 0, event.getLength());
						isDirty = true;

					}

				}

				@Override
				public void opRead(final SelectableChannel channel, final long currentTimeNanos) {
					try {
						readSignalCount++;
						int bytesRead = 0;
						do {

							bytesRead = socketChannel.read(readBuffer);
							// logger.info("Read:" + read);
							if (bytesRead == -1) {
								closeConnection();
								return;
							} else {

								totalRead += bytesRead;
								readCount++;
								int bufferPosition = readBuffer.position(); // point in buffer of last read byte
								int startPosition = 0;

								readBuffer.position(startPosition);
								readBuffer.limit(bufferPosition);

								while (readBuffer.limit() - readBuffer.position() > 4) {
									final int length = getIntFromArray(readBytes,
											readBuffer.position() + TestEvent.Offsets.length);
									assertThat(length, Matchers.greaterThan(16));
									if (startPosition + length <= readBuffer.limit()) {
										// got a full message
										final int type = getIntFromArray(readBytes,
												readBuffer.position() + TestEvent.Offsets.type);
										switch (type) {
										case TestEvent.MessageType.clientMessage:

											final long sendTime = getLongFromArray(readBytes,
													readBuffer.position() + TestEvent.Offsets.time);
											final long nowNanoTime = System.nanoTime();
											propHisto.addObservation(nowNanoTime - sendTime);
											// occasional logging
											if ((messageReadCount & ((256 * 1024) - 1)) == 0) {
												logger.info("id:{} Prop Histo:{} {}", id, propHisto.getCount(),
														propHisto.toString());
												propHisto.clear();

												logger.debug(
														"Took to recv:{} len:{} posn:{} lim:{} readCount:{} bytesRead:{} bytesReadCount:{}",
														(nowNanoTime - sendTime), length, startPosition,
														readBuffer.limit(), readCount, bytesRead, totalRead);

											}
											// return some data
											if (!currentlyBlocked) {
												if (writeBuffer.remaining() < length) {
													if (flush()) {
														// continue
													} else {
														return;
													}
												}
												putIntToArray(readBytes, startPosition + TestEvent.Offsets.type,
														TestEvent.MessageType.serverMessage);

												putLongToArray(readBytes, startPosition + TestEvent.Offsets.responsTime,
														System.nanoTime());
												putLongToArray(readBytes, startPosition + TestEvent.Offsets.respSeqNum,
														respSeqNum++);
												writeBuffer.put(readBytes, startPosition, length);
												isDirty = true;

											}
											messageReadCount++;
											break;
										case TestEvent.MessageType.dataEvent:
											closeConnection();
											Assert.fail("unexpected dataEvent");
										case TestEvent.MessageType.serverMessage:
											closeConnection();
											Assert.fail("unexpected serverMessage");
										default:
											closeConnection();
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
										logger.info("COMPACT posn:{} Lim:{}", readBuffer.position(),
												readBuffer.limit());
										readBuffer.compact();
										logger.info("COMPACT posn:{} Lim:{}", readBuffer.position(),
												readBuffer.limit());
										break;
									}
								}

							}
						} while (bytesRead > 0);
					} catch (Exception e) {
						closeConnection();
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
							closeConnection();
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
										closeConnection();
									} else {
										thisWrite += bytesWritten;
										totalWriteSocket += bytesWritten;
										if (bytesWritten < bytesRemaining) {
											writeBuffer.compact();
											logger.debug(
													"{} baglock incomplete - socket not caught up size:{} writeCount:{} writeSignalCOunt:{} writtenBytes:{} totalWriteSocket:{} thisWrte:{} took:(us):{} chan:{} sockCHan:{}", //
													id, queue.size(), writeCount, writeSignalCount, bytesWritten,
													totalWriteSocket, thisWrite, (System.nanoTime() - start) / 1000L,
													channel, socketChannel);
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

		interface MessageType {
			int dataEvent = 1;
			int clientMessage = 2;
			int serverMessage = 3;

		}

		private EventType type = EventType.UNKNOWN;
		private byte[] data = null;
		private int targetID = 0;
		private final long seqNum;
		private long nanoSendTime = 0;
		private static long counter = 0;
		private final ByteBuffer writeBuffer;
		private final ByteBuffer readBuffer;

		interface Offsets {
			int length = 0;
			int type = 4;
			int seqnum = 8;
			int respSeqNum = 16;
			int time = 24;
			int responsTime = 32;
		}

		private TestEvent() {
			seqNum = counter++;
			data = new byte[4096];
			writeBuffer = ByteBuffer.wrap(data);
			writeBuffer.order(ByteOrder.LITTLE_ENDIAN);
			readBuffer = ByteBuffer.wrap(data);
			readBuffer.order(ByteOrder.LITTLE_ENDIAN);
		}

		public void setData(final byte[] data_, final int offset, final int length, final int type) {
			writeBuffer.clear();
			writeBuffer.put(data_, offset, length);
			putIntToArray(data, Offsets.length, length);
			putIntToArray(data, Offsets.type, type);
		}

		public int getLength() {
			return getIntFromArray(data, Offsets.length);
		}

		public int getType() {
			return getIntFromArray(data, Offsets.type);
		}

		public byte[] getData() {
			return data;
		}

		public static final EventFactory<TestEvent> EVENT_FACTORY = new EventFactory<TestEvent>() {
			@Override
			public TestEvent newInstance() {
				return new TestEvent();
			}
		};

	}

}