package com.ajt.disruptorIO;

import static org.hamcrest.CoreMatchers.is;
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
		final long toSend = 100_000_000L;
		final long messageratePerSecond = 100_000_000L;
		final long readRatePerSecond = 1000_000_000L;
		final long writeRatePerSecond = 1000_000_000L;
		handlers = new TestEventServer[] { new TestEventServer(nioWaitStrategyServer, false) };
		disruptorServer.handleEventsWith(handlers);
		testFastServer(toSend, messageratePerSecond, readRatePerSecond, writeRatePerSecond, 99, false);
	}

	// runs on the main thread
	private void testFastServer(final long toSend, final long messageratePerSecond, final long readRatePerSecond,
			final long writeRatePerSecond, int clients, boolean lossy) throws Exception {

		try (final ServerSocketChannel socketChannel = ServerSocketChannel.open()) {
			logger.info("Disruptor creating new disruptor for this context. toSend:{} rateAt:{}", toSend,
					messageratePerSecond);
			byte[] dataToSendToClient = "ddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
					.getBytes();
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
					nioWaitStrategyServer);

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

				if ((a & (1024 * 1024 - 1)) == 0) {
					// pass to the disruptor thread.
					logger.debug("total sent:{} sendRate:{} runTimeMS:{} ", a,
							a * 1000000000 / (currentTimeNanos - startTimeNanos),
							(currentTimeNanos - startTimeNanos) / 1000000L);
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
			assertThat(handlers[0].counter.get(), is(toSend));
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

		public TestEventClient(int count, final long writeRatePerSecond, final long readRatePerSecond, SocketAddress sa,
				NIOWaitStrategy nioWait) {
			this.sa = sa;
			this.count = count;
			this.writeRatePerSecond = writeRatePerSecond;
			this.readRatePerSecond = readRatePerSecond;
			clients = new NIOClientConnector[count];
			for (int a = 0; a < count; a++) {
				clients[a] = new NIOClientConnector();
			}
			this.nioWait = nioWait;

		}

		/** print out some stats */
		public void dump() {
			for (int c = 0; c < clients.length; c++) {
				logger.debug(
						"{}:   runningFor(ms):{} MsgCount:{} ReadMBYTE:{} readByteRate(mbit):{} readCycle:{} readCount:{} writeAtLeast:{} writtenBytes:{} ",
						c, // c
						(_currentTimeNanos - _startTimeNanos) / 1000000, // runfor
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
			// TODO Auto-generated method stub
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
			long bytesReadCount = 0;
			long bytesWritten = 0;
			long recvMsgCount = 0;
			SelectionKey sk;
			final byte[] readBytes;
			final ByteBuffer readBuffer;

			public NIOClientConnector() {
				readBytes = new byte[1024 * 1024];
				readBuffer = ByteBuffer.wrap(readBytes);
			}

			public void close() throws Exception {
				logger.info("CLientClosing:" + bytesRead);
				sk.cancel();
				isRunning = false;
			}

			public void close(SelectableChannel channel) throws Exception {
				logger.info("Closing:" + sk + " " + channel);
				close();
				channel.close();
			}

			@Override
			public void opRead(final SelectableChannel channel, final long currentTimeNanos) {
				bytesReadCallback++;
				_currentTimeNanos = currentTimeNanos;
				try {
					final SocketChannel sk = (SocketChannel) channel;
					for (int a = 0; a < 20; a++) {
						readBuffer.clear();
						final long readCount = sk.read(readBuffer);
						if (readCount == 0) {
							break;
						} else if (readCount == -1) {
							close(channel);
						} else {
							bytesReadCount++;
							bytesRead += readCount;
						}
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
					logger.info("opConnect Complete :" + channel + " " + sk.interestOps() + " " + sk2.interestOps()
							+ " " + finishConnect);
				} catch (Exception e) {
					logger.error("Error connecting", e);
				}
			}

			@Override
			public void opWrite(SelectableChannel channel, long currentTimeNanos) {

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

				private void closeConnection() {
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
								closeConnection();
								return;
							} else {
								totalRead += read;
								readCount++;
							}
						} while (read > 0);
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