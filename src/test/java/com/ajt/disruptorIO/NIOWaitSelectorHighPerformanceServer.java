package com.ajt.disruptorIO;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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

public class NIOWaitSelectorHighPerformanceServer {
	static {
		System.setProperty("org.apache.logging.log4j.simplelog.StatusLogger.level", "TRACE");
		System.setProperty("org.apache.logging.log4j.level", "DEBUG");

	}
	private final Logger logger = LoggerFactory.getLogger(NIOWaitSelectorHighPerformanceServer.class);
	private NIOWaitStrategy.NIOClock clock;
	private ExceptionHandler<TestEvent> errorHandler;
	private long sequenceNum = 0;

	@Before
	public void setup() {
		clock = NIOWaitStrategy.getDefaultClock();
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

	Disruptor<TestEvent> disruptor;
	LatencyTimer lt;

	@After
	public void teardown() {
		try {
			nioWaitStrategy.close();
		} catch (Exception e) {

		}
		disruptor.shutdown();
	}

	private NIOWaitStrategy nioWaitStrategy;
	final ThreadFactory threadFactory = new ThreadFactory() {

		@Override
		public Thread newThread(Runnable r) {
			final Thread th = new Thread(r, "WaStratThread");

			return th;
		}
	};

	@Test
	public void testServerConnection() throws Exception {
		final long toSend = 100_000_000L;
		final long messageratePerSecond = 1_000_000L;
		final long readRatePerSecond = 1000_000_1000L;
		final long writeRatePerSecond = 1000_000_000L;
		handlers = new TestEventHandler[] { new TestEventHandler(nioWaitStrategy, false) };
		disruptor.handleEventsWith(handlers);
		testFastServer(toSend, messageratePerSecond, readRatePerSecond, writeRatePerSecond, 8, false);
	}

	private volatile TestEventHandler[] handlers;
	TestClient[] tc = new TestClient[128];

	/** simple test client. socket listener per client */
	class TestClient {
		final AtomicBoolean isRunning = new AtomicBoolean(true);
		ServerSocketChannel socketChannel;
		final AtomicLong readShouldBeAtLeast = new AtomicLong();
		final AtomicLong writeShouldBeAtLeast = new AtomicLong();
		final AtomicLong bytesRead = new AtomicLong();
		final AtomicLong bytesWritten = new AtomicLong();
		final AtomicLong recvCOut = new AtomicLong(0);
		SelectionKey key;
		private NIOWaitStrategy.SelectorCallback callback;

		final Socket s;
		final Thread readThread;
		final Thread writeThread;
		final int id;

		TestClient(int count, final long writeRatePerSecond, final long readRatePerSecond) throws Exception {
			id = count;
			socketChannel = ServerSocketChannel.open();
			s = new Socket();
			// pick a random address
			socketChannel.configureBlocking(false);
			socketChannel.bind(null, 0);
			// call method on correct thread.

			Thread.sleep(50);
			final SocketAddress sa = socketChannel.getLocalAddress();
			logger.info("local connection to :{}", sa);
			s.connect(sa, 1000);
			s.setTcpNoDelay(false);
			final InputStream is = s.getInputStream();
			final OutputStream os = s.getOutputStream();
			final long startTimeNanos = System.nanoTime();
			// blocking - force rates.
			readThread = new Thread(new Runnable() {

				@Override
				public void run() {
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
				}
			});
			readThread.setName("testClientReadThread-" + count);
			readThread.start();
			writeThread = new Thread(new Runnable() {

				@Override
				public void run() {
					try {
						byte[] writeBytes = "Hello worldHello worldHello worldHello worldHello worldHello worldHelloworldHello worldHello worldHello world"
								.getBytes();
						while (isRunning.get()) {
							final long currentTimeNanos = System.nanoTime();
							writeShouldBeAtLeast
									.set(writeRatePerSecond * ((currentTimeNanos - startTimeNanos) / 1000_000_000L));
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
				}
			});
			writeThread.setName("testClientWriteThread-" + count);

			writeThread.start();

		}

		public void start() {
			nioWaitStrategy.getScheduledExecutor().execute(new Runnable() {

				@Override
				public void run() {
					try {
						handlers[0].newServer(socketChannel, id);
					} catch (Exception e) {
						logger.error("Error starting server", e);
					}
				}
			});
		}

		void close() {
			nioWaitStrategy.getScheduledExecutor().execute(new Runnable() {

				@Override
				public void run() {
					try {
						handlers[0].closeServer(id);
					} catch (Exception e) {
						logger.error("Error starting server", e);
					}
				}
			});
			isRunning.set(false);
			readThread.interrupt();
			writeThread.interrupt();
		}
	}

	private void testFastServer(final long toSend, final long messageratePerSecond, final long readRatePerSecond,
			final long writeRatePerSecond, int clients, boolean lossy) throws Exception {

		logger.info("Disruptor creating new disruptor for this context. toSend:{} rateAt:{}", toSend,
				messageratePerSecond);
		String dataToSendToClient = "ddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
		try {

			logger.debug(
					"Starting AsyncLogger disruptor for this context with ringbufferSize={}, waitStrategy={}, "
							+ "exceptionHandler={}...",
					disruptor.getRingBuffer().getBufferSize(), nioWaitStrategy.getClass().getSimpleName(),
					errorHandler);
			disruptor.start();
			lt.register(nioWaitStrategy);

			final RingBuffer<TestEvent> rb = disruptor.getRingBuffer();

			tc = new TestClient[clients];
			for (int b = 0; b < clients; b++) {
				tc[b] = new TestClient(b, writeRatePerSecond, readRatePerSecond);
				tc[b].start();
			}
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

				if ((a & 8191) == 0) {
					for (int c = 0; c < clients; c++) {
						logger.debug(
								"{}: Pause elapsed:{} diff:{} sentMsg:{}  thisInst:{} readAtLeast:{} bytesRead:{} writeAtLeast:{} writtenBytes:{} ",
								c, (currentTimeNanos - startTimeNanos) / 1000000,
								currentTimeNanos - timeShouldBeAtLeast, a, tc[c].recvCOut.get(),
								tc[c].readShouldBeAtLeast.get(), tc[c].bytesRead, tc[c].writeShouldBeAtLeast.get(),
								tc[c].bytesWritten);
					}
				}

			}
			logger.info("Finished sending");
			for (int c = 0; c < clients; c++) {

				tc[c].s.close();
				tc[c].isRunning.set(false);
			}
			final long endTime = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(10);
			while (System.nanoTime() < endTime) {
				if (handlers[0].counter.get() == toSend) {
					logger.info("completed :{}", toSend);

					break;
				}
			}
			lt.stop();
			Thread.sleep(10);
			assertThat(handlers[0].counter.get(), is(toSend));
			long end = System.nanoTime();
			for (int c = 0; c < clients; c++) {
				logger.info("Took(ms):{} DataSend:{} rate:{} MBwritten:{} rate:{}", (end - startTimeNanos),
						handlers[0].counter.get(), handlers[0].counter.get() * 1000 / (end - startTimeNanos),
						tc[c].bytesWritten.get() / 1000000,
						tc[c].bytesWritten.get() / (1000 * end - 1000 * startTimeNanos));
				logger.debug(
						"{}:  sentMsg:{}  thisInst:{} readAtLeast:{} bytesRead:{} writeAtLeast:{} writtenBytes:{} ", c,
						a, tc[c].recvCOut.get(), tc[c].readShouldBeAtLeast.get(), tc[c].bytesRead,
						tc[c].writeShouldBeAtLeast.get(), tc[c].bytesWritten);
			}
		} finally {

		}

	}

	public class TestEventHandler implements EventHandler<TestEvent> {
		private final Logger logger = LoggerFactory.getLogger(TestEventHandler.class);
		private final AtomicLong counter = new AtomicLong();
		private final NIOWaitStrategy waitStrat;
		private EstablishedConnectionCallback[] ecc = new EstablishedConnectionCallback[128];

		// private final HashSet<EstablishedConnectionCallback> socketChannelSet = new
		// HashSet<>();
		private final boolean coalsce;
		private final Histogram elapsedHisto = new Histogram(new long[] { 100, 200, 400, 800, 1600, 3200, 6400, 12800,
				25600, 51200, 102400, 204800, 409600, 819200, 1638400, 3276800L, 6553600L, 13107200L, 13107200L * 2L,
				13107200L * 4L, 13107200L * 8L, 13107200L * 16L, 13107200L * 10000L });
		private final Histogram delayHisto = new Histogram(new long[] { 100, 200, 400, 800, 1600, 3200, 6400, 12800,
				25600, 51200, 102400, 204800, 409600, 819200, 1638400, 3276800L, 6553600L, 13107200L, 13107200L * 2L,
				13107200L * 4L, 13107200L * 8L, 13107200L * 16L, 13107200L * 10000L });

		public TestEventHandler(NIOWaitStrategy waiter, boolean compact) {
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
			tc[event.targetID].recvCOut.incrementAndGet();
			if (ecc[event.targetID] == null) {
				return;
			}
			ecc[event.targetID].handleData(event, sequence, endOfBatch);

		}

		void closeServer(final int id) throws Exception {
			logger.info("closeServer id:{} ", id);
			logger.info("ElapsedHisto:" + elapsedHisto.getCount() + " " + elapsedHisto.toString());
			logger.info("DelayHisto:" + delayHisto.getCount() + " " + delayHisto.toString());
			tc[id].close();

		}

		void newServer(final SelectableChannel channel, int id) throws Exception {
			tc[id].callback = new NIOAcceptorCallback(id);
			tc[id].key = waitStrat.registerSelectableChannel(channel, tc[id].callback);
			tc[id].key.interestOps(SelectionKey.OP_ACCEPT);
			logger.info("Registered for opAccept " + tc[id].key.interestOps() + " chnnal:" + channel + " reg:"
					+ channel.isRegistered() + " blocking:" + channel.isBlocking() + "  open:" + channel.isOpen());

		}

		/**
		 * callback to accept a new connection on the server socket
		 * 
		 * @author ajt
		 *
		 */
		public class NIOAcceptorCallback implements NIOWaitStrategy.SelectorCallback {
			NIOAcceptorCallback(int id2) {
				id = id2;
			}

			int id;

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
					boolean finishConnect = socketChannel.finishConnect();
					socketChannel.configureBlocking(false);

					ecc[id] = new EstablishedConnectionCallback(socketChannel, id);
					ecc[id].key = waitStrat.registerSelectableChannel(socketChannel, ecc[id]);
					ecc[id].key.interestOps(SelectionKey.OP_READ);
					logger.info("opAcccept completed registered:{} finishConnect:", socketChannel.isRegistered(),
							finishConnect);
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
		}

		/**
		 * callback to interact with an established client session
		 * 
		 * @author ajt
		 *
		 */
		public class EstablishedConnectionCallback implements NIOWaitStrategy.SelectorCallback {
			int maxQueueSize = 65536;
			final LinkedList<byte[]> queue = new LinkedList<>();
			boolean currentlyBlocked = false;

			private final SocketChannel socketChannel;
			private final int id;
			private SelectionKey key;
			private long totalRead = 0;
			private long readCount = 0;
			private long signalCount = 0;
			private long totalWriteSocket = 0;
			private long writeSignalCount = 0;
			private long writeCount = 0;

			private final ByteBuffer readBuffer;
			private final ByteBuffer writeBuffer;

			private void close() {
				logger.info("Closing socket:{} totalRead:{} signalCount:{} readiteration:{}", socketChannel, totalRead,
						signalCount, readCount);
				logger.info("ElapsedHisto:" + elapsedHisto.getCount() + " " + elapsedHisto.toString());
				logger.info("DelayHisto:" + delayHisto.getCount() + " " + delayHisto.toString());

				try {
					socketChannel.close();
				} catch (Exception e) {
					logger.info("Error closing connnection " + socketChannel + " key:" + key, e);
				}
				if (key.isValid()) {
					key.cancel();
				}
				ecc[id].close();

			}

			EstablishedConnectionCallback(final SocketChannel sc, int id) {
				this.socketChannel = sc;
				this.id = id;
				readBuffer = ByteBuffer.allocate(4096);
				readBuffer.order(ByteOrder.LITTLE_ENDIAN);
				writeBuffer = ByteBuffer.allocate(4096);
				writeBuffer.order(ByteOrder.LITTLE_ENDIAN);
			}

			long dataLossCount = 0;

			public void handleData(final TestEvent event, final long sequence, final boolean endOfBatch)
					throws Exception {
				final byte[] data = event.data.toString().getBytes();
				if (currentlyBlocked) {
					if (coalsce) {
						// drop data
						logger.trace("Coalse: drop data size:{}", data.length);
					} else {
						if (queue.size() > maxQueueSize) {
							if ((dataLossCount++ & 1023) == 0) {
								logger.error("Data lost: backlog exeeded count:{}", dataLossCount);
							}
						} else {
							queue.add(data);
						}
					}
				} else {
					ecc[event.targetID].writeBuffer.clear();
					ecc[event.targetID].writeBuffer.put(data);
					ecc[event.targetID].writeBuffer.flip();
					final int remaining = ecc[event.targetID].writeBuffer.remaining();
					final int bytesWritten = ecc[event.targetID].socketChannel.write(ecc[event.targetID].writeBuffer);
					if (bytesWritten == -1) {
						close();
					} else {
						if (bytesWritten < remaining) {
							// didnt write everything
							logger.info("Didnt write all - blocking:{}", id);
							currentlyBlocked = true;
							ecc[event.targetID].key.interestOps(SelectionKey.OP_READ + SelectionKey.OP_WRITE);
							ecc[event.targetID].writeBuffer.compact();
							ecc[event.targetID].writeBuffer.flip();
						}
						writeCount++;
						totalWriteSocket += bytesWritten;
					}
				}
			}

			@Override
			public void opRead(final SelectableChannel channel, final long currentTimeNanos) {
				try {
					signalCount++;
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
				try {
					int bytesRemaining = writeBuffer.remaining();
					int bytesWritten = socketChannel.write(writeBuffer);
					logger.debug(" EstablishedConnectionCallback - OPWrite called  - sup size:{} wc:{} sdc:{} writ:{}",
							queue.size(), writeCount, writeSignalCount, bytesWritten);
					writeSignalCount++;
					writeCount++;
					if (bytesWritten == -1) {
						// closed.
						logger.info("closing server in opWrite -1 ret");
						closeServer(id);
					} else {
						totalWriteSocket += bytesWritten;
						if (bytesRemaining == bytesWritten) {
							byte[] data;
							while ((data = queue.poll()) != null) {

								writeBuffer.clear();
								writeBuffer.put(data);
								writeBuffer.flip();
								bytesRemaining = writeBuffer.remaining();
								bytesWritten = socketChannel.write(writeBuffer);
								writeCount++;
								if (bytesWritten == -1) {
									// closed.
									logger.info("closing server in opWrite flush -1 ret");
									closeServer(id);
								} else {
									totalWriteSocket += bytesWritten;
									if (bytesWritten < bytesRemaining) {
										writeBuffer.compact();
										writeBuffer.flip();
										logger.debug(
												"baglog incomplete - socket not caught up size:{} wc:{} sdc:{} writ:{}",
												queue.size(), writeCount, writeSignalCount, bytesWritten);
										return;
									}
								}
							}
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
				} catch (Exception e) {
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

	private static final class TestEvent {
		enum EventType {
			data, newServer, close, UNKNOWN;
		}

		private EventType type = EventType.UNKNOWN;
		private Object data = null;
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

	private static final class TestData {
		int dataType;
		byte[] data;
	}

}