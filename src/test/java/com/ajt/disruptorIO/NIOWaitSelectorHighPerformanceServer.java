package com.ajt.disruptorIO;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ajt.disruptorIO.LatencyTimer;
import com.ajt.disruptorIO.NIOWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.collections.Histogram;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import junit.extensions.RepeatedTest;

public class NIOWaitSelectorHighPerformanceServer {
	static {
		System.setProperty("org.apache.logging.log4j.simplelog.StatusLogger.level", "TRACE");
		System.setProperty("org.apache.logging.log4j.level", "DEBUG");

	}
	private final Logger logger = LoggerFactory.getLogger(NIOWaitSelectorHighPerformanceServer.class);
	private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool();
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
		final long messageratePerSecond = 10000_000L;
		final long readRatePerSecond = 1000_1000L;
		final long writeRatePerSecond = 1000_000L;
		logger.info("Disruptor creating new disruptor for this context. toSend:{} rateAt:{}", toSend,
				messageratePerSecond);

		try (final ServerSocketChannel socketChannel = ServerSocketChannel.open();
				final ServerSocket serverSocket = new ServerSocket(0);
				final Socket s = new Socket();) {

			final TestEventHandler[] handlers = { new TestEventHandler(nioWaitStrategy, false) };

			disruptor.handleEventsWith(handlers);

			logger.debug(
					"Starting AsyncLogger disruptor for this context with ringbufferSize={}, waitStrategy={}, "
							+ "exceptionHandler={}...",
					disruptor.getRingBuffer().getBufferSize(), nioWaitStrategy.getClass().getSimpleName(),
					errorHandler);
			disruptor.start();
			lt.register(nioWaitStrategy);

			final RingBuffer<TestEvent> rb = disruptor.getRingBuffer();

			socketChannel.configureBlocking(false);
			socketChannel.bind(null, 0);
			// call method on correct thread.
			nioWaitStrategy.getScheduledExecutor().execute(new Runnable() {

				@Override
				public void run() {
					try {
						handlers[0].newServer(socketChannel);
					} catch (Exception e) {
						logger.error("Error starting server", e);
					}
				}
			});

			Thread.sleep(50);
			final SocketAddress sa = socketChannel.getLocalAddress();
			logger.info("local connection to :{}", sa);
			s.connect(sa, 1000);
			s.setTcpNoDelay(false);
			InputStream is = s.getInputStream();
			OutputStream os = s.getOutputStream();

			final long startTimeNanos = System.nanoTime();

			AtomicLong bytesWritten = new AtomicLong(0);
			AtomicLong bytesRead = new AtomicLong(0);
			AtomicBoolean isRunning = new AtomicBoolean(true);
			AtomicLong readShouldBeAtLeast = new AtomicLong();
			AtomicLong writeShouldBeAtLeast = new AtomicLong();
			// blocking - force rates.
			Thread readThread = new Thread(new Runnable() {

				@Override
				public void run() {
					try {
						final byte[] readArray = new byte[1024];
						while (isRunning.get()) {
							final long currentTimeNanos = System.nanoTime();
							readShouldBeAtLeast
									.set((readRatePerSecond * 1000000000L) / (currentTimeNanos - startTimeNanos));
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
					} catch (Exception e) {
						logger.error("Error in read:", e);
					}
				}
			});
			readThread.start();

			Thread writeThread = new Thread(new Runnable() {

				@Override
				public void run() {
					try {
						byte[] writeBytes = "Hello worldHello worldHello worldHello worldHello worldHello worldHelloworldHello worldHello worldHello world"
								.getBytes();
						while (isRunning.get()) {
							final long currentTimeNanos = System.nanoTime();
							writeShouldBeAtLeast.set((writeRatePerSecond* (currentTimeNanos  - startTimeNanos  ))/1000000000L);
							if (bytesWritten.get() < writeShouldBeAtLeast.get()) {

								os.write(writeBytes);

								bytesWritten.addAndGet(writeBytes.length);
							} else {
								Thread.sleep(1);
							}

						}
					} catch (Exception e) {
						logger.error("Error in read:", e);
					}
				}
			});
			writeThread.start();
			long a = 0;

			while (a < toSend) {
				final long currentTimeNanos = System.nanoTime();
				final long timeShouldBeAtLeast = (a * 1000000000) / messageratePerSecond + startTimeNanos;
				if (currentTimeNanos > timeShouldBeAtLeast) {

					try {

						sequenceNum = rb.tryNext(1);

						final TestEvent te = rb.get(sequenceNum);
						te.type = TestEvent.EventType.data;
						te.nanoSendTime = System.nanoTime();
						rb.publish(sequenceNum);
						a++;
					} catch (InsufficientCapacityException ice) {
					} finally {

					}

				} else {
					Thread.sleep(1);
				}

				if ((a & 65535) == 0) {
					logger.debug(
							"Pause elapsed:{} diff:{} sentMsg:{}  readAtLeast:{} bytesRead:{} writeAtLeast:{} writtenBytes:{} ",
							(currentTimeNanos - startTimeNanos) / 1000000, currentTimeNanos - timeShouldBeAtLeast, a,
							readShouldBeAtLeast.get(), bytesRead, writeShouldBeAtLeast.get(), bytesWritten);
				}

			}
			s.close();
			isRunning.set(false);
			final long endTime = System.nanoTime() + 10000000000L;
			while (System.nanoTime() < endTime) {
				if (handlers[0].counter.get() == toSend) {
					logger.info("completed :{}", toSend);

					break;
				}
			}
			lt.stop();
			Thread.sleep(10);
			assertThat(handlers[0].counter.get(), is(toSend + 1));
			long end = System.nanoTime();
			logger.info("Took(ms):{} DataSend:{} rate:{} MBwritten:{} rate:{}", (end - startTimeNanos),
					handlers[0].counter.get(), handlers[0].counter.get() * 1000 / (end - startTimeNanos),
					bytesWritten.get() / 1000000, bytesWritten.get() / (1000 * end - 1000 * startTimeNanos));
		}
	}

	private class TestEventHandler implements EventHandler<TestEvent> {
		private final Logger logger = LoggerFactory.getLogger(TestEventHandler.class);
		private final AtomicLong counter = new AtomicLong();
		private final NIOWaitStrategy waitStrat;
		private final NIOCallback callback;
		private final HashSet<EstablishedConnectionCallback> socketChannelSet = new HashSet<>();
		private final ByteBuffer writeMessage;
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
			callback = new NIOCallback();
			writeMessage = ByteBuffer.wrap("HelloWorld\n".getBytes());
			writeMessage.flip();
		}

		@Override
		public void onEvent(final TestEvent event, final long sequence, final boolean endOfBatch) throws Exception {
			final long now = System.nanoTime();
			switch (event.type) {
			case data:
				handleData(event, sequence, endOfBatch);
				break;

			case close:
				closeServer(event, sequence, endOfBatch);
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

		}

		void closeServer(final TestEvent event, final long sequence, final boolean endOfBatch) throws Exception {
			logger.info("closeServer");
			logger.info("ElapsedHisto:" + elapsedHisto.getCount() + " " + elapsedHisto.toString());
			logger.info("DelayHisto:" + delayHisto.getCount() + " " + delayHisto.toString());
			Iterator<EstablishedConnectionCallback> i = socketChannelSet.iterator();
			while (i.hasNext()) {
				EstablishedConnectionCallback ecc = i.next();
				ecc.close();
			}

		}

		void newServer(final SelectableChannel channel) throws Exception {

			final SelectionKey key = waitStrat.registerSelectableChannel(channel, callback);
			key.interestOps(SelectionKey.OP_ACCEPT);
			logger.info("Registered for opAccept " + key.interestOps() + " chnnal:" + channel + " reg:"
					+ channel.isRegistered() + " blocking:" + channel.isBlocking() + "  open:" + channel.isOpen());

		}

		/**
		 * callback to accept a new connection on the server socket
		 * 
		 * @author ajt
		 *
		 */
		private class NIOCallback implements NIOWaitStrategy.SelectorCallback {

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
					socketChannel.configureBlocking(false);
					int remaining = writeMessage.remaining();
					int written = socketChannel.write(writeMessage);
					if (written != remaining) {
						throw new RuntimeException("Error full buffer written:" + written + " " + remaining);
					}
					writeMessage.position(0).limit(remaining);
					final EstablishedConnectionCallback ecc = new EstablishedConnectionCallback(socketChannel);
					ecc.key = waitStrat.registerSelectableChannel(socketChannel, ecc);
					ecc.key.interestOps(SelectionKey.OP_READ);
					socketChannelSet.add(ecc);
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
				logger.info("opWrite");

			}
		}

		/**
		 * callback to interact with an established client session
		 * 
		 * @author ajt
		 *
		 */
		private class EstablishedConnectionCallback implements NIOWaitStrategy.SelectorCallback {
			private final SocketChannel socketChannel;
			private SelectionKey key;
			private long totalRead = 0;
			private long readCount = 0;
			private long signalCount = 0;
			private final ByteBuffer readBuffer;

			private void close() {
				logger.info("Closing socket:{} totalRead:{} signalCount:{} readiteration:{}", socketChannel, totalRead,
						signalCount, readCount);
				logger.info("ElapsedHisto:" + elapsedHisto.getCount() + " " + elapsedHisto.toString());
				logger.info("DelayHisto:" + delayHisto.getCount() + " " + delayHisto.toString());
				socketChannelSet.remove(this);
				try {
					socketChannel.close();
				} catch (Exception e) {
					logger.info("Error closing connnection " + socketChannel + " key:" + key, e);
				}
				if (key.isValid()) {
					key.cancel();
				}

			}

			EstablishedConnectionCallback(final SocketChannel sc) {
				this.socketChannel = sc;

				readBuffer = ByteBuffer.allocate(4096);
				readBuffer.order(ByteOrder.LITTLE_ENDIAN);
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
			public void opWrite(SelectableChannel channel, long currentTimeNanos) {

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