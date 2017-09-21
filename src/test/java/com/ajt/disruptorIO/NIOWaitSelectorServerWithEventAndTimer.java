package com.ajt.disruptorIO;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

public class NIOWaitSelectorServerWithEventAndTimer {
	static {
		System.setProperty("org.apache.logging.log4j.simplelog.StatusLogger.level", "TRACE");
		System.setProperty("org.apache.logging.log4j.level", "DEBUG");

	}
	private final Logger logger = LoggerFactory.getLogger(NIOWaitSelectorServerWithEventAndTimer.class);
	private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool();
	private NIOWaitStrategy.NIOClock clock;
	private ExceptionHandler<TestEvent> errorHandler;
	private long sequenceNum = 0;

	@Before
	public void setup() {
		clock = NIOWaitStrategy.getDefaultClock();
		errorHandler = new ExceptionHandler<NIOWaitSelectorServerWithEventAndTimer.TestEvent>() {

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
	}

	@Test
	public void ServerConnection() throws Exception {

		logger.trace("[{}] AsyncLoggerDisruptor creating new disruptor for this context.", "test");
		int ringBufferSize = 2048;

		try (final NIOWaitStrategy nioWaitStrategy = new NIOWaitStrategy(clock);
				final ServerSocketChannel socketChannel = ServerSocketChannel.open();
			final Socket s = new Socket();) {
			final ThreadFactory threadFactory = new ThreadFactory() {

				@Override
				public Thread newThread(Runnable r) {
					final Thread th = new Thread(r, "WaStratThread");

					return th;
				}
			};

			final Disruptor<TestEvent> disruptor = new Disruptor<>(TestEvent.EVENT_FACTORY, ringBufferSize,
					threadFactory, ProducerType.SINGLE, nioWaitStrategy);
			final TestEventHandler[] handlers = { new TestEventHandler(nioWaitStrategy) };
			final LatencyTimer lt = new LatencyTimer();

			disruptor.setDefaultExceptionHandler(errorHandler);

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

			try {
				sequenceNum = rb.next();
				final TestEvent te = rb.get(sequenceNum);
				te.type = TestEvent.EventType.newServer;
				te.data = socketChannel;
			} finally {
				rb.publish(sequenceNum);
			}
			Thread.sleep(50);
			final SocketAddress sa = socketChannel.getLocalAddress();
			logger.info("local connection to :{}", sa);
			s.connect(sa, 1000);
			s.setTcpNoDelay(false);
			InputStream is = s.getInputStream();
			OutputStream os = s.getOutputStream();
			byte[] writeBytes = "Hello worldHello worldHello worldHello worldHello worldHello worldHelloworldHello worldHello worldHello world"
					.getBytes();
			final long start = System.currentTimeMillis();
			final long toSend = 100_000_000L;
			long bytesWritten = 0;
			for (long a = 0; a < toSend; a++) {
				try {
					sequenceNum = rb.next();

					final TestEvent te = rb.get(sequenceNum);
					te.type = TestEvent.EventType.data;
				} finally {
					rb.publish(sequenceNum);
				}
				if (Math.random() > 0.9) {
					os.write(writeBytes);
					bytesWritten += writeBytes.length;
					// Thread.sleep(1);
				}

			}
			s.close();

			final long endTime = System.currentTimeMillis() + 10000L;
			while (System.currentTimeMillis() < endTime) {
				if (handlers[0].counter.get() == toSend + 1) {
					logger.info("completed :{}", toSend);

					break;
				}
			}
			lt.stop();
			Thread.sleep(10);
			assertThat(handlers[0].counter.get(), is(toSend + 1));
			long end = System.currentTimeMillis();
			logger.info("Took(ms):{} DataSend:{} rate:{} MBwritten:{} rate:{}", (end - start),
					handlers[0].counter.get(), handlers[0].counter.get() * 1000 / (end - start), bytesWritten / 1000000,
					bytesWritten / (1000 * end - 1000 * start));
			disruptor.shutdown();
		}
	}

	private class TestEventHandler implements EventHandler<TestEvent> {
		private final Logger logger = LoggerFactory.getLogger(TestEventHandler.class);
		private final AtomicLong counter = new AtomicLong();
		private final NIOWaitStrategy waitStrat;
		private final NIOCallback callback;
		private final HashSet<EstablishedConnectionCallback> socketChannelSet = new HashSet<>();
		private final ByteBuffer writeMessage;

		public TestEventHandler(NIOWaitStrategy waiter) {
			waitStrat = waiter;
			callback = new NIOCallback();
			writeMessage = ByteBuffer.wrap("HelloWorld\n".getBytes());
			writeMessage.flip();
		}

		@Override
		public void onEvent(final TestEvent event, final long sequence, final boolean endOfBatch) throws Exception {
			switch (event.type) {
			case data:
				handleData(event, sequence, endOfBatch);
				break;
			case newServer:
				newServer(event, sequence, endOfBatch);
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

		}

		void handleData(final TestEvent event, final long sequence, final boolean endOfBatch) throws Exception {

		}

		void closeServer(final TestEvent event, final long sequence, final boolean endOfBatch) throws Exception {

		}

		void newServer(final TestEvent event, final long sequence, final boolean endOfBatch) throws Exception {
			final SelectableChannel channel = (SelectableChannel) event.data;

			final SelectionKey key = waitStrat.registerSelectableChannel(channel, callback);
			key.interestOps(SelectionKey.OP_ACCEPT);
			logger.info("Registered for opAccept " + key.interestOps() + " chnnal:" + channel + " reg:"
					+ channel.isRegistered() + " blocking:" + channel.isBlocking() + "  open:" + channel.isOpen());

		}

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
				// TODO Auto-generated method stub

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

		private volatile EventType type = EventType.UNKNOWN;
		private Object data = null;
		private final long seqNum;
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

	/**
	 * Returns the thread ID of the background appender thread. This allows us to
	 * detect Logger.log() calls initiated from the appender thread, which may cause
	 * deadlock when the RingBuffer is full. (LOG4J2-471)
	 *
	 * @param executor
	 *            runs the appender thread
	 * @return the thread ID of the background appender thread
	 */
	public static long getExecutorThreadId(final ExecutorService executor) {
		final Future<Long> result = executor.submit(new Callable<Long>() {
			@Override
			public Long call() {
				return Thread.currentThread().getId();
			}
		});
		try {
			return result.get();
		} catch (final Exception ex) {
			final String msg = "Could not obtain executor thread Id. "
					+ "Giving up to avoid the risk of application deadlock.";
			throw new IllegalStateException(msg, ex);
		}
	}
}