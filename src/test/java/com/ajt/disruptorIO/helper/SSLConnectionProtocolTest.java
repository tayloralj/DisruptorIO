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

import java.net.InetSocketAddress;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.hamcrest.Matchers;
import org.hamcrest.core.Is;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ajt.disruptorIO.ConnectionHelper;
import com.ajt.disruptorIO.NIOWaitStrategy;
import com.ajt.disruptorIO.SSLTCPSenderHelper;
import com.ajt.disruptorIO.TestEvent;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

public class SSLConnectionProtocolTest {
	static {
		System.setProperty("org.apache.logging.log4j.simplelog.StatusLogger.level", "TRACE");
		System.setProperty("org.apache.logging.log4j.level", "DEBUG");

	}
	private final Logger logger = LoggerFactory.getLogger(SSLConnectionProtocolTest.class);
	private ExceptionHandler<TestEvent> errorHandler;
	private long sequenceNum = 0;
	private Disruptor<TestEvent> disruptorServer;
	private Disruptor<TestEvent> disruptorClient;
	private NIOWaitStrategy nioWaitStrategyClient;
	private NIOWaitStrategy nioWaitStrategyServer;

	private ServerConnectionHelper[] handlers;
	private ThreadFactory threadFactoryServer;
	private ThreadFactory threadFactoryClient;

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
		logger.info("Teardown");
		disruptorServer.shutdown();
		try {
			nioWaitStrategyServer.close();
		} catch (Exception e) {
			logger.info("Error closing nioWait", e);
		}
		if (handlers != null) {
			for (int a = 0; a < handlers.length; a++) {
				if (handlers[a] != null) {
					handlers[a].close();
				}
				handlers[a] = null;
			}
			handlers = null;
		}
		threadFactoryServer = null;

		disruptorClient.shutdown();
		try {
			nioWaitStrategyClient.close();
		} catch (Exception e) {
			logger.info("Error closing nioWait", e);
		}
		threadFactoryClient = null;

	}

	// runs on the main thread
	private void testFastServer(final long toSend, final long messageratePerSecond, final long readRatePerSecond,
			final long writeRatePerSecond, final int clients, final boolean lossy) throws Exception {
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
			final long startTimeNanos = System.nanoTime() - 1;
			int b = 0;
			final RingBuffer<TestEvent> rb = disruptorServer.getRingBuffer();
			boolean connected = false;
			while (!connected) {
				connected = true;
				int connectedCount = 0;
				for (int a = 0; a < tc.clients.length; a++) {
					if (false == tc.clients[a].isConnected()) {
						connected = false;
					} else {
						connectedCount++;
					}
				}
				final long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNanos);
				Assert.assertThat("not  connected in time  connected:" + connectedCount, elapsed,
						Matchers.lessThan(300L + 200 * tc.clients.length));
			}
			logger.info("All connected");
			while (actualMessageSendCount < toSend) {
				Assert.assertThat(handlers[0].isClosed(), Is.is(false));

				final long currentTimeNanos = System.nanoTime();
				final long elapsed = currentTimeNanos - startTimeNanos;
				// control send rate
				if (elapsed * messageratePerSecond > actualMessageSendCount * 1000000000L) {

					try {
						if (lossy) {
							sequenceNum = rb.next(1);
						} else {
							sequenceNum = rb.next(1);
						}

						final TestEvent te = rb.get(sequenceNum);
						te.type = TestEvent.EventType.data;
						te.targetID = b;
						TestEvent.putLongToArray(byteDataToSend, TestEvent.Offsets.time, System.nanoTime());
						TestEvent.putLongToArray(byteDataToSend, TestEvent.Offsets.seqnum, sequenceNum);

						te.setData(byteDataToSend, 0, byteDataToSend.length, TestEvent.MessageType.dataFromServerEvent);
						te.nanoSendTime = System.nanoTime();
						rb.publish(sequenceNum);
						actualMessageSendCount++;
						Assert.assertThat("Error is closed, should be open" + handlers[0].toString(),
								handlers[0].isClosed(), Is.is(false));

					} catch (Exception ice) {
						// land here if a lossy client
					} finally {
						// move onto next client
						b++;
						if (b >= clients) {
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
			final long endTime = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(1000);
			while (System.nanoTime() < endTime) {
				if (handlers[0].counter.get() == toSend) {
					logger.info("completed :{}", toSend);
					break;
				}
			}

			assertThat("Message count did not all get delivered by disruptor to client, slow or blocked client ? ",
					handlers[0].counter.get(), Matchers.is(toSend));
			nioWaitStrategyClient.getScheduledExecutor().execute(() -> {
				logger.info("Closing client");
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
		final Logger logger = LoggerFactory.getLogger(TestEventClient.class);
		final int count;
		final long writeRatePerSecond;
		final long readRatePerSecond;
		final InetSocketAddress sa;
		final ClientConnectionHelper[] clients;
		final ConnectionHelper helper;

		boolean isRunning = false;
		long _startTimeNanos = 0;
		long _currentTimeNanos = 0;

		public TestEventClient(final int count, //
				final long writeRatePerSecond, //
				final long readRatePerSecond, //
				final InetSocketAddress sa, //
				final NIOWaitStrategy nioWait) throws Exception {
			this.sa = sa;
			this.count = count;
			this.writeRatePerSecond = writeRatePerSecond;
			this.readRatePerSecond = readRatePerSecond;
			helper = new SSLTCPSenderHelper(nioWait, "resources/client.jks", "resources/client.truststore", "password",
					false);
			clients = new ClientConnectionHelper[count];
			for (int a = 0; a < count; a++) {
				clients[a] = new ClientConnectionHelper(writeRatePerSecond, a, sa, nioWait);
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
			logger.info("Closing all clients");
			isRunning = false;
			for (int c = 0; c < clients.length; c++) {
				if (clients[c] != null) {

					logger.debug(
							"{}:  Client  runningFor(ms):{} closed:{} MsgCount:{} ReadMBYTE:{} readByteRate(mbit):{} readCycle:{} readCount:{} writeAtLeast:{} writtenBytes:{} ",
							c, // c
							(_currentTimeNanos - _startTimeNanos) / 1000000, // runfor
							clients[c].isClosed, //
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

	}

	@Test
	public void testServerConnectionFull() throws Exception {
		final long toSend = 20_000_000L;
		final long messageratePerSecond = 200_000_000L;
		final long readRatePerSecond = 1_000000L;
		final long writeRatePerSecond = 1_000000L;
		final int clientCount = 2;
		final boolean lossy = false;
		handlers = new ServerConnectionHelper[] {
				new ServerConnectionHelper(new SSLTCPSenderHelper(nioWaitStrategyServer, "resources/client.jks",
						"resources/client.truststore", "password", true), lossy, null, clientCount) };
		disruptorServer.handleEventsWith(handlers);
		testFastServer(toSend, messageratePerSecond, readRatePerSecond, writeRatePerSecond, clientCount, lossy);
	}

	@Test
	public void testServerConnectionLossy() throws Exception {
		final long toSend = 20_000_000L;
		final long messageratePerSecond = 1_000_000;
		final long readRatePerSecond = 1_000_000_000L;
		final long writeRatePerSecond = 1_000L;
		final int clientCount = 1;
		final boolean lossy = true;
		handlers = new ServerConnectionHelper[] {
				new ServerConnectionHelper(new SSLTCPSenderHelper(nioWaitStrategyServer, "resources/client.jks",
						"resources/client.truststore", "password", true), lossy, null, clientCount) };
		disruptorServer.handleEventsWith(handlers);
		testFastServer(toSend, messageratePerSecond, readRatePerSecond, writeRatePerSecond, clientCount, lossy);
	}

	@Test
	public void testServerConnection10Full() throws Exception {
		final long toSend = 10_000_000L;
		final long messageratePerSecond = 100_000L;
		final long readRatePerSecond = 1_000_000_000L;
		final long writeRatePerSecond = 1_000L;
		final int clientCount = 2;
		final boolean lossy = false;
		handlers = new ServerConnectionHelper[] {
				new ServerConnectionHelper(new SSLTCPSenderHelper(nioWaitStrategyServer, "resources/client.jks",
						"resources/client.truststore", "password", false), lossy, null, clientCount) };
		disruptorServer.handleEventsWith(handlers);
		testFastServer(toSend, messageratePerSecond, readRatePerSecond, writeRatePerSecond, clientCount, lossy);
	}

	@Test
	public void testServerConnection10Lossy() throws Exception {
		final long toSend = 10_000_000L;
		final long messageratePerSecond = 500_000L;
		final long readRatePerSecond = 1_000_000L;
		final long writeRatePerSecond = 1_000L;
		final int clientCount = 2;
		final boolean lossy = true;
		handlers = new ServerConnectionHelper[] {
				new ServerConnectionHelper(new SSLTCPSenderHelper(nioWaitStrategyServer, "resources/client.jks",
						"resources/client.truststore", "password", false), lossy, null, clientCount) };
		disruptorServer.handleEventsWith(handlers);
		testFastServer(toSend, messageratePerSecond, readRatePerSecond, writeRatePerSecond, clientCount, lossy);
	}

	@Test
	public void testServerConnection10_20Full() throws Exception {
		final long toSend = 10_000_000L;
		final long messageratePerSecond = 500_000L;
		final long readRatePerSecond = 1_000000L;
		final long writeRatePerSecond = 1_000L;
		final int clientCount = 2;
		final boolean lossy = false;
		handlers = new ServerConnectionHelper[] {
				new ServerConnectionHelper(new SSLTCPSenderHelper(nioWaitStrategyServer, "resources/client.jks",
						"resources/client.truststore", "password", false), lossy, null, clientCount) };
		disruptorServer.handleEventsWith(handlers);
		testFastServer(toSend, messageratePerSecond, readRatePerSecond, writeRatePerSecond, clientCount, lossy);
	}

	@Test
	public void testServerConnection10_20Lossy() throws Exception {
		final long toSend = 10_000_000L;
		final long messageratePerSecond = 5000_000L;
		final long readRatePerSecond = 1_00000_000L;
		final long writeRatePerSecond = 1_000L;
		final int clientCount = 2;
		final boolean lossy = true;
		handlers = new ServerConnectionHelper[] {
				new ServerConnectionHelper(new SSLTCPSenderHelper(nioWaitStrategyServer, "resources/client.jks",
						"resources/client.truststore", "password", false), lossy, null, clientCount) };
		disruptorServer.handleEventsWith(handlers);
		testFastServer(toSend, messageratePerSecond, readRatePerSecond, writeRatePerSecond, clientCount, lossy);
	}

}