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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Enumeration;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;

import org.hamcrest.Matchers;
import org.hamcrest.core.Is;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ajt.disruptorIO.ConnectionHelper;
import com.ajt.disruptorIO.ConnectionHelper.SenderCallback;
import com.ajt.disruptorIO.ConnectionHelper.SenderCallin;
import com.ajt.disruptorIO.NIOWaitStrategy;
import com.ajt.disruptorIO.SSLTCPSenderHelper;
import com.ajt.disruptorIO.TestEvent;
import com.google.common.io.Resources;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

public class SSLConnectionTest {
	static {
		System.setProperty("org.apache.logging.log4j.simplelog.StatusLogger.level", "TRACE");
		System.setProperty("org.apache.logging.log4j.level", "DEBUG");

	}
	private final static Logger logger = LoggerFactory.getLogger(SSLConnectionTest.class);
	private ExceptionHandler<TestEvent> errorHandler;
	private long sequenceNum = 0;
	private Disruptor<TestEvent> disruptorServer;
	private Disruptor<TestEvent> disruptorClient;
	private NIOWaitStrategy nioWaitStrategyClient;
	private NIOWaitStrategy nioWaitStrategyServer;

	private ServerConnectionHelper[] handlers;
	private ThreadFactory threadFactoryServer;
	private ThreadFactory threadFactoryClient;
	private SSLContext sslContext;

	@Before
	public void setup() throws Exception {

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

	private static SSLContext setupContext(String passwd, String keyStoreFile, String trustStoreFile) throws Exception {
		char[] passphrase = passwd.toCharArray();

		final KeyStore ks = KeyStore.getInstance("JKS");
		final URL keystoreURL = Resources.getResource(keyStoreFile);
		ks.load(keystoreURL.openStream(), passphrase);
		final KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
		kmf.init(ks, passphrase);
		logger.info("KeyManager:{} keyStore:{}", kmf, ks);

		final KeyStore ts = KeyStore.getInstance("JKS");
		final URL truststoreURL = Resources.getResource(trustStoreFile);
		ts.load(truststoreURL.openStream(), passphrase);
		final TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		tmf.init(ts);
		logger.info("TMF:{} ", tmf);
		X509ExtendedTrustManager tmf2 = new X509ExtendedTrustManager() {

			@Override
			public X509Certificate[] getAcceptedIssuers() {

				return ((X509ExtendedTrustManager) tmf.getTrustManagers()[0]).getAcceptedIssuers();
			}

			@Override
			public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
				logger.info("checkServerTrusted X509Certificate authType:" + authType);

			}

			@Override
			public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
				logger.info("checkClientTrusted authType:" + authType);

			}

			@Override
			public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
					throws CertificateException {
				logger.info("checkServerTrusted authType:" + authType + " engine:" + engine);

			}

			@Override
			public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket)
					throws CertificateException {
				logger.info("checkServerTrusted authTYpe:" + authType + " socket:" + socket);

			}

			@Override
			public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
					throws CertificateException {
				logger.info("checkClientTrusted authType:" + authType + " engine:" + engine);

			}

			@Override
			public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket)
					throws CertificateException {
				logger.info("checkServerTrusted");

			}
		};

		final SSLContext sslCtx = SSLContext.getInstance("TLSv1.2");
		// final SSLContext sslCtx = SSLContext.getDefault();

		// sslCtx.init(null, null, null);
		// sslCtx.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new
		// SecureRandom());

		sslCtx.init(kmf.getKeyManagers(), new TrustManager[] { tmf2 }, new SecureRandom());
		return sslCtx;
	}

	@After
	public void teardown() {
		logger.info("Teardown");
		try {
			disruptorServer.shutdown();
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

		try {
			disruptorClient.shutdown();
			nioWaitStrategyClient.close();
		} catch (Exception e) {
			logger.info("Error closing nioWait", e);
		}
		threadFactoryClient = null;
		threadFactoryServer = null;

	}

	// runs on the main thread
	private void testFastServer(final long messagesToSend, //
			final long sendMessageRatePerSecond, //
			final long readRatePerSecond, //
			final long writeRatePerSecond, //
			final int clients, final boolean lossy, final String cipher) throws Exception {
		try {
			logger.info("Disruptor creating new disruptor for this context. toSend:{} rateAt:{}", messagesToSend,
					sendMessageRatePerSecond);

			disruptorServer.start();

			final long maxWaitForBind = System.currentTimeMillis() + 10000;
			while (System.currentTimeMillis() < maxWaitForBind && handlers[0].remoteAddress == null) {
				Thread.sleep(1);
			}
			if (handlers[0].remoteAddress == null) {
				Assert.fail("Error remote address not set for connect to");
			}

			// create a client set using the client disruptor
			final TestEventClient tc = new TestEventClient(clients, readRatePerSecond, writeRatePerSecond,
					(InetSocketAddress) handlers[0].remoteAddress, nioWaitStrategyClient, cipher);
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
				Assert.assertThat("Client not running", tc.isClosed(), Matchers.is(false));
				connected = true;

				for (int a = 0; a < tc.clients.length; a++) {
					if (false == tc.clients[a].isConnected()) {
						connected = false;
					}
				}
				final long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNanos);
				Assert.assertThat("not  connected in time", elapsed, Matchers.lessThan(300L + 400 * tc.clients.length));
			}
			logger.info("All connected");
			while (actualMessageSendCount < messagesToSend) {
				Assert.assertThat(handlers[0].isClosed(), Is.is(false));

				final long currentTimeNanos = System.nanoTime();
				final long elapsed = currentTimeNanos - startTimeNanos;
				// control send rate
				if (elapsed * sendMessageRatePerSecond > actualMessageSendCount * 1000000000L) {

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
						Assert.assertThat(handlers[0].isClosed(), Is.is(false));

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
				if (handlers[0].counter.get() == messagesToSend) {
					logger.info("completed :{}", messagesToSend);
					break;
				}
			}

			assertThat("Message count did not all get delivered by disruptor to client, slow or blocked client ? ",
					handlers[0].counter.get(), Matchers.is(messagesToSend));
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
				final long readRatePerSecond, //
				final long writeRatePerSecond, //
				final InetSocketAddress sa, //
				final NIOWaitStrategy nioWait, //
				final String cipher) throws Exception {
			this.sa = sa;
			this.count = count;
			this.writeRatePerSecond = writeRatePerSecond;
			this.readRatePerSecond = readRatePerSecond;

			SSLParameters sslP = SSLContext.getDefault().getSupportedSSLParameters();
			if (cipher != null) {
				sslP.setCipherSuites(new String[] { cipher });
			}

			helper = new SSLTCPSenderHelper(nioWait,
					setupContext("password", "resources/client.jks", "resources/client.truststore"), sslP);
			clients = new ClientConnectionHelper[count];
			for (int a = 0; a < count; a++) {
				clients[a] = new ClientConnectionHelper(writeRatePerSecond, a, sa, nioWait);
			}
		}

		@Override
		public void onEvent(TestEvent event, long sequence, boolean endOfBatch) throws Exception {
			logger.warn("Not exepecting callback");
		}

		boolean isClosed() {
			boolean isClosed = false;
			if (isRunning == false)
				isClosed = true;
			for (int a = 0; a < clients.length; a++) {
				if (clients[a].isClosed) {
					isClosed = true;
				}
			}
			return isClosed;
		}

		void start() {
			try {
				isRunning = true;

				logger.info("Start");
				for (int a = 0; a < count; a++) {

					helper.connectTo(sa, clients[a]);

				}
			} catch (Exception e) {
				logger.error("Error during start", e);
				Assert.fail("fail during start;" + e);
				close();
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

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testSSLConnectionTimeout() throws Exception {
		sslContext = setupContext("password", "resources/client.jks", "resources/client.truststore");

		final SSLTCPSenderHelper sslTCP = new SSLTCPSenderHelper(nioWaitStrategyServer, sslContext, null);
		ConnectionHelper.SenderCallin callin = sslTCP.bindTo(null, new SenderCallback() {
			@Override
			public void writeUnblocked(SenderCallin callin) {
			}

			@Override
			public void writeNowBlocked(SenderCallin callin) {
			}

			@Override
			public void readData(SenderCallin callin, ByteBuffer buffer) {
			}

			@Override
			public void connected(SenderCallin callin) {
			}

			@Override
			public void closed(SenderCallin callin) {
			}
		});
		disruptorServer.handleEventsWith(new EventHandler() {

			@Override
			public void onEvent(Object event, long sequence, boolean endOfBatch) throws Exception {

			}

		});
		disruptorServer.start();
		SocketChannel sc = SocketChannel.open(callin.getRemoteAddress());
		sc.configureBlocking(false);
		ByteBuffer bb = ByteBuffer.wrap(new byte[128]);

		Assert.assertThat("not connected " + sc, sc.isConnected(), Is.is(true));
		final long timeoutError = System.currentTimeMillis() + SSLTCPSenderHelper.SSL_TIMEOUT + 50;
		while (System.currentTimeMillis() < timeoutError) {
			try {
				bb.clear();
				final int read = sc.read(bb);
				if (read == -1) {
					// closed
					return;
				}
			} catch (Exception e) {
				logger.info("socket closed");
				return;
			}
			Thread.sleep(50);
			logger.info(
					" checking socket closed:" + sc.isConnectionPending() + " " + sc.isOpen() + " " + sc.isConnected());

		}
		Assert.fail("Error socket not closed in time");
		// Assert.assertThat("not closed " + s + " " + s.isClosed(), s.isConnected(),
		// is(false));

	}

	@Test
	public void testServerConnectionRatePerCipher() throws Exception {
		sslContext = setupContext("password", "resources/client.jks", "resources/client.truststore");

		final long toSend = 1000_000L;
		final long messageratePerSecond = 1_000_000L; // high
		final long readRatePerSecond = 1_000_000_000L; // high
		final long writeRatePerSecond = 1_000L; //
		final int[] clientCount = new int[] { 1, 2, 4, 8, 16 };
		final boolean lossy = true;
		String cph = "";
		// cph="TLS_ECDHE_ECDSA_WITH_3DES_EDE_CBC_SHA";
		// cph = null;
		final SSLParameters sslP = SSLContext.getDefault().getSupportedSSLParameters();

		final String[] suites = sslP.getCipherSuites();
		InetSocketAddress[] address = new InetSocketAddress[] { null, null };

		Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
		{
			while (interfaces.hasMoreElements()) {
				NetworkInterface nic = interfaces.nextElement();
				Enumeration<InetAddress> addresses = nic.getInetAddresses();
				while (address == null && addresses.hasMoreElements()) {
					InetAddress address2 = addresses.nextElement();
					if (!address2.isLoopbackAddress()) {
						address[1] = new InetSocketAddress(address2.getHostName(), 0);
					}
				}
			}
		}
		for (int c = 0; c < address.length; c++) {
			for (int b = 0; b < clientCount.length; b++) {
				for (int a = 0; a < suites.length; a++) {
					cph = suites[a];
					try {
						setup();
						long start = System.currentTimeMillis();
						logger.info("TRYING CIPHER:{}", cph);
						sslP.setCipherSuites(new String[] { cph });
						sslP.setUseCipherSuitesOrder(true);
						SSLTCPSenderHelper sslTCP = new SSLTCPSenderHelper(nioWaitStrategyServer, sslContext, sslP);
						handlers = new ServerConnectionHelper[] {
								new ServerConnectionHelper(sslTCP, lossy, address[c], clientCount[b]) };
						disruptorServer.handleEventsWith(handlers);
						testFastServer(toSend, messageratePerSecond, readRatePerSecond, writeRatePerSecond,
								clientCount[b], lossy, cph);
						long finish = System.currentTimeMillis();
						logger.info("SUCCESS took:" + (finish - start) + " cph:" + cph);
					} catch (AssertionError | IllegalStateException ae) {
						logger.error("ERROR FAILED " + cph, ae);
					} finally {
						teardown();
					}
				}
				setup();
			}
		}

	}

}