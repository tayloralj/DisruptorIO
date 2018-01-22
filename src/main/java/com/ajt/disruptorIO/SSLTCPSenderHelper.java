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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableEntryException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ajt.disruptorIO.NIOWaitStrategy.SelectorCallback;
import com.ajt.disruptorIO.NIOWaitStrategy.TimerCallback;
import com.ajt.disruptorIO.NIOWaitStrategy.TimerHandler;
import com.google.common.io.Resources;

/**
 * help to create tls connections uses the java ssl engine, and wraps/unwraps
 * the data so it can be sent through the single threaded api.
 * 
 * @author ajt
 *
 */
public class SSLTCPSenderHelper implements ConnectionHelper {
	protected final Logger logger = LoggerFactory.getLogger(SSLTCPSenderHelper.class);
	protected NIOWaitStrategy wait;
	private static int count = 0;

	public static boolean recordStats = false;

	private final SSLContext sslc;
	private final SSLParameters sslParameters;

	public String toString() {
		return "SSLTCPSender helper count:" + count + " sslContext:" + sslc + " param:" + sslParameters;
	}

	/**
	 * use existing context
	 * 
	 * @param context
	 */
	public SSLTCPSenderHelper(//
			final NIOWaitStrategy waiter, //
			final SSLContext context, //
			final SSLParameters params) {
		this.wait = waiter;
		sslc = context;
		sslParameters = params;
	}

	/**
	 * create a connection and pass in the tls connection details for use.
	 * 
	 * @param waiter
	 * @param keyStoreFile
	 * @param trustStoreFile
	 * @param passwd
	 * @param debug
	 * @throws KeyStoreException
	 * @throws CertificateException
	 * @throws NoSuchAlgorithmException
	 * @throws IOException
	 * @throws UnrecoverableEntryException
	 * @throws KeyManagementException
	 */
	public SSLTCPSenderHelper(//
			final NIOWaitStrategy waiter, //
			final String keyStoreFile, //
			final String trustStoreFile, //
			final String passwd, //
			final boolean debug) //
			throws KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException,
			UnrecoverableEntryException, KeyManagementException {
		this.wait = waiter;
		sslParameters = null;

		if (debug) {
			System.setProperty("javax.net.debug", "all");
		}
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

		sslc = sslCtx;

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ajt.disruptorIO.ConnectionHelper#connectTo(java.net.InetSocketAddress,
	 * com.ajt.disruptorIO.SenderCallback)
	 */
	@Override
	public ConnectionHelper.SenderCallin connectTo(final InetSocketAddress remote, final SenderCallback callback)
			throws IOException {

		final SocketChannel sc = SocketChannel.open();
		sc.configureBlocking(false);
		sc.connect(remote);
		logger.info("connectTo:{}", remote);

		final SSLEngine sslEngine = sslc.createSSLEngine();
		sslEngine.setUseClientMode(true);
		if (sslParameters != null) {
			sslEngine.setSSLParameters(sslParameters);
		}
		if (logger.isTraceEnabled()) {
			for (int a = 0; a < sslEngine.getEnabledCipherSuites().length; a++) {
				logger.trace("Enabled client:{}", sslEngine.getEnabledCipherSuites()[a]);
			}
			for (int a = 0; a < sslEngine.getSupportedCipherSuites().length; a++) {
				logger.trace("getSupportedCipherSuites client:{}", sslEngine.getSupportedCipherSuites()[a]);
			}
		}
		final IOHandler IOh = new IOHandler(sc, callback, sslEngine);
		return IOh;

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ajt.disruptorIO.ConnectionHelper#bindTo(java.net.InetSocketAddress,
	 * com.ajt.disruptorIO.SenderCallback)
	 */
	@Override
	public ConnectionHelper.SenderCallin bindTo(final InetSocketAddress local, final SenderCallback callback)
			throws IOException {
		final ServerSocketChannel socketChannel = ServerSocketChannel.open();
		socketChannel.configureBlocking(false);
		socketChannel.bind(local, 0);

		logger.info("bindTo:{} channel:{}", local, socketChannel);
		final SSLEngine sslEngine = sslc.createSSLEngine();
		sslEngine.setWantClientAuth(true);
		sslEngine.setUseClientMode(false);
		if (sslParameters != null) {
			sslEngine.setSSLParameters(sslParameters);
		}
		if (logger.isTraceEnabled()) {
			for (int a = 0; a < sslEngine.getEnabledCipherSuites().length; a++) {
				logger.trace("Enabled server:{}", sslEngine.getEnabledCipherSuites()[a]);
			}
			for (int a = 0; a < sslEngine.getSupportedCipherSuites().length; a++) {
				logger.trace("getSupportedCipherSuites server:{}", sslEngine.getSupportedCipherSuites()[a]);
			}
		}

		final IOHandler IOh = new IOHandler(socketChannel, callback, sslEngine);

		return IOh;

	}

	private class IOHandler implements SelectorCallback, ConnectionHelper.SenderCallin {
		private ServerSocketChannel serverSocketChannel = null;
		private SocketChannel socketChannel = null;
		private SelectionKey sk = null;
		private final SenderCallback callback;
		private SocketAddress remoteAddress;
		private SocketAddress localAddress;
		private final ByteBuffer decodedReadBuffer;
		private final ByteBuffer sslReadBuffer;
		private final ByteBuffer writeBuffer;
		private ByteBuffer sslWriteBuffer;
		private final byte[] readBytes;
		private final byte[] writeBytes;
		private boolean isClosed = false;
		private final int counter;
		private int id;
		private boolean inReadWrite = false;
		private boolean isReadBlocked = false;
		private boolean isWriteBlocked = false;
		private long writeBlockStartAt = 0;
		// some basic stats
		private long bytesWritten = 0;
		private long bytesRead = 0;
		private long opReadCallback = 0;
		private long opWriteCallback = 0;
		private long messageSendCount = 0;
		private long flushCount = 0;

		private final SSLEngine sslEngine;
		private final static int readWriteBufferSize = 131072;

		private final SocketConnectionTimeout socketConnectTimeout;
		private final TimerHandler socketConnectTimeoutHandler;

		private IOHandler(final ServerSocketChannel ssc, final SenderCallback callback, final SSLEngine sslEngine)
				throws IOException, ClosedChannelException {

			this.serverSocketChannel = ssc;
			this.callback = callback;
			this.counter = count++;
			this.sslEngine = sslEngine;
			readBytes = new byte[readWriteBufferSize];
			writeBytes = new byte[readWriteBufferSize];

			decodedReadBuffer = ByteBuffer.wrap(readBytes);
			decodedReadBuffer.order(ByteOrder.LITTLE_ENDIAN);
			writeBuffer = ByteBuffer.wrap(writeBytes);
			writeBuffer.order(ByteOrder.LITTLE_ENDIAN);

			sslReadBuffer = ByteBuffer.wrap(new byte[readBytes.length / 2]);
			sslReadBuffer.order(ByteOrder.LITTLE_ENDIAN);

			sslWriteBuffer = ByteBuffer.wrap(new byte[writeBytes.length * 2]);
			sslWriteBuffer.order(ByteOrder.LITTLE_ENDIAN);

			if (serverSocketChannel.isOpen()) {
				sk = wait.registerSelectableChannel(serverSocketChannel, this);
				sk.interestOps(SelectionKey.OP_ACCEPT);
				remoteAddress = ssc.getLocalAddress();
				localAddress = ssc.getLocalAddress();
				logger.info("ServerSocket local:{} remote:{}", remoteAddress, localAddress);
			} else {
				throw new ClosedChannelException();
			}
			socketConnectTimeout = null;
			socketConnectTimeoutHandler = null;
		}

		private IOHandler(final SocketChannel sc, final SenderCallback callback, final SSLEngine sslEngine)
				throws IOException {

			this.socketChannel = sc;
			this.callback = callback;
			this.counter = count++;
			this.sslEngine = sslEngine;
			readBytes = new byte[readWriteBufferSize];
			writeBytes = new byte[readWriteBufferSize];
			decodedReadBuffer = ByteBuffer.wrap(readBytes);
			decodedReadBuffer.order(ByteOrder.LITTLE_ENDIAN);
			writeBuffer = ByteBuffer.wrap(writeBytes);
			writeBuffer.order(ByteOrder.LITTLE_ENDIAN);
			sslReadBuffer = ByteBuffer.wrap(new byte[readBytes.length / 2]);
			sslReadBuffer.order(ByteOrder.LITTLE_ENDIAN);

			sslWriteBuffer = ByteBuffer.wrap(new byte[writeBytes.length * 2]);
			sslWriteBuffer.order(ByteOrder.LITTLE_ENDIAN);

			sk = wait.registerSelectableChannel(sc, this);
			sk.interestOps(SelectionKey.OP_CONNECT);
			socketConnectTimeout = new SocketConnectionTimeout();
			socketConnectTimeoutHandler = wait.createTimer(socketConnectTimeout, "SSLConnectionTimeout " + sc);
		}

		private class SocketConnectionTimeout implements TimerCallback {

			@Override
			public void timerCallback(final long dueAt, final long currentNanoTime) {
				logger.error("Error timeout waiting for SSL session to be established, closing:" + socketChannel);
				close();
			}

		}

		@Override
		public String toString() {
			// dump out all we know.
			return "localAddress:" + getLocalAddress()//
					+ " remoteAddress:" + getRemoteAddress()//
					+ " id:" + id//
					+ " instanceCounter:" + counter//
					+ " isClosed:" + isClosed//
					+ " isWriteBlocked:" + isWriteBlocked()//
					+ " isReadBlocked:" + isReadBlocked()//
					+ " skInt:" + ((sk == null) ? "SK=NULL" : sk.interestOps())//
					+ " bytesWritten:" + bytesWritten//
					+ " bytesRead:" + bytesRead//
					+ " opReadCallback:" + opReadCallback//
					+ " opWriteCallback:" + opWriteCallback//
					+ " messageSendCount:" + messageSendCount//
					+ " flushCount:" + flushCount//
					+ " sslLim.ps:" + sslWriteBuffer.position()//
					+ " sslLim:" + sslWriteBuffer.limit()//
					+ " writeBuffer.posn:" + writeBuffer.position()//
					+ " wteBf.lim:" + writeBuffer.limit();

		}

		public int maxBuffer() {
			return readBytes.length;
		}

		public int hashCode() {
			return counter;
		}

		void manageHandshake(final HandshakeStatus startStatus) throws Exception {

			switch (startStatus) {
			case FINISHED:
				logger.info("WRAP FINISHED Handshake completed:{} cipherSuite:{} protocol:{} buffer:{}", //
						startStatus, //
						sslEngine.getSession().getCipherSuite(), sslEngine.getSession().getProtocol(), //
						sslEngine.getSession().getApplicationBufferSize());

				isWriteBlocked = false;
				isReadBlocked = false;
				callback.connected(this);
				if (socketConnectTimeoutHandler.isRegistered()) {
					socketConnectTimeoutHandler.cancelTimer();
				} else {
					logger.error("Error was expectint timer to be registered during timeout {}",socketConnectTimeoutHandler);
				}
				setKeyStatus();
				break;
			case NEED_UNWRAP:
				if (logger.isTraceEnabled()) {
					logger.trace("NEED_UNWRAP");
				}
				break;
			case NEED_WRAP: {
				if (logger.isTraceEnabled()) {
					logger.trace("NEED_WRAP  writeBiffer.posn:{} ssl.posn:{}", writeBuffer.position(),
							sslWriteBuffer.position());
				}
				writeBuffer.clear();
				writeBuffer.flip();
				sslWriteBuffer.clear();
				final SSLEngineResult result = sslEngine.wrap(writeBuffer, sslWriteBuffer);
				if (logger.isTraceEnabled()) {
					logger.trace("wrap:{}", result);
				}
				final int sslRemaining = sslWriteBuffer.position();
				sslWriteBuffer.flip();
				final int writeCount = socketChannel.write(sslWriteBuffer);
				if (writeCount == -1) {
					close();
					throw new IOException("Error closed sending need wrap");
				}
				if (writeCount != sslRemaining) {
					close();
					throw new IOException("Error didnt write all during need wrap");
				}
				if (logger.isTraceEnabled()) {
					logger.trace("NEED_WRAP sent:{}", writeCount);
				}
				writeBuffer.clear();
				sslWriteBuffer.clear();

				final HandshakeStatus hsStatus = result.getHandshakeStatus();
				manageHandshake(hsStatus);
				logger.info("break Recursive wrap status :{} afterRecurse:{}", hsStatus,
						sslEngine.getHandshakeStatus());
				break;
			}
			case NEED_TASK:
				logger.trace("NEED_TASK");
				Runnable runnable;
				while ((runnable = sslEngine.getDelegatedTask()) != null) {
					if (logger.isTraceEnabled()) {
						logger.trace("\trunning delegated task...:{}", runnable);
					}
					runnable.run();
				}
				final HandshakeStatus hsStatus = sslEngine.getHandshakeStatus();
				if (logger.isTraceEnabled()) {
					logger.trace("Recursive wrap:{}", hsStatus);
				}
				manageHandshake(hsStatus);
				if (logger.isTraceEnabled()) {
					logger.trace("\tNEED_TASK  Recursive HandshakeStatus: " + hsStatus + " now:"
							+ sslEngine.getHandshakeStatus());
				}
				break;

			case NOT_HANDSHAKING:
				break;
			default:
				logger.error("handskare status unknown:" + startStatus);
			}

		}

		@Override
		public void opAccept(final SelectableChannel channel, final long currentTimeNanos) {

			try {
				final SocketChannel socketChannel = serverSocketChannel.accept();
				socketChannel.configureBlocking(false);

				remoteAddress = socketChannel.getRemoteAddress();
				final SSLEngine sslEngineConnectio = sslc.createSSLEngine();
				sslEngineConnectio.setUseClientMode(false);
				sslEngineConnectio.setNeedClientAuth(true);

				final IOHandler handler = new IOHandler(socketChannel, callback, sslEngineConnectio);
				final SelectionKey key = wait.registerSelectableChannel(socketChannel, handler);
				handler.sk = key;

				handler.remoteAddress = socketChannel.getRemoteAddress();
				handler.localAddress = socketChannel.getLocalAddress();

				sslEngine.beginHandshake();
				manageHandshake(sslEngine.getHandshakeStatus());
				isWriteBlocked = false;
				isReadBlocked = false;

				key.interestOps(SelectionKey.OP_READ);
				handler.socketConnectTimeoutHandler.fireIn(TimeUnit.MILLISECONDS.toNanos(SSL_TIMEOUT));

				logger.info("opAccept local:{} remote:{}", localAddress, remoteAddress);
			} catch (Exception ioe) {
				logger.error("Error connecting to server socket:", ioe);
				close();
			}
		}

		@Override
		public void opConnect(final SelectableChannel channel, final long currentTimeNanos) {

			try {

				final SocketChannel sc = (SocketChannel) channel;

				sc.configureBlocking(false);
				sc.setOption(StandardSocketOptions.SO_SNDBUF, DEFAULT_BUFFER);
				sc.setOption(StandardSocketOptions.SO_RCVBUF, DEFAULT_BUFFER);
				sc.setOption(StandardSocketOptions.TCP_NODELAY, true);

				final boolean finishConnect = sc.finishConnect();
				if (finishConnect == false) {
					throw new IOException("Error finishing connection " + sc);
				}
				remoteAddress = socketChannel.getRemoteAddress();
				localAddress = socketChannel.getLocalAddress();
				logger.info("opConnect local:{} remote:{}", localAddress, remoteAddress);

				sslEngine.beginHandshake();
				manageHandshake(sslEngine.getHandshakeStatus());

				isReadBlocked = false;
				isWriteBlocked = false;

				sk.interestOps(SelectionKey.OP_READ);
				
				socketConnectTimeoutHandler.fireIn(TimeUnit.MILLISECONDS.toNanos(SSL_TIMEOUT));
			} catch (Exception ioe) {
				logger.error("Error finishing connection", ioe);
				close();
			}
		}

		@Override
		public void opWrite(final SelectableChannel channel, final long currentTimeNanos) {
			// logger.info("opWrite start");

			opWriteCallback++;
			inReadWrite = true;
			try {
				final boolean potentiallUnblockWrite;
				if (isWriteBlocked) {
					potentiallUnblockWrite = true;
				} else {
					potentiallUnblockWrite = false;
				}

				// only flush the ssl write buffer
				final int remainingToWrite = sslWriteBuffer.position();
				sslWriteBuffer.flip();
				final int written = socketChannel.write(sslWriteBuffer);
				if (written == -1) {
					logger.error("closed connnection on opWrite");
					close();
					inReadWrite = false;
					return;
				} else if (written < remainingToWrite) {
					bytesWritten += written;
					// logger.info(
					// "opWrite partial write written:{} totalWrite:{} originalTarget:{}
					// sslWrite.posn:{} lim:{}",
					// written, bytesWritten, remainingToWrite, sslWriteBuffer.position(),
					// sslWriteBuffer.limit());
					// still blocked
					sslWriteBuffer.compact();

				} else if (written == remainingToWrite) {
					bytesWritten += written;
					sslWriteBuffer.clear();
					if (writeBuffer.position() > 0) {
						// may block again.
						isWriteBlocked = false;
						// logger.info("opWrite still data to flush:" + toString());
						flush();
						// logger.info("opWrite flush completed:" + toString());
						if (isWriteBlocked == false) {
							// logger.info("opWrite now unblocked");
							writeBlockStartAt = 0;
							callback.writeUnblocked(this);
						}
					} else {
						if (potentiallUnblockWrite) {
							// unblock
							writeBlockStartAt = 0;
							isWriteBlocked = false;
							callback.writeUnblocked(this);
						}
					}
				} else {
					logger.info("sdsd");
				}

			} catch (Exception e) {
				inReadWrite = false;
				logger.error("Error writing", e);
				close();
				return;
			}
			inReadWrite = false;
			setKeyStatus();
		}

		@Override
		public void opRead(final SelectableChannel channel, final long currentTimeNanos) {
			opReadCallback++;
			int bytesToParse = 0;
			if (isReadBlocked) {
				logger.warn("got read callback when read blocked " + this.toString());
				// odd.
				return;
			}
			decodedReadBuffer.clear();
			inReadWrite = true;
			int callbackBytesRead = -1;
			try {

				int maxReadCounter = 10;
				do {

					callbackBytesRead = socketChannel.read(sslReadBuffer);
					if (callbackBytesRead == -1) {
						close();
						inReadWrite = false;
						return;
					}
					if (callbackBytesRead == 0) {
						maxReadCounter = 0;
						break;
					}
					bytesToParse = sslReadBuffer.position();
					sslReadBuffer.flip();
					boolean exitLoop = false;
					while (exitLoop == false && !isReadBlocked) {

						final SSLEngineResult result = sslEngine.unwrap(sslReadBuffer, decodedReadBuffer);
						if (logger.isTraceEnabled()) {
							logger.trace("read bytes in:{} decoded:{} posn:{} UNWRAP:{}", callbackBytesRead,
									decodedReadBuffer.position(), sslReadBuffer.position(), result);
						}
						manageHandshake(result.getHandshakeStatus());
						switch (result.getStatus()) {
						case BUFFER_OVERFLOW:
							logger.error("Buffer BUFFER_OVERFLOW");
							break;
						case BUFFER_UNDERFLOW:
							// logger.info(
							// "Buffer BUFFER_UNDERFLOW sslReadBuffer.position:{} limit:{} readBytes:{}
							// decryt:{}",
							// sslReadBuffer.position(), sslReadBuffer.limit(), bytesToParse,
							// decodedReadBuffer.position());
							sslReadBuffer.compact();
							// logger.info("Buffer BUFFER_UNDERFLOW after compact sslReadBuffer.position:{}
							// limit:{}",
							// sslReadBuffer.position(), sslReadBuffer.limit());
							exitLoop = true;
							break;
						case CLOSED:
							close();
							break;
						case OK:
							if (sslReadBuffer.position() == bytesToParse) {
								// all decoded
								exitLoop = true;
								sslReadBuffer.clear();
							}
							if (decodedReadBuffer.position() > 0) {
								decodedReadBuffer.flip();
								callback.readData(this, decodedReadBuffer);
								decodedReadBuffer.clear();
							}
							if (logger.isTraceEnabled()) {
								logger.trace("okay: sslPosb:{} toal:{} read:{} exitLoop:{}", sslReadBuffer.position(),
										bytesToParse, callbackBytesRead, exitLoop);
							}
							break;
						default:
						}
					}

					bytesRead += callbackBytesRead;
				} while (callbackBytesRead > 0// no point if not reading data;
						&& maxReadCounter-- > 0 //
						&& !isReadBlocked // can be set in the callback
				);
				inReadWrite = false;
				setKeyStatus();
				if (logger.isTraceEnabled()) {
					logger.trace("opRead.exit status:{} ", this.toString());
				}
			} catch (SSLException ssle) {
				logger.error("SSLException sslReadPosition:" + sslReadBuffer.position() + " sslReadLim:"
						+ sslReadBuffer.limit() + " callbackRead:" + callbackBytesRead + " bytesToParse:"
						+ bytesToParse, ssle);
				close();
			} catch (Exception e) {
				if (isClosed) {
					logger.warn("Already closed on read callback channel:" + channel);
				} else {
					logger.error("Error on read", e);

					close();
				}
			}

		}

		@Override
		public long sendMessage(final byte[] message, final int offset, final int length)
				throws ClosedChannelException {
			try {
				long written = 0;
				if (writeBuffer.remaining() < length) {
					written += flush();
					if (bufferRemaining() < length) {
						return -1;
					}
				}
				writeBuffer.put(message, offset, length);
				messageSendCount++;
				return written;
			} catch (Exception e) {
				logger.error("Error sending message", e);
				close();
				throw new ClosedChannelException();
			}

		}

		@Override
		public long flush() throws IOException {
			if (logger.isTraceEnabled()) {
				logger.trace("flush start:" + isWriteBlocked + " " + isWriteBlocked());
			}
			if (isClosed) {
				throw new IOException("Error can not flush when closed");
			}
			if (isWriteBlocked) {
				// logger.debug("write blocked, can not flush:" + toString());
				return 0;
			}
			long thisFlush = 0;
			assert (sslWriteBuffer.position() == 0);
			inReadWrite = true;
			if (logger.isTraceEnabled()) {
				logger.trace(" flush id:{} posn:{} limit:{} sslPosn:{}", id, writeBuffer.position(),
						writeBuffer.limit(), sslWriteBuffer.position());
			}

			int writeCount = 0;
			writeBuffer.flip();
			boolean exitLoop = false;
			try {
				while (!exitLoop) {
					if (writeBuffer.position() == writeBuffer.limit()) {
						if (logger.isTraceEnabled()) {
							logger.trace("flush: finished, clear");
						}
						if (isWriteBlocked) {
							logger.trace("flush. writeUnblocked");
							writeBlockStartAt = 0;
							isWriteBlocked = false;
							callback.writeUnblocked(this);
						}
						writeBuffer.clear();
						exitLoop = true;
						break;
					}
					final int sslRemaining;
					final SSLEngineResult result = sslEngine.wrap(writeBuffer, sslWriteBuffer);

					switch (result.getStatus()) {
					case BUFFER_OVERFLOW:
						logger.error("buffer overflow");
						sslWriteBuffer = ByteBuffer.wrap(new byte[sslWriteBuffer.array().length * 2]);
						sslWriteBuffer.order(ByteOrder.LITTLE_ENDIAN);
						sslWriteBuffer.clear();
						break;
					case BUFFER_UNDERFLOW:
						logger.trace("flush underflow");
						exitLoop = true;
						break;
					case CLOSED:
						close();
						exitLoop = true;
						break;
					case OK:
						sslRemaining = sslWriteBuffer.position();
						sslWriteBuffer.flip();
						writeCount = socketChannel.write(sslWriteBuffer);
						if (writeCount == -1) {
							close();
							return -1;
						} else if (writeCount < sslRemaining) {
							// logger.info(
							// "flush:wrote some, still blocked writeCount:{} pre.posn:{} pre.lim:{}
							// ssl.pos:{} ssl.lim:{}",
							// writeCount, writeBuffer.position(), writeBuffer.limit(),
							// sslWriteBuffer.position(),
							// sslWriteBuffer.limit());
							writeBlockStartAt = wait.currentTimeNanos;
							isWriteBlocked = true;
							sslWriteBuffer.compact();
							writeBuffer.compact();
							exitLoop = true;
						} else if (writeCount == sslRemaining) {
							sslWriteBuffer.clear();
							if (logger.isTraceEnabled()) {
								logger.trace("flush, wrote all writeBuffer:{} {}", writeBuffer.position(),
										writeBuffer.limit());
							}
						}
						thisFlush += writeCount;
						break;

					default:
						logger.error("unknown type:" + result);
					}
				}
				bytesWritten += thisFlush;
				flushCount++;
				inReadWrite = false;
				setKeyStatus();
				if (logger.isTraceEnabled()) {
					logger.trace("flush write:{} thisFlush:{} total:{} count:{}", writeCount, thisFlush, bytesWritten,
							flushCount);
				}
				return thisFlush;

			} catch (IOException ioe) {
				if (isClosed) {
					logger.warn("calling flush on already closed channel:" + socketChannel);
				} else {
					throw ioe;
				}
			} catch (Exception e) {
				logger.error("Error in fluush. closing", e);
				close();
			} finally {
			}
			throw new IOException("unknown error");

		}

		int currentOps;

		/** set read/write status in case more data to be flushed */
		private void setKeyStatus() {
			currentOps = sk.interestOps();
			final int futureOps;
			if (isWriteBlocked) {
				if (isReadBlocked) {
					futureOps = SelectionKey.OP_WRITE;
				} else {
					futureOps = SelectionKey.OP_WRITE + SelectionKey.OP_READ;
				}
			} else {
				if (isReadBlocked) {
					// !writeBLocked + readBlocked
					futureOps = 0;

				} else {
					// !writeBloced + !readBlocked
					futureOps = SelectionKey.OP_READ;
				}
			}
			if (currentOps == futureOps) {
				// nothing doing
			} else {
				// has changed. lets set it.
				sk.interestOps(futureOps);
				currentOps = futureOps;
			}
			// logger.debug("setKeyStatus:" + futureOps);
		}

		@Override
		public int bytesInBuffer() {
			return writeBuffer.position();
		}

		@Override
		public long isWriteBlocked() {
			if (isWriteBlocked) {
				return wait.currentTimeNanos - writeBlockStartAt;
			} else {
				return -1;
			}
		}

		private boolean isEngineClosed() {
			return sslEngine.isInboundDone() && sslEngine.isOutboundDone();
		}

		@Override
		public void close() {
			logger.info("Closing local:{} remote:{} sslC:{}", localAddress, remoteAddress, sslc);

			if (isClosed) {
				logger.info("Already closed");
				return;
			}
			isClosed = true;

			if (socketConnectTimeoutHandler.isRegistered()) {
				// cancel the timer. normallly expect it to be closed already
				socketConnectTimeoutHandler.cancelTimer();
			}
			if (bytesInBuffer() > 0) {
				try {
					final long inBuffer = flush();

					logger.debug("close in outbound:{}", inBuffer);

				} catch (Exception e) {

				}
			}
			try {
				sslEngine.closeOutbound();
				logger.debug("close outbound");
				sslEngine.closeInbound();
				logger.debug("close inbound");
				final long sent = flush();
				logger.debug("flush close outbound:{}", sent);

			} catch (Exception e) {

			}
			try {
				if (socketChannel != null && socketChannel.isOpen()) {
					socketChannel.close();
				}
			} catch (Exception e) {

			} finally {
				socketChannel = null;
			}

			try {
				if (sk != null) {
					sk.cancel();
				}
			} finally {
				sk = null;
			}

			try {
				if (serverSocketChannel != null && serverSocketChannel.isOpen()) {
					serverSocketChannel.close();
				}
			} catch (Exception e) {

			} finally {
				serverSocketChannel = null;
			}

			callback.closed(this);
			logger.info("Close Completed");
		}

		@Override
		public SocketAddress getRemoteAddress() {
			return remoteAddress;
		}

		@Override
		public SocketAddress getLocalAddress() {
			return localAddress;
		}

		@Override
		public int bufferRemaining() {
			return writeBuffer.remaining();
		}

		@Override
		public int getId() {
			return id;
		}

		@Override
		public void setId(int id) {
			this.id = id;
		}

		@Override
		public long isReadBlocked() {
			return isReadBlocked ? 1 : -1;
		}

		@Override
		public void blockRead() {
			logger.info("block read");
			if (isReadBlocked) {
				return;
			}
			isReadBlocked = true;
			if (inReadWrite) {
				return;
			}
			setKeyStatus();
		}

		@Override
		public void unblockRead() {
			if (!isReadBlocked) {
				return;
			}
			isReadBlocked = false;
			if (inReadWrite) {
				return;
			}
			setKeyStatus();
		}

	}

}
