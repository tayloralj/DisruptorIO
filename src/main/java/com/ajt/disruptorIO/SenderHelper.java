package com.ajt.disruptorIO;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ajt.disruptorIO.NIOWaitStrategy.SelectorCallback;

public class SenderHelper {
	protected final Logger logger = LoggerFactory.getLogger(SenderHelper.class);
	protected NIOWaitStrategy wait;
	private static int count = 0;

	public static final int DEFAULT_BUFFER = 64 * 1024;
	public static boolean recordStats = false;

	public SenderHelper(NIOWaitStrategy waiter) {
		this.wait = waiter;
	}

	public SenderCallin connectTo(final InetSocketAddress remote, final SenderCallback callback) throws IOException {

		final SocketChannel sc = SocketChannel.open();
		sc.configureBlocking(false);
		sc.connect(remote);
		logger.info("connectTo:{}", remote);

		final IOHandler IOh = new IOHandler(sc, callback);
		return IOh;

	}

	public SenderCallin bindTo(final InetSocketAddress local, SenderCallback callback) throws IOException {
		final ServerSocketChannel socketChannel = ServerSocketChannel.open();
		socketChannel.configureBlocking(false);
		socketChannel.bind(local, 0);
		logger.info("bindTo:{} channel:{}", local, socketChannel);

		final IOHandler IOh = new IOHandler(socketChannel, callback);
		return IOh;

	}

	private class IOHandler implements SelectorCallback, SenderCallin {
		private ServerSocketChannel serverSocketChannel = null;
		private SocketChannel socketChannel = null;
		private SelectionKey sk = null;
		private final SenderCallback callback;
		private SocketAddress remoteAddress;
		private SocketAddress localAddress;
		private final ByteBuffer readBuffer;
		private final ByteBuffer writeBuffer;
		private final byte[] readBytes;
		private final byte[] writeBytes;
		private boolean isClosed = false;
		private boolean isBlocked = false;
		private long blockStartAt = 0;
		private final int counter;

		public IOHandler(final ServerSocketChannel ssc, final SenderCallback callback)
				throws IOException, ClosedChannelException {

			this.serverSocketChannel = ssc;
			this.callback = callback;
			this.counter = count++;
			readBytes = new byte[16384];
			writeBytes = new byte[16384];
			readBuffer = ByteBuffer.wrap(readBytes);
			readBuffer.order(ByteOrder.LITTLE_ENDIAN);
			writeBuffer = ByteBuffer.wrap(writeBytes);
			writeBuffer.order(ByteOrder.LITTLE_ENDIAN);
			if (serverSocketChannel.isOpen()) {
				sk = wait.registerSelectableChannel(serverSocketChannel, this);
				sk.interestOps(SelectionKey.OP_ACCEPT);
				remoteAddress = ssc.getLocalAddress();
				localAddress = ssc.getLocalAddress();
				logger.info("ServerSocket local:{} remote:{}", remoteAddress, localAddress);
			} else {
				throw new ClosedChannelException();
			}
		}

		public IOHandler(final SocketChannel sc, final SenderCallback callback) throws IOException {

			this.socketChannel = sc;
			this.callback = callback;
			this.counter = count++;

			readBytes = new byte[16384];
			writeBytes = new byte[16384];
			readBuffer = ByteBuffer.wrap(readBytes);
			readBuffer.order(ByteOrder.LITTLE_ENDIAN);
			writeBuffer = ByteBuffer.wrap(writeBytes);
			writeBuffer.order(ByteOrder.LITTLE_ENDIAN);

			sk = wait.registerSelectableChannel(sc, this);
			sk.interestOps(SelectionKey.OP_CONNECT);

		}

		public int maxBuffer() {
			return readBytes.length;
		}

		public int hashCode() {
			return counter;
		}

		@Override
		public void opRead(final SelectableChannel channel, final long currentTimeNanos) {

			try {
				int bytesRead = -1;
				int readCounter = 20;
				do {
					bytesRead = socketChannel.read(readBuffer);
					if (bytesRead == -1) {
						close();
						break;
					}
					if (bytesRead == 0) {
						break;
					}
					readBuffer.flip();
					callback.readData(this, readBuffer);
					readBuffer.clear();
				} while (bytesRead > 0 && readCounter-- > 0);
			} catch (Exception e) {
				logger.error("Error on read", e);
				close();
			}
		}

		@Override
		public void opAccept(final SelectableChannel channel, long currentTimeNanos) {

			try {
				final SocketChannel socketChannel = serverSocketChannel.accept();
				socketChannel.configureBlocking(false);
				remoteAddress = socketChannel.getRemoteAddress();
				final IOHandler handler = new IOHandler(socketChannel, callback);
				final SelectionKey key = wait.registerSelectableChannel(socketChannel, handler);
				handler.sk = key;
				key.interestOps(SelectionKey.OP_READ);

				handler.remoteAddress = socketChannel.getRemoteAddress();
				handler.localAddress = socketChannel.getLocalAddress();

				logger.info("opAccept local:{} remote:{}", localAddress, remoteAddress);
				callback.connected(handler);
			} catch (IOException ioe) {
				logger.error("Error connecting to server socket:", ioe);
				close();
			}
		}

		@Override
		public void opConnect(final SelectableChannel channel, long currentTimeNanos) {

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

				sk.interestOps(SelectionKey.OP_READ);
			} catch (IOException ioe) {
				logger.error("Error finishing connection", ioe);
				close();
			}
		}

		@Override
		public void opWrite(final SelectableChannel channel, long currentTimeNanos) {

			try {
				flush();

			} catch (Exception e) {
				logger.error("Error writing", e);
				close();
			}
		}

		@Override
		public boolean sendMessage(final byte[] message, final int offset, final int length)
				throws ClosedChannelException {
			try {
				if (writeBuffer.remaining() < length) {
					flush();
					if (bufferRemaining() < length) {
						return false;
					}
				}
				writeBuffer.put(message, offset, length);
				return true;
			} catch (Exception e) {
				logger.error("Error sending message", e);
				close();
				throw new ClosedChannelException();
			}

		}

		@Override
		public long flush() throws IOException {
			if (logger.isTraceEnabled()) {
				logger.trace(" flush id:{} posn:{}", id, writeBuffer.position());
			}
			writeBuffer.flip();
			final int remaining = writeBuffer.remaining();
			final int writeCount = socketChannel.write(writeBuffer);
			if (writeCount == -1) {
				close();
			} else if (writeCount == remaining) {
				writeBuffer.clear();
				if (isBlocked) {
					sk.interestOps(SelectionKey.OP_READ);
					isBlocked = false;
					callback.unblocked(this);
				}
			} else {
				writeBuffer.compact();
				// blocked
				if (!isBlocked) {
					sk.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
					blockStartAt = wait.currentTimeNanos;
					isBlocked = true;
					callback.blocked(this);
				}

			}
			return bufferRemaining();
		}

		@Override
		public int byteInBuffer() {
			return writeBuffer.position();
		}

		@Override
		public long isBlocked() {
			if (isBlocked) {
				return wait.currentTimeNanos - blockStartAt;
			} else {
				return 0;
			}
		}

		@Override
		public void close() {
			logger.info("Closing local:{} remote:{}", localAddress, remoteAddress);
			if (isClosed) {
				logger.info("Already closed");
				return;
			}
			isClosed = true;
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

		private int id;

		@Override
		public int getId() {
			// TODO Auto-generated method stub
			return id;
		}

		@Override
		public void setId(int id) {
			this.id = id;
		}

	}

	/**
	 * callback interface for inform about status of connections
	 * 
	 * @author ajt
	 *
	 */
	public interface SenderCallback {
		public SenderCallin connected(final SenderCallin callin);

		public void blocked(final SenderCallin callin);

		public void unblocked(final SenderCallin callin);

		public void readData(final SenderCallin callin, final ByteBuffer buffer);

		public void closed(final SenderCallin callin);

	}

	/**
	 * client calls this, gets as returned on connection
	 * 
	 * @author ajt
	 *
	 */
	public interface SenderCallin extends AutoCloseable {

		public SocketAddress getRemoteAddress();

		public SocketAddress getLocalAddress();

		/**
		 * return true if sent or buffered, will return false if can not gtee sending at
		 * this point
		 */
		public boolean sendMessage(final byte[] message, final int offset, final int length)
				throws ClosedChannelException;

		/** flush as much as possible to socket */
		public long flush() throws IOException;

		/** flush should be called if >0 */
		public int byteInBuffer();

		/** space available before will block */
		public int bufferRemaining();

		public int maxBuffer();

		/** true if blocked, time when block started */
		public long isBlocked();

		public int getId();

		public void setId(int id);
	}

}
