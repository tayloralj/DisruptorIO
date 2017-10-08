package com.ajt.disruptorIO;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;

public interface ConnectionHelper {

	int DEFAULT_BUFFER = 64 * 1024;

	ConnectionHelper.SenderCallin connectTo(InetSocketAddress remote, SenderCallback callback) throws IOException;

	ConnectionHelper.SenderCallin bindTo(InetSocketAddress local, SenderCallback callback) throws IOException;

	/**
	 * client calls this, gets as returned on connection
	 * 
	 * @author ajt
	 *
	 */
	interface SenderCallin extends AutoCloseable {

		public SocketAddress getRemoteAddress();

		public SocketAddress getLocalAddress();

		/**
		 * return true if sent or buffered, will return false if can not gtee sending at
		 * this point
		 */
		public long sendMessage(final byte[] message, final int offset, final int length)
				throws ClosedChannelException;

		/** flush as much as possible to socket */
		public long flush() throws IOException;

		/** flush should be called if >0 */
		public int byteInBuffer();

		/** space available before will block */
		public int bufferRemaining();

		public int maxBuffer();

		/** true if blocked, time when block started */
		public long isWriteBlocked();

		/** true if blocked, time when block started */
		public long isReadBlocked();

		/** suspend inbound reading of data until unblock called */
		public void blockRead();

		/** will start delivering data from the socket - if there is any */
		public void unblockRead();

		/** return the id you set */
		public int getId();

		public void setId(int id);

		/** please would you close this connection */
		public void close();
	}

	public interface SenderCallback {
		/** when connected */
		public void connected(final ConnectionHelper.SenderCallin callin);

		/** got a bunch of data in the send buffer which could not be sent yet */
		public void writeNowBlocked(final ConnectionHelper.SenderCallin callin);

		/** all data now flushed from the buffer. free to continue */
		public void writeUnblocked(final ConnectionHelper.SenderCallin callin);

		/** some data was read from the remote connectin - here it is */
		public void readData(final ConnectionHelper.SenderCallin callin, final ByteBuffer buffer);

		/** the connection is now closed */
		public void closed(final ConnectionHelper.SenderCallin callin);

	}
}