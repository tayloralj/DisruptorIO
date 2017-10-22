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
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;

public interface ConnectionHelper {

	public int DEFAULT_BUFFER = 64 * 1024;

	/** connect to a remote address, single callback when connected */
	public ConnectionHelper.SenderCallin connectTo(final InetSocketAddress remote, final SenderCallback callback)
			throws IOException;

	/**
	 * bind to a local address, callback will be made for each new connection
	 * 
	 * @param local
	 * @param callback
	 * @return
	 * @throws IOException
	 */
	ConnectionHelper.SenderCallin bindTo(final InetSocketAddress local, final SenderCallback callback)
			throws IOException;

	/**
	 * client calls this, gets as returned on connection api to control a socket
	 * 
	 * @author ajt
	 *
	 */
	public interface SenderCallin extends AutoCloseable {

		public SocketAddress getRemoteAddress();

		public SocketAddress getLocalAddress();

		/**
		 * returns >0 if data was acceped and flushed returns 0 if nothing written -
		 * yet, but data accepted returns -1 if the data could not be buffered throws
		 * exception if the channel is closed
		 */
		public long sendMessage(final byte[] message, final int offset, final int length) throws ClosedChannelException;

		/**
		 * flush as much as possible to socket, can be called if no data to send. this
		 * should be called by the client, eg at the end of a batch of data
		 */
		public long flush() throws IOException;

		/** flush should be called if >0 */
		public int bytesInBuffer();

		/** space available before will block */
		public int bufferRemaining();

		/** max amount of data which can be in the buffer at once */
		public int maxBuffer();

		/**
		 * >0 if blocked, time will be the nanoseconds when the time when block started
		 */
		public long isWriteBlocked();

		/** true if blocked, time when block started */
		public long isReadBlocked();

		/**
		 * suspend inbound reading of data until unblock called. is intended for use by
		 * clients who need to suspend arrival of data until some processing is
		 * completed
		 */
		public void blockRead();

		/** will start delivering data from the socket - if there is any */
		public void unblockRead();

		/** return the id you set */
		public int getId();

		/** handle way to indenfify */
		public void setId(final int id);

		/** please would you close this connection */
		public void close();
	}

	/** callbacks to be recieved by the client at any time */
	public interface SenderCallback {
		/**
		 * when connected to endpoint, either as server socket or connection to remote
		 * endpoint
		 */
		public void connected(final ConnectionHelper.SenderCallin callin);

		/** got a bunch of data in the send buffer which could not be sent yet */
		public void writeNowBlocked(final ConnectionHelper.SenderCallin callin);

		/** all data now flushed from the buffer. free to continue */
		public void writeUnblocked(final ConnectionHelper.SenderCallin callin);

		/**
		 * some data was read from the remote connectin - here it is. once callback is
		 * completed the buffer will be reused. copy everything out of the buffer
		 */
		public void readData(final ConnectionHelper.SenderCallin callin, final ByteBuffer buffer);

		/**
		 * the connection is now closed. may be triggered by anything wich closed the
		 * socket
		 */
		public void closed(final ConnectionHelper.SenderCallin callin);

	}
}