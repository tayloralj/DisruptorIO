package com.ajt.disruptorIO;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.lmax.disruptor.EventFactory;

final class TestEvent {
	enum EventType {
		data, newServer, close, UNKNOWN;
	}

	interface MessageType {
		int dataEvent = 1;
		int clientMessage = 2;
		int serverMessage = 3;

	}

	TestEvent.EventType type = EventType.UNKNOWN;
	byte[] data = null;
	int targetID = 0;
	final long seqNum;
	long nanoSendTime = 0;
	private static long counter = 0;
	private final ByteBuffer writeBuffer;
	private final ByteBuffer readBuffer;

	interface Offsets {
		int length = 0;
		int type = 4;
		int seqnum = 8;
		int respSeqNum = 16;
		int time = 24;
		int responsTime = 32;
	}

	static long getLongFromArray(final byte[] data, final int offset) {
		return ((long) (data[offset + 0]) << 56) + //
				(((long) data[offset + 1] & 0xFF) << 48) + //
				(((long) data[offset + 2] & 0xFF) << 40) + //
				(((long) data[offset + 3] & 0xFF) << 32) + //
				(((long) data[offset + 4] & 0xFF) << 24) + //
				(((long) data[offset + 5] & 0xFF) << 16) + //
				(((long) data[offset + 6] & 0xFF) << 8) + //
				(((long) data[offset + 7] & 0xFF));
	}

	static int getIntFromArray(final byte[] data, final int offset) {
		return ((int) (data[offset + 0]) << 24) + //
				(((int) data[offset + 1] & 0xFF) << 16) + //
				(((int) data[offset + 2] & 0xFF) << 8) + //
				(((int) data[offset + 3] & 0xFF));
	}

	static void putLongToArray(final byte[] data, final int offset, final long value) {
		data[offset + 0] = (byte) ((value >> 56) & 0xFF);
		data[offset + 1] = (byte) ((value >> 48) & 0xFF);
		data[offset + 2] = (byte) ((value >> 40) & 0xFF);
		data[offset + 3] = (byte) ((value >> 32) & 0xFF);
		data[offset + 4] = (byte) ((value >> 24) & 0xFF);
		data[offset + 5] = (byte) ((value >> 16) & 0xFF);
		data[offset + 6] = (byte) ((value >> 8) & 0xFF);
		data[offset + 7] = (byte) ((value) & 0xFF);
	}

	static void putIntToArray(final byte[] data, final int offset, final int value) {
		data[offset + 0] = (byte) ((value >> 24) & 0xFF);
		data[offset + 1] = (byte) ((value >> 16) & 0xFF);
		data[offset + 2] = (byte) ((value >> 8) & 0xFF);
		data[offset + 3] = (byte) ((value) & 0xFF);
	}

	private TestEvent() {
		seqNum = counter++;
		data = new byte[4096];
		writeBuffer = ByteBuffer.wrap(data);
		writeBuffer.order(ByteOrder.LITTLE_ENDIAN);
		readBuffer = ByteBuffer.wrap(data);
		readBuffer.order(ByteOrder.LITTLE_ENDIAN);
	}

	public void setData(final byte[] data_, final int offset, final int length, final int type) {
		writeBuffer.clear();
		writeBuffer.put(data_, offset, length);
		putIntToArray(data, Offsets.length, length);
		putIntToArray(data, Offsets.type, type);
	}

	public int getLength() {
		return getIntFromArray(data, Offsets.length);
	}

	public int getType() {
		return getIntFromArray(data, Offsets.type);
	}

	public byte[] getData() {
		return data;
	}

	public static final EventFactory<TestEvent> EVENT_FACTORY = new EventFactory<TestEvent>() {
		@Override
		public TestEvent newInstance() {
			return new TestEvent();
		}
	};

}