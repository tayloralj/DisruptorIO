package com.ajt.disruptorIO;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.collections.Histogram;

public final class TestEvent {
	public enum EventType {
		data, newServer, close, UNKNOWN;
	}

	public interface MessageType {
		int dataFromServerEvent = 1;
		int clientRequestMessage = 2;
		int serverResponseMessage = 3;

	}

	/** point into a message where data is stored */
	public interface Offsets {
		int length = 0;
		int type = 4;
		int seqnum = 8;
		int respSeqNum = 16;
		int time = 24;
		int responsTime = 32;
	}

	public TestEvent.EventType type = EventType.UNKNOWN;
	public byte[] data = null;
	public int targetID = 0;
	public final long seqNum;
	public long nanoSendTime = 0;
	private static long counter = 0;
	private final ByteBuffer writeBuffer;
	private final ByteBuffer readBuffer;

	public static Histogram getHisto() {
		Histogram h = new Histogram(new long[] { 100, 200, 400, 1000, 4000, 8000, 20000, 50000, 100000, 200000, 500000,
				2000000, 5000000, 5000000 * 4, 5000000 * 10, 5000000 * 20, 5000000 * 50 });

		return h;
	}

	public static String toStringHisto(Histogram h) {
		final StringBuilder sb = new StringBuilder();
		sb.append("Mean: " + h.getMean());
		final int size = h.getSize();
		for (int a = 0; a < size; a++) {
			if (h.getCountAt(a) > 0) {
				sb.append(" ").append(h.getUpperBoundAt(a)).append(":").append(h.getCountAt(a));
			}
		}
		return sb.toString();
	}

	public static long getLongFromArray(final byte[] data, final int offset) {
		return ((long) (data[offset + 0]) << 56) + //
				(((long) data[offset + 1] & 0xFF) << 48) + //
				(((long) data[offset + 2] & 0xFF) << 40) + //
				(((long) data[offset + 3] & 0xFF) << 32) + //
				(((long) data[offset + 4] & 0xFF) << 24) + //
				(((long) data[offset + 5] & 0xFF) << 16) + //
				(((long) data[offset + 6] & 0xFF) << 8) + //
				(((long) data[offset + 7] & 0xFF));
	}

	public static int getIntFromArray(final byte[] data, final int offset) {
		return ((int) (data[offset + 0]) << 24) + //
				(((int) data[offset + 1] & 0xFF) << 16) + //
				(((int) data[offset + 2] & 0xFF) << 8) + //
				(((int) data[offset + 3] & 0xFF));
	}

	public static void putLongToArray(final byte[] data, final int offset, final long value) {
		data[offset + 0] = (byte) ((value >> 56) & 0xFF);
		data[offset + 1] = (byte) ((value >> 48) & 0xFF);
		data[offset + 2] = (byte) ((value >> 40) & 0xFF);
		data[offset + 3] = (byte) ((value >> 32) & 0xFF);
		data[offset + 4] = (byte) ((value >> 24) & 0xFF);
		data[offset + 5] = (byte) ((value >> 16) & 0xFF);
		data[offset + 6] = (byte) ((value >> 8) & 0xFF);
		data[offset + 7] = (byte) ((value) & 0xFF);
	}

	public static void putIntToArray(final byte[] data, final int offset, final int value) {
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