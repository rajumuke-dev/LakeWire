package lakewire.storage.lkw1;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lakewire.storage.Crc32c;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MutableRecordBatch;

public final class Lkw1Reader {

    private static final byte[] MAGIC = "LKW1".getBytes(StandardCharsets.US_ASCII);

    private Lkw1Reader() {
    }

    public static Lkw1Object read(byte[] bytes) {
        return decode(bytes).object();
    }

    /** One row per RecordBatch, in file order. {@code byteStart} is absolute (A29). */
    public static List<Lkw1Batch> batches(byte[] bytes) {
        Decoded decoded = decode(bytes);
        List<Lkw1Batch> rows = new ArrayList<>();
        List<Lkw1Chunk> chunks = decoded.object().chunks();
        for (int i = 0; i < chunks.size(); i++) {
            Lkw1Chunk chunk = chunks.get(i);
            long start = decoded.starts()[i];
            for (MutableRecordBatch batch : MemoryRecords.readableRecords(ByteBuffer.wrap(chunk.records())).batches()) {
                rows.add(new Lkw1Batch(chunk.topicId(), chunk.part(), batch.baseOffset(), batch.maxTimestamp(), start, batch.sizeInBytes()));
                start += batch.sizeInBytes();
            }
        }
        return List.copyOf(rows);
    }

    private static Decoded decode(byte[] bytes) {
        if (bytes == null || bytes.length < 4 + 1 + 2 + 8 + 8 + 16) {
            throw new Lkw1Exception("LKW1 object is truncated");
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int footer = bytes.length - 16;
        buffer.position(footer);
        long indexOffsetLong = buffer.getLong();
        int indexCrc = buffer.getInt();
        byte[] footerMagic = new byte[4];
        buffer.get(footerMagic);
        if (!Arrays.equals(footerMagic, MAGIC)) {
            throw new Lkw1Exception("LKW1 footer magic mismatch");
        }
        if (indexOffsetLong < 0 || indexOffsetLong > footer) {
            throw new Lkw1Exception("LKW1 index offset is out of range");
        }
        int indexOffset = (int) indexOffsetLong;
        int indexLength = footer - indexOffset;
        if (indexLength % Lkw1Writer.ENTRY_BYTES != 0) {
            throw new Lkw1Exception("LKW1 index is truncated");
        }
        byte[] index = Arrays.copyOfRange(bytes, indexOffset, footer);
        if (Integer.toUnsignedLong(indexCrc) != Crc32c.of(index)) {
            throw new Lkw1Exception("LKW1 index crc mismatch");
        }

        buffer.position(0);
        byte[] headerMagic = new byte[4];
        buffer.get(headerMagic);
        if (!Arrays.equals(headerMagic, MAGIC)) {
            throw new Lkw1Exception("LKW1 header magic mismatch");
        }
        int version = buffer.get() & 0xFF;
        if (version != 1) {
            throw new Lkw1Exception("LKW1 format version mismatch");
        }
        int brokerLength = buffer.getShort() & 0xFFFF;
        if (buffer.position() + brokerLength + 16 > indexOffset) {
            throw new Lkw1Exception("LKW1 object is truncated");
        }
        byte[] brokerBytes = new byte[brokerLength];
        buffer.get(brokerBytes);
        long brokerEpoch = buffer.getLong();
        long createdMs = buffer.getLong();
        int headerEnd = buffer.position();
        if (headerEnd > indexOffset) {
            throw new Lkw1Exception("LKW1 object is truncated");
        }

        ByteBuffer entries = ByteBuffer.wrap(index);
        List<Lkw1Chunk> chunks = new ArrayList<>(indexLength / Lkw1Writer.ENTRY_BYTES);
        long[] starts = new long[indexLength / Lkw1Writer.ENTRY_BYTES];
        while (entries.hasRemaining()) {
            long topicId = entries.getLong();
            int part = entries.getInt();
            long byteStart = entries.getLong();
            int byteLen = entries.getInt();
            int nRecords = entries.getInt();
            short nBatches = entries.getShort();
            long maxTimestamp = entries.getLong();
            long expectedCrc = Integer.toUnsignedLong(entries.getInt());
            if (byteStart < headerEnd || byteLen < 0 || byteStart + byteLen > indexOffset) {
                throw new Lkw1Exception("LKW1 chunk bounds are invalid for topic " + topicId + " part " + part);
            }
            byte[] records = Arrays.copyOfRange(bytes, (int) byteStart, (int) byteStart + byteLen);
            if (Crc32c.of(records) != expectedCrc) {
                throw new Lkw1Exception("LKW1 chunk crc mismatch for topic " + topicId + " part " + part);
            }
            starts[chunks.size()] = byteStart;
            chunks.add(new Lkw1Chunk(topicId, part, records, nRecords, nBatches, maxTimestamp));
        }
        Lkw1Object object = new Lkw1Object(new String(brokerBytes, StandardCharsets.UTF_8), brokerEpoch, createdMs, List.copyOf(chunks));
        return new Decoded(object, starts);
    }

    private record Decoded(Lkw1Object object, long[] starts) {
    }
}
