package lakewire.cli;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import lakewire.storage.lkw1.Lkw1Chunk;
import lakewire.storage.lkw1.Lkw1Object;
import lakewire.storage.lkw1.Lkw1Writer;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MemoryRecordsBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

class InspectCommandTest {

    @Test
    void printsHeaderAndIndexFromALocalFile(@TempDir Path dir) throws Exception {
        byte[] body = Lkw1Writer.write(new Lkw1Object(
            "1",
            7,
            1_700_000_000_000L,
            List.of(new Lkw1Chunk(4, 0, new byte[] {1, 2, 3, 4}, 1, (short) 1, 99))
        ));
        Path file = dir.resolve("sample.lkw");
        Files.write(file, body);
        StringWriter out = new StringWriter();
        int code = LakewireCli.execute(
            new String[] {"inspect", "object", file.toString()},
            new PrintWriter(out, true),
            new PrintWriter(out, true)
        );
        assertThat(code).isZero();
        assertThat(out.toString())
            .contains("LKW1 brokerId=1 epoch=7 createdMs=1700000000000")
            .contains("topic=4 part=0 nRecords=1 nBatches=1 maxTs=99 bytes=4");
    }

    @Test
    void printsOneRowPerRecordBatch(@TempDir Path dir) throws Exception {
        byte[] first = batch(5L, 2, 1_000);
        byte[] second = batch(7L, 3, 2_000);
        byte[] records = new byte[first.length + second.length];
        System.arraycopy(first, 0, records, 0, first.length);
        System.arraycopy(second, 0, records, first.length, second.length);
        Lkw1Chunk chunk = new Lkw1Chunk(4, 2, records, 5, (short) 2, 2_002);
        byte[] body = Lkw1Writer.write(new Lkw1Object("b", 1, 2, List.of(chunk)));
        long start = Lkw1Writer.chunkStarts("b", List.of(chunk))[0];
        Path file = dir.resolve("batches.lkw");
        Files.write(file, body);
        StringWriter out = new StringWriter();
        int code = LakewireCli.execute(
            new String[] {"inspect", "object", file.toString()},
            new PrintWriter(out, true),
            new PrintWriter(out, true)
        );
        assertThat(code).isZero();
        assertThat(out.toString().lines().map(String::trim).map(line -> line.split("\\s+")).map(List::of))
            .contains(
                List.of("topic", "part", "baseOffset", "maxTimestamp", "byteStart", "length"),
                List.of("4", "2", "5", "1001", String.valueOf(start), String.valueOf(first.length)),
                List.of("4", "2", "7", "2002", String.valueOf(start + first.length), String.valueOf(second.length))
            );
    }

    private static byte[] batch(long baseOffset, int records, long timestamp) {
        ByteBuffer buffer = ByteBuffer.allocate(256 + records * 64);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, Compression.NONE, TimestampType.CREATE_TIME, baseOffset);
        for (int i = 0; i < records; i++) {
            builder.append(timestamp + i, new byte[] {(byte) i}, new byte[] {(byte) i});
        }
        ByteBuffer bytes = builder.build().buffer();
        byte[] copy = new byte[bytes.remaining()];
        bytes.get(copy);
        return copy;
    }
}
