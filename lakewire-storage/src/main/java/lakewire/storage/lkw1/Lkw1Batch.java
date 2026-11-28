package lakewire.storage.lkw1;

/** One RecordBatch inside an LKW1 object. {@code byteStart} is the absolute file offset. */
public record Lkw1Batch(long topicId, int part, long baseOffset, long maxTimestamp, long byteStart, int length) {
}
