package lakewire.cli;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import lakewire.storage.ObjectHead;
import lakewire.storage.ObjectStore;
import lakewire.storage.S3ObjectStore;
import lakewire.storage.S3StorageConfig;
import lakewire.storage.lkw1.Lkw1Batch;
import lakewire.storage.lkw1.Lkw1Chunk;
import lakewire.storage.lkw1.Lkw1Object;
import lakewire.storage.lkw1.Lkw1Reader;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;
import picocli.CommandLine.Model.CommandSpec;

@Command(
    name = "inspect",
    mixinStandardHelpOptions = true,
    description = "Inspect LakeWire artifacts",
    subcommands = InspectCommand.ObjectInspect.class
)
final class InspectCommand {

    @Command(name = "object", description = "Decode an LKW1 header and index without Postgres")
    static final class ObjectInspect implements Callable<Integer> {

        @Spec
        CommandSpec command;

        @Parameters(index = "0", paramLabel = "<key-or-path>", description = "object key, s3:// URI, or local path")
        String spec;

        @Override
        public Integer call() throws Exception {
            byte[] bytes = read(spec);
            Lkw1Object object = Lkw1Reader.read(bytes);
            StringBuilder out = new StringBuilder();
            out.append("LKW1 brokerId=").append(object.brokerId())
                .append(" epoch=").append(object.brokerEpoch())
                .append(" createdMs=").append(object.createdMs())
                .append(" chunks=").append(object.chunks().size())
                .append(System.lineSeparator());
            for (Lkw1Chunk chunk : object.chunks()) {
                out.append("  topic=").append(chunk.topicId())
                    .append(" part=").append(chunk.part())
                    .append(" nRecords=").append(chunk.nRecords())
                    .append(" nBatches=").append(chunk.nBatches())
                    .append(" maxTs=").append(chunk.maxTimestamp())
                    .append(" bytes=").append(chunk.records().length)
                    .append(System.lineSeparator());
            }
            String row = "  %-20s %-10s %-20s %-20s %-20s %-10s%n";
            out.append(String.format(row, "topic", "part", "baseOffset", "maxTimestamp", "byteStart", "length"));
            for (Lkw1Batch batch : Lkw1Reader.batches(bytes)) {
                out.append(String.format(row, batch.topicId(), batch.part(), batch.baseOffset(),
                    batch.maxTimestamp(), batch.byteStart(), batch.length()));
            }
            command.commandLine().getOut().print(out);
            return 0;
        }

        static byte[] read(String spec) throws Exception {
            if (spec.startsWith("s3://")) {
                URI uri = URI.create(spec);
                String bucket = uri.getHost();
                String key = uri.getPath();
                if (key.startsWith("/")) {
                    key = key.substring(1);
                }
                Map<String, String> env = new HashMap<>(System.getenv());
                env.putIfAbsent("LAKEWIRE_S3_BUCKET", bucket);
                S3StorageConfig base = S3StorageConfig.fromEnvironment(env);
                S3StorageConfig config = new S3StorageConfig(
                    bucket,
                    base.region(),
                    base.endpoint(),
                    base.pathStyle(),
                    base.conditionalPut(),
                    base.accessKey(),
                    base.secretKey()
                );
                try (S3ObjectStore store = new S3ObjectStore(config)) {
                    return readAll(store, key);
                }
            }
            Path path = Path.of(spec);
            if (Files.isRegularFile(path)) {
                return Files.readAllBytes(path);
            }
            String dataDir = System.getenv().getOrDefault("LAKEWIRE_DATA_DIR", "data");
            Path local = Path.of(dataDir).resolve(spec);
            if (Files.isRegularFile(local)) {
                return Files.readAllBytes(local);
            }
            throw new IllegalArgumentException("object not found: " + spec);
        }

        private static byte[] readAll(ObjectStore store, String key) throws Exception {
            ObjectHead head = store.head(key).get(30, TimeUnit.SECONDS);
            if (head.size() > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("object is larger than 2 GiB");
            }
            return store.getRange(key, 0, (int) head.size()).get(30, TimeUnit.SECONDS);
        }
    }
}
