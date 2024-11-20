package pl.ipipan.fastcorporaindexer;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.solr.schema.IndexSchema;

class IndexProcessor implements AutoCloseable {

    static final String DEFAULT_FIELD_CONTENT_NAME = "text_data";

    private final IndexWriter indexWriter;
    private final Stream<Path> corporaSubDirectories;
    private final IndexSchema indexSchema;
    private final int commitFrequency;

    private final AtomicInteger indexedFilesCount;
    private final Instant start;

    public IndexProcessor(IndexWriter indexWriter,
                          Path sourceCorporaDirectory,
                          IndexSchema indexSchema,
                          int commitFrequency) throws IOException {
        this.indexWriter = indexWriter;
        this.corporaSubDirectories = Files.list(sourceCorporaDirectory)
                .filter(Files::isDirectory);
        this.indexSchema = indexSchema;
        this.commitFrequency = commitFrequency;
        this.indexedFilesCount = new AtomicInteger();
        this.start = Instant.now();
    }

    public void processInParallel() {
        corporaSubDirectories
                .parallel()
                .forEach(this::processCorporaSubdirectory);
    }

    public void processSequentially() {
        corporaSubDirectories.forEach(this::processCorporaSubdirectory);
    }

    private void processCorporaSubdirectory(Path subDir) {
        try {
            System.out.println("Indexing source files from path " + subDir + ", indexed so far: " + indexedFilesCount.get()
                    + ", time elapsed so far: " + Duration.between(start, Instant.now()).toSeconds());
            Map<String, Object> metadataJson = loadMetadataJson(subDir);
            Document doc = createLuceneDocument(metadataJson);
            indexWriter.addDocument(doc);
            if (indexedFilesCount.incrementAndGet() % commitFrequency == 0) {
                indexWriter.commit();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, Object> loadMetadataJson(Path subDir) throws IOException {
        var metadataJson = Files.readString(subDir.resolve("doc.json"));

        Map<String, Object> jsonDoc = new ObjectMapper().readValue(metadataJson, Map.class);
        jsonDoc.put(DEFAULT_FIELD_CONTENT_NAME, subDir.toAbsolutePath().resolve("morph.xml").toString());
        indexSchema.getCopyFieldsMap().forEach((sourceField, copyFields) -> {
            Object sourceFieldValue = jsonDoc.get(sourceField);
            copyFields.forEach(copyField -> jsonDoc.put(copyField.getDestination().getName(), sourceFieldValue));
        });
        return jsonDoc;
    }

    private Document createLuceneDocument(Map<String, Object> metadataJson) {
        Document doc = new Document();
        indexSchema.getFields().forEach((fieldName, schemaField) -> {
            if (fieldName.equals("_version_")
                    || schemaField.getType().getTypeName().equals("int")
                    || schemaField.getType().getTypeName().equals("long")) {
                return;
            }
            if (schemaField.indexed() && metadataJson.containsKey(fieldName)) {
                doc.add(switch (schemaField.getType().getTypeName()) {
                    case "string" -> new StringField(fieldName, metadataJson.get(fieldName).toString(),
                            schemaField.stored() ? Field.Store.YES : Field.Store.NO);
                    case "text", "mtas" -> new TextField(fieldName, metadataJson.get(fieldName).toString(),
                            schemaField.stored() ? Field.Store.YES : Field.Store.NO);
                    default -> throw new IllegalStateException("Unknown type:" + schemaField.getType().getTypeName());
                });
            }
        });
        return doc;
    }

    public void optimizeAndCloseIndex(int targetSegmetsCount) throws IOException {
        System.out.println("Optimizing index...");
        try (indexWriter) {
            indexWriter.commit();
            indexWriter.forceMerge(targetSegmetsCount);
        }
        System.out.println("Index optimized!");
    }

    @Override
    public void close() throws IOException {
        this.corporaSubDirectories.close();
        if (indexWriter.isOpen()) {
            indexWriter.close();
        }
    }

    public int getIndexedFilesCount() {
        return indexedFilesCount.get();
    }

    public Instant getStart() {
        return start;
    }
}
