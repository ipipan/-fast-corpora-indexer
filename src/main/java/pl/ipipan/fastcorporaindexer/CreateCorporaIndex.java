package pl.ipipan.fastcorporaindexer;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import mtas.analysis.util.MtasCharFilterFactory;
import mtas.analysis.util.MtasTokenizerFactory;
import mtas.codec.MtasCodec;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.util.FilesystemResourceLoader;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NRTCachingDirectory;
import org.apache.lucene.store.NativeFSLockFactory;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.schema.ClassicIndexSchemaFactory;
import org.apache.solr.schema.IndexSchema;

/**
 * Prepares Lucene indexing processor based on entry configuration.
 *
 * @author mkaminski
 */
interface CreateCorporaIndex {

    static void indexCorpora(Path sourceCorporaDirectory,
                             Path targetLuceneIndex,
                             Path configurationDirectory,
                             double maxBufferSizeMB,
                             int commitFrequency,
                             boolean processInParallel,
                             boolean onlyOptimize,
                             String tokenizerConfigurationFile) throws IOException {

        NRTCachingDirectory directory = openLuceneIndexDirectory(targetLuceneIndex);
        PerFieldAnalyzerWrapper defaultAnalyzer = new PerFieldAnalyzerWrapper(
                new StandardAnalyzer(),
                Map.of(IndexProcessor.DEFAULT_FIELD_CONTENT_NAME, createMtasAnalyzer(configurationDirectory, tokenizerConfigurationFile))
        );
        IndexWriter indexWriter = openIndexWriter(directory, defaultAnalyzer, maxBufferSizeMB);
        IndexSchema indexSchema = loadSolrSchema(configurationDirectory);

        try (var indexProcessor = new IndexProcessor(indexWriter, sourceCorporaDirectory, indexSchema, commitFrequency); directory) {
            if (!onlyOptimize) {
                if (processInParallel) {
                    indexProcessor.processInParallel();
                } else {
                    indexProcessor.processSequentially();
                }
            }
            indexProcessor.optimizeAndCloseIndex(16);
            System.out.println("Indexing " + indexProcessor.getIndexedFilesCount() + " corpora files completed in " +
                    Duration.between(indexProcessor.getStart(), Instant.now()).toSeconds() + " seconds!");
        }
    }

    private static NRTCachingDirectory openLuceneIndexDirectory(Path targetIndexDirectory) throws IOException {
        return new NRTCachingDirectory(FSDirectory.open(
                targetIndexDirectory.toAbsolutePath(),
                NativeFSLockFactory.getDefault()),
                1000,
                1000);
    }

    private static IndexWriter openIndexWriter(Directory directory,
                                               Analyzer analyzer,
                                               double ramBufferSizeMB) throws IOException {
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setUseCompoundFile(false);
        config.setCodec(new MtasCodec());
        config.setRAMBufferSizeMB(ramBufferSizeMB);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        config.setReaderPooling(false);
        return new IndexWriter(directory, config);
    }

    private static Analyzer createMtasAnalyzer(Path configFilesDirectory, String tokenizerConfFile) throws IOException {
        return CustomAnalyzer
                .builder(new FilesystemResourceLoader(
                        configFilesDirectory.toAbsolutePath(),
                        CreateCorporaIndex.class.getClassLoader()))
                .addCharFilter(MtasCharFilterFactory.class, new HashMap<>(Map.of("type", "file")))
                .withTokenizer(MtasTokenizerFactory.class, new HashMap<>(Map.of("configFile", tokenizerConfFile)))
                .build();
    }

    private static IndexSchema loadSolrSchema(Path configFilesDirectory) {
        Path solrResourcesPath = configFilesDirectory.toAbsolutePath();
        var solrResourceLoader = new SolrResourceLoader(solrResourcesPath);
        return new IndexSchema(
                solrResourcesPath.getParent().getFileName().toString(),
                () -> ClassicIndexSchemaFactory.getParsedSchema(
                        new FileInputStream(solrResourcesPath.resolve("schema.xml").toAbsolutePath().toString()),
                        solrResourceLoader,
                        "schema.xml"),
                org.apache.lucene.util.Version.LATEST,
                solrResourceLoader, null);
    }
}
