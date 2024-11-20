package pl.ipipan.fastcorporaindexer;

import java.lang.reflect.Method;
import java.nio.file.Path;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Main entry class for Fast Corpora Indexer command line application.
 *
 * @author Mateusz Kamiński
 */
public interface FastCorporaIndexer {

    static void main(String... args) {
        Method commandMethod = CommandLine.getCommandMethods(FastCorporaIndexer.class, "indexCorpora").getFirst();
        CommandLine cmd = new CommandLine(commandMethod);
        int exitCode = cmd.execute(args);
        System.exit(exitCode);
    }

    @Command(
            description = "Creates a new index or appends to existing index based on source corpora files.",
            version = "1.0.0",
            mixinStandardHelpOptions = true,
            helpCommand = true,
            showDefaultValues = true,
            name = "fast-corpora-indexer"
    )
    static int indexCorpora(@Option(names = {"-s", "--source-corpora-dir"},
                                    required = true,
                                    description = "Path to source corpora directory which consist of subdirectories with metadata and morph files.")
                            Path sourceCorporaDir,

                            @Option(names = {"-t", "--target-index-dir"},
                                    required = true,
                                    description = "Path to target directory for Lucene index segments, existing or a new one.")
                            Path targetIndexDir,

                            @Option(names = {"-c", "--configuration-dir"},
                                    required = true,
                                    description = "Path to directory containing index configuration files like schema.xml, mtas.xml and ccl.xml.")
                            Path configurationFilesDir,

                            @Option(names = {"-tf", "--tokenizer-file"},
                                    description = "Tokenizer configuration file inside configuration files directory which is compatible for source corpora files to be indexed.",
                                    defaultValue = "ccl.xml")
                            String tokenizerConfigurationFile,

                            @Option(names = {"-rb", "--ram-buffer-size"},
                                    description = "RAM buffer size for Lucene indexing process.",
                                    defaultValue = "4096.0")
                            double ramBufferSizeMB,

                            @Option(names = {"-cf", "--commit-frequency"},
                                    description = "Docs count after hard commit should be made.",
                                    defaultValue = "10000")
                            int commitFrequency,

                            @Option(names = {"-p", "--parallel"},
                                    description = "Should be indexing processed in parallel.",
                                    defaultValue = "true")
                            boolean processInParallel,

                            @Option(names = {"-oo", "--only-optimize"},
                                    description = "Should be files indexing be omitted for only optimizing index process.",
                                    defaultValue = "false")
                            boolean onlyOptimize) {
        try {
            CreateCorporaIndex.indexCorpora(
                    sourceCorporaDir,
                    targetIndexDir,
                    configurationFilesDir,
                    ramBufferSizeMB,
                    commitFrequency,
                    processInParallel,
                    onlyOptimize,
                    tokenizerConfigurationFile
            );
            return CommandLine.ExitCode.OK;
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace(System.err);
            return CommandLine.ExitCode.SOFTWARE;
        }
    }
}
