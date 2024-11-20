# Fast Corpora Indexer

Simple but fast corpora indexer command line application.

For help type:

```shell
fast-corpora-indexer -h
```

It is advisable to run this application in screen session on servers: https://www.baeldung.com/linux/screen-command.

To run properly it also needs `.dtd` files to be placed in the same directory next to the indexer executable.
It's caused by the fact that MTAS requires xml files validation.

## Build

In order to build this application you need to have locally available:

1. JDK 21 (Temurin),
2. MTAS build on branch: https://github.com/m4ttek/mtas-textexploration/tree/mkaminski/ipi-fixes in your local maven repository.

To start building the application, type:

```shell
./mvnw clean install # for windows use mvwn.cmd
```

Should it process successfully, the executable file `target/fast-corpora-indexer` should be ready to go (still requires JRE 21 to run on target machine).

You may investigate used JVM flags in `pom.xml` file, within `<flags>` tag.
They may be adjusted editing first lines of the executable file.
