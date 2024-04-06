package org.embulk.output.databricks.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.rules.TemporaryFolder;

public class IOUtil {
  public static File createInputFile(TemporaryFolder testFolder, String header, String... data)
      throws IOException {
    File in = testFolder.newFile("embulk-output-databricks-input.csv");
    // Remove hacking double column after merging https://github.com/embulk/embulk/pull/1476.
    List<String> contents =
        Stream.concat(Stream.of(header + ",_index:double"), Stream.of(data).map(s -> s + ",1.0"))
            .collect(Collectors.toList());
    Files.write(in.toPath(), contents);
    return in;
  }
}
