package org.gbif.occurrence.validation.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class FileBashUtilities {

  /**
   * Private constructor.
   */
  private FileBashUtilities() {
    //empty constructor
  }

  public static int countLines(String fileName) throws IOException {
    String[] out = executeSimpleCmd(String.format("wc -l %s | awk '{print $1;}'",fileName));
    return Integer.parseInt(out[0]);
  }

  public static String[] splitFile(String fileName, int splitSize, String outputDir) throws IOException {
    File outDir = new File(outputDir);
    File inFile = new File(fileName);
    Preconditions.checkArgument((outDir.exists() && outDir.isDirectory()) || !outDir.exists(),
                                "Output path is not a directory");
    Preconditions.checkArgument((outDir.exists() && outDir.list().length == 0) || !outDir.exists(),
                                "Output directory should be empty");
    Preconditions.checkArgument(inFile.exists(), "Input file doesn't exist");
    if (!outDir.exists()) {
      outDir.mkdirs();
    }

    executeSimpleCmd(String.format("split -l %s %s %s", Integer.toString(splitSize), fileName,
                                   Paths.get(outputDir,inFile.getName())));
    return outDir.list();
  }

  private static String[] executeSimpleCmd(String bashCmd) throws  IOException {
    String[] cmd = { "/bin/sh", "-c", bashCmd};
    Process process = Runtime.getRuntime().exec(cmd);
    try(BufferedReader in = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
      String line;
      List<String> out = Lists.newArrayList();
      while((line = in.readLine()) != null) {
        out.add(line);
      }
      while(process.isAlive()){
        System.out.print("waiting");
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ex){

        }
      }
      return out.toArray(new String[]{});
    } finally {
      if(process.isAlive()) {
        process.destroy();
      }
    }
  }
}
