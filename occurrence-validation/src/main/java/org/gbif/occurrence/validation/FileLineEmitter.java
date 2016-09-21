package org.gbif.occurrence.validation;

import org.gbif.occurrence.validation.api.DataFile;
import org.gbif.occurrence.validation.api.RecordProcessor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import akka.actor.UntypedActor;

public class FileLineEmitter<T> extends UntypedActor {


  private final RecordProcessor<T> recordProcessor;

  public FileLineEmitter(RecordProcessor<T> recordProcessor) {
    this.recordProcessor = recordProcessor;
  }
  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof DataFile) {
      doWork((DataFile) message);
    } else {
      unhandled(message);
    }
  }

  private void doWork(DataFile dataFile) throws IOException {

    try (BufferedReader br = new BufferedReader(new FileReader(dataFile.getFileName()))) {
      String line;
      if(dataFile.isHasHeaders()) {
        line = br.readLine();
      }
      while ((line = br.readLine()) != null) {
        getSender().tell(recordProcessor.process(line));
      }
      getSender().tell(new DataWorkResult(dataFile, DataWorkResult.Result.SUCCESS));
    } catch (Exception ex) {
      getSender().tell(new DataWorkResult(dataFile, DataWorkResult.Result.FAILED));
    }
  }


}
