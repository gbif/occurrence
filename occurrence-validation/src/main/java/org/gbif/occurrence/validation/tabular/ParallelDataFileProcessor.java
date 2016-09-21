package org.gbif.occurrence.validation.tabular;

import org.gbif.occurrence.processor.interpreting.result.OccurrenceInterpretationResult;
import org.gbif.occurrence.validation.DataWorkResult;
import org.gbif.occurrence.validation.FileBashUtilities;
import org.gbif.occurrence.validation.FileLineEmitterFactory;
import org.gbif.occurrence.validation.api.DataFile;
import org.gbif.occurrence.validation.api.DataFileProcessor;
import org.gbif.occurrence.validation.api.RecordProcessorFactory;
import org.gbif.occurrence.validation.api.ResultsCollector;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.UUID;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.routing.RoundRobinRouter;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParallelDataFileProcessor extends UntypedActor implements DataFileProcessor {


  private static final Logger LOG = LoggerFactory.getLogger(ParallelDataFileProcessor.class);


  private static final int FILE_SPLIT_SIZE = 10000;

  private int numOfActors;

  private int numOfInputRecords;

  private Set<DataWorkResult> results;

  private final ResultsCollector<OccurrenceInterpretationResult> collector;

  private final RecordProcessorFactory<OccurrenceInterpretationResult> recordProcessorFactory;

  public ParallelDataFileProcessor(ResultsCollector collector, RecordProcessorFactory recordProcessorFactory) {
    this.collector = collector;
    this.recordProcessorFactory = recordProcessorFactory;
  }

  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof OccurrenceInterpretationResult) {
      collector.accumulate((OccurrenceInterpretationResult)message);
    } else if (message instanceof DataWorkResult) {
       results.add((DataWorkResult)message);
      System.out.println(message);
      if(results.size() == numOfActors) {
        getContext().stop(self());
        getContext().system().shutdown();
        LOG.info("# of records processed: " + numOfInputRecords);
        LOG.info("Results: " + collector);
      }
    }
  }

  @Override
  public void process(DataFile dataFile) {
    try {
      numOfInputRecords = dataFile.getNumOfLines();
      int splitSize = numOfInputRecords > FILE_SPLIT_SIZE ?
        (dataFile.getNumOfLines() / FILE_SPLIT_SIZE) : 1;
      File outDir = new File(UUID.randomUUID().toString());
      outDir.deleteOnExit();
      String outDirPath = new File(UUID.randomUUID().toString()).getAbsolutePath();
      String[] splits = FileBashUtilities.splitFile(dataFile.getFileName(), numOfInputRecords / splitSize, outDirPath);
      numOfActors = splits.length;
      ActorRef workerRouter = getContext().actorOf(new Props(new FileLineEmitterFactory(recordProcessorFactory))
                                                     .withRouter(new RoundRobinRouter(numOfActors)), "dataFileRouter");
      results = Sets.newHashSetWithExpectedSize(numOfActors);
      for(int i = 0; i < splits.length; i++) {
        DataFile dataInputSplitFile = new DataFile();
        File splitFile = new File(outDirPath, splits[i]);
        splitFile.deleteOnExit();
        dataInputSplitFile.setFileName(splitFile.getAbsolutePath());
        dataInputSplitFile.setColumns(dataFile.getColumns());
        dataInputSplitFile.setHasHeaders(dataFile.isHasHeaders() && (i == 0));
        workerRouter.tell(dataInputSplitFile,self());
      }

    } catch (IOException ex) {
      getSender().tell("Error");
    }
  }

}
