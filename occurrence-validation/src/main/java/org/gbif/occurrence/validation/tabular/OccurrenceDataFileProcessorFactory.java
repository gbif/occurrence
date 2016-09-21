package org.gbif.occurrence.validation.tabular;

import org.gbif.occurrence.processor.interpreting.result.OccurrenceInterpretationResult;
import org.gbif.occurrence.validation.DataWorkResult;
import org.gbif.occurrence.validation.FileBashUtilities;
import org.gbif.occurrence.validation.FileLineEmitterFactory;
import org.gbif.occurrence.validation.ValidationResultsAggregator;
import org.gbif.occurrence.validation.api.DataFile;
import org.gbif.occurrence.validation.api.DataFileProcessor;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.UUID;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.actor.UntypedActorFactory;
import akka.routing.RoundRobinRouter;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OccurrenceDataFileProcessorFactory extends UntypedActor implements DataFileProcessor {


  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceDataFileProcessorFactory.class);

  private static final long SLEEP_TIME_BEFORE_TERMINATION = 5000L;

  private static final int FILE_SPLIT_SIZE = 10000;

  private final String apiUrl;

  private final DataFile dataFile;

  private int numOfActors;

  private int numOfInputRecords;

  private Set<DataWorkResult> results;

  private  final ValidationResultsAggregator aggregator = new ValidationResultsAggregator();

  public OccurrenceDataFileProcessorFactory(String apiUrl, DataFile dataFile) {
    this.apiUrl = apiUrl;
    this.dataFile = dataFile;
  }

  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof DataFile) {
      runActors((DataFile)message);
    } else if (message instanceof OccurrenceInterpretationResult) {
      accumulateResults((OccurrenceInterpretationResult)message);
    } else if (message instanceof DataWorkResult) {
       results.add((DataWorkResult)message);
      if(results.size() == numOfActors) {
        getContext().stop(self());
        getContext().system().shutdown();
        LOG.info("# of records processed: " + numOfInputRecords);
        LOG.info("Results: ", aggregator.toString());
      }
    }
  }

  private void runActors(DataFile dataFile) {
    try {
      numOfInputRecords = dataFile.getNumOfLines();
      int splitSize = numOfInputRecords > FILE_SPLIT_SIZE ?
        (dataFile.getNumOfLines() / FILE_SPLIT_SIZE) : 1;
      File outDir = new File(UUID.randomUUID().toString());
      outDir.deleteOnExit();
      String outDirPath = new File(UUID.randomUUID().toString()).getAbsolutePath();
      String[] splits = FileBashUtilities.splitFile(dataFile.getFileName(), numOfInputRecords / splitSize, outDirPath);
      numOfActors = splits.length;
      ActorRef workerRouter = getContext().actorOf(new Props(new FileLineEmitterFactory(new OccurrenceLineProcessorFactory(apiUrl,
                                                                                                                           dataFile
                                                                                                                             .getDelimiterChar(),
                                                                                                                           dataFile
                                                                                                                             .getColumns())))
                                                     .withRouter(new RoundRobinRouter(splits.length)), "dataFileRouter");
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

  private void accumulateResults(OccurrenceInterpretationResult result) {
    aggregator.accumulate(result);
  }


  /**
   * This method it's mirror of the 'main' method, is kept for clarity in parameters usage.
   */
  public void process(DataFile dataFile) {

    final ActorSystem system = ActorSystem.create("DataFileProcessorSystem-" + dataFile.getFileName());
    // Create an Akka system

    // create the master
    final ActorRef master = system.actorOf(new Props(new UntypedActorFactory() {
      public UntypedActor create() {
        return this;
      }
    }), "DataFileProcessor-" + dataFile.getFileName());
    try {
      // start the calculation
      master.tell(dataFile);
      while (!master.isTerminated()) {
        try {
          Thread.sleep(SLEEP_TIME_BEFORE_TERMINATION);
        } catch (InterruptedException ie) {
          LOG.error("Thread interrupted", ie);
        }
      }
      system.shutdown();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

}
