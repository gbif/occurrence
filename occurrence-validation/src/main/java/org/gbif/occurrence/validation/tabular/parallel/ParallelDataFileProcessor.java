package org.gbif.occurrence.validation.tabular.parallel;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.occurrence.validation.api.DataFile;
import org.gbif.occurrence.validation.api.DataFileProcessor;
import org.gbif.occurrence.validation.api.DataFileValidationResult;
import org.gbif.occurrence.validation.api.RecordProcessorFactory;
import org.gbif.occurrence.validation.api.ResultsCollector;
import org.gbif.occurrence.validation.model.RecordInterpretionBasedEvaluationResult;
import org.gbif.occurrence.validation.tabular.processor.OccurrenceLineProcessorFactory;
import org.gbif.occurrence.validation.util.FileBashUtilities;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.LongAdder;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.actor.UntypedActorFactory;
import akka.routing.RoundRobinRouter;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParallelDataFileProcessor implements DataFileProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(ParallelDataFileProcessor.class);

  private static final int FILE_SPLIT_SIZE = 10000;

  private static final long SLEEP_TIME_BEFORE_TERMINATION = 50000L;

  private final String apiUrl;


  private static class ParallelDataFileProcessorMaster extends UntypedActor {

    private final ResultsCollector<Map<OccurrenceIssue, LongAdder>> collector;
    private final RecordProcessorFactory recordProcessorFactory;

    private Set<DataWorkResult> results;
    private int numOfActors;
    private DataFile dataFile;


    public ParallelDataFileProcessorMaster(ResultsCollector collector, RecordProcessorFactory recordProcessorFactory) {
      this.collector = collector;
      this.recordProcessorFactory = recordProcessorFactory;
    }

    @Override
    public void onReceive(Object message) throws Exception {
      if (message instanceof  DataFile) {
        dataFile = (DataFile)message;
        runActors();
      } else if (message instanceof RecordInterpretionBasedEvaluationResult) {
        collector.accumulate((RecordInterpretionBasedEvaluationResult) message);
      } else if (message instanceof DataWorkResult) {
        results.add((DataWorkResult) message);
        System.out.println(message);
        if (results.size() == numOfActors) {
          getContext().stop(self());
          getContext().system().shutdown();
          LOG.info("# of records processed: " + dataFile.getNumOfLines());
          LOG.info("Results: " + collector);
        }
      }
    }

    public void runActors() {
      try {
        int numOfInputRecords = dataFile.getNumOfLines();
        int splitSize = numOfInputRecords > FILE_SPLIT_SIZE ?
          (dataFile.getNumOfLines() / FILE_SPLIT_SIZE) : 1;
        File outDir = new File(UUID.randomUUID().toString());
        outDir.deleteOnExit();
        String outDirPath = outDir.getAbsolutePath();
        String[] splits = FileBashUtilities.splitFile(dataFile.getFileName(), numOfInputRecords / splitSize, outDirPath);
        numOfActors = splits.length;
        ActorRef workerRouter = getContext().actorOf(new Props(new SingleFileReaderFactory(recordProcessorFactory))
                                                       .withRouter(new RoundRobinRouter(numOfActors)), "dataFileRouter");
        results = Sets.newHashSetWithExpectedSize(numOfActors);

        for(int i = 0; i < splits.length; i++) {
          DataFile dataInputSplitFile = new DataFile();
          File splitFile = new File(outDirPath, splits[i]);
          splitFile.deleteOnExit();
          dataInputSplitFile.setFileName(splitFile.getAbsolutePath());
          dataInputSplitFile.setColumns(dataFile.getColumns());
          dataInputSplitFile.setHasHeaders(dataFile.isHasHeaders() && (i == 0));

          workerRouter.tell(dataInputSplitFile, self());
        }
      } catch (IOException ex) {
        LOG.error("Error processin file",ex);
      }
    }

  }

  public ParallelDataFileProcessor(String apiUrl) {
    this.apiUrl = apiUrl;
  }

  @Override
  public DataFileValidationResult process(DataFile dataFile) {
    ConcurrentValidationCollector validationCollector = new ConcurrentValidationCollector();
    final ActorSystem system = ActorSystem.create("DataFileProcessorSystem");
    // Create an Akka system

    // create the master
    final ActorRef master = system.actorOf(new Props(new UntypedActorFactory() {
      public UntypedActor create() {
        return new ParallelDataFileProcessorMaster(validationCollector,
                                                   new OccurrenceLineProcessorFactory(apiUrl));
      }
    }), "DataFileProcessor");
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
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    } finally {
      system.shutdown();
      LOG.info("Processing time for file {}: {} seconds", dataFile.getFileName(), system.uptime());
    }
    return new DataFileValidationResult(validationCollector.getAggregatedResult());
  }
}
