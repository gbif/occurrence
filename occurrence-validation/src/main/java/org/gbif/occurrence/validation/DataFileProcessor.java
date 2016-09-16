package org.gbif.occurrence.validation;

import org.gbif.occurrence.processor.interpreting.result.OccurrenceInterpretationResult;

import java.io.IOException;
import java.util.UUID;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.actor.UntypedActorFactory;
import akka.routing.RoundRobinRouter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataFileProcessor extends UntypedActor {


  private static final Logger LOG = LoggerFactory.getLogger(DataFileProcessor.class);

  private static final long SLEEP_TIME_BEFORE_TERMINATION = 5000L;

  private static final int FILE_SPLIT_SIZE = 10000;

  private String apiUrl;

  public DataFileProcessor(String apiUrl) {
    this.apiUrl = apiUrl;
  }

  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof DataInputFile) {
      runActors((DataInputFile)message);
    } else if (message instanceof OccurrenceInterpretationResult) {
      accumulateResults((OccurrenceInterpretationResult)message);
    }
  }

  private void runActors(DataInputFile dataInputFile) {
    try {
      int splitSize = dataInputFile.getNumOfLines() > FILE_SPLIT_SIZE ?
        (dataInputFile.getNumOfLines() / FILE_SPLIT_SIZE) : dataInputFile.getNumOfLines();
      String outDir = UUID.randomUUID().toString();
      String[] splits = FileBashUtilities.splitFile(dataInputFile.getFileName(), splitSize, outDir);
      ActorRef workerRouter = getContext().actorOf(new Props(new FileLineEmitterFactory(apiUrl))
                                                     .withRouter(new RoundRobinRouter(splits.length)), "dataFileRouter");
      for(String split : splits){
        DataInputFile dataInputSplitFile = new DataInputFile();
        dataInputSplitFile.setFileName(split);
        workerRouter.tell(dataInputSplitFile,self());
      }

    } catch (IOException ex) {
      getSender().tell("Error");
    }
  }

  private void accumulateResults(OccurrenceInterpretationResult occurrenceInterpretationResult) {
     LOG.info(occurrenceInterpretationResult.getUpdated().toString());
  }


  /**
   * This method it's mirror of the 'main' method, is kept for clarity in parameters usage.
   */
  public static void run(String inputFile, String apiUrl) {

    // Create an Akka system
    ActorSystem system = ActorSystem.create("DataFileProcessorSystem");

    // create the master
    ActorRef master = system.actorOf(new Props(new UntypedActorFactory() {
      public UntypedActor create() {
        return new DataFileProcessor(apiUrl);
      }
    }), "DataFileProcessor");
    try {
      int numOfLines = FileBashUtilities.countLines(inputFile);
      DataInputFile dataInputFile = new DataInputFile();
      dataInputFile.setFileName(inputFile);
      dataInputFile.setNumOfLines(numOfLines);
      // start the calculation
      master.tell(dataInputFile);
      while (!master.isTerminated()) {
        try {
          Thread.sleep(SLEEP_TIME_BEFORE_TERMINATION);
        } catch (InterruptedException ie) {
          LOG.error("Thread interrupted", ie);
        }
      }
      system.shutdown();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }
}
