package org.gbif.occurrence.validation.tabular.parallel;

import org.gbif.occurrence.validation.api.RecordProcessorFactory;

import akka.actor.Actor;
import akka.actor.UntypedActorFactory;

public class SingleFileReaderFactory implements UntypedActorFactory {

  private final RecordProcessorFactory recordProcessorFactory;

  public SingleFileReaderFactory(RecordProcessorFactory recordProcessorFactory) {
    this.recordProcessorFactory = recordProcessorFactory;
  }

  @Override
  public Actor create() throws Exception {
    return new SingleFileReaderActor(recordProcessorFactory.create());
  }

}
