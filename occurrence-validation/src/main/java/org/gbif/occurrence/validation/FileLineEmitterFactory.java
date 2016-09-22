package org.gbif.occurrence.validation;

import org.gbif.occurrence.validation.api.RecordProcessorFactory;


import akka.actor.Actor;
import akka.actor.UntypedActorFactory;


public class FileLineEmitterFactory implements UntypedActorFactory {

  private RecordProcessorFactory recordProcessorFactory;

  public FileLineEmitterFactory(RecordProcessorFactory recordProcessorFactory) {
    this.recordProcessorFactory = recordProcessorFactory;
  }

  @Override
  public Actor create() throws Exception {
    return new FileLineEmitter(recordProcessorFactory.create());
  }


}
