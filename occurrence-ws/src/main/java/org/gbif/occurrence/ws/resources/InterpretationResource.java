package org.gbif.occurrence.ws.resources;

import org.gbif.occurrence.common.interpretation.InterpretationRemark;
import org.gbif.occurrence.common.interpretation.InterpretationRemarkClassification;

import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.google.inject.Singleton;

/**
 *
 * Resource related to interpretation and processing
 *
 */
@Path("occurrence/interpretation")
@Produces(MediaType.APPLICATION_JSON)
@Singleton
public class InterpretationResource {

  @GET
  @Path("remarks")
  public List<InterpretationRemark> getInterpretationRemark() {
    return InterpretationRemarkClassification.CLASSIFICATION;
  }

}
