package org.gbif.occurrence.ws.resources;

import org.gbif.occurrence.common.interpretation.InterpretationRemark;
import org.gbif.occurrence.common.interpretation.InterpretationRemarksDefinition;

import java.util.Set;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.google.inject.Singleton;

/**
 *
 * Resource related to interpretation/processing of occurrences
 *
 */
@Path("occurrence/interpretation")
@Produces(MediaType.APPLICATION_JSON)
@Singleton
public class InterpretationResource {

  private static final InterpretationRemarksWrapper INTERPRETATION_REMARKS =
          new InterpretationRemarksWrapper(InterpretationRemarksDefinition.REMARKS);

  @GET
  public InterpretationRemarksWrapper getInterpretation() {
    return INTERPRETATION_REMARKS;
  }

  /**
   * This wrapper is simply used to send the remarks under the "remarks" variable instead of root object.
   * Just in case this endpoint needs to return more in the future.
   */
  private static class InterpretationRemarksWrapper {
    private final Set<InterpretationRemark> remarks;

    public InterpretationRemarksWrapper (Set<InterpretationRemark> remarks) {
      this.remarks = remarks;
    }

    public Set<InterpretationRemark> getRemarks() {
      return remarks;
    }
  }

}
