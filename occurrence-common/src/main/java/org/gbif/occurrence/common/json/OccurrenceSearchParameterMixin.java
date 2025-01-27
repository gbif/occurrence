package org.gbif.occurrence.common.json;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;

@JsonDeserialize(as = OccurrenceSearchParameter.class)
public class OccurrenceSearchParameterMixin {}
