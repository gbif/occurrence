package org.gbif.occurrence.search.es;

import org.gbif.dwc.terms.Term;

public class ChecklistEsField extends BaseEsField {

  public ChecklistEsField(String searchFieldName, Term term) {
    super(searchFieldName, term);
  }

  public String getSearchFieldName(String checklistKey) {
    return String.format(getSearchFieldName(), checklistKey);
  }
}
