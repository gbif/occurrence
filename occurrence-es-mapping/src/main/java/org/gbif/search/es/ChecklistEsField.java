package org.gbif.search.es;

import org.gbif.dwc.terms.Term;

public class ChecklistEsField extends BaseEsField {

  public ChecklistEsField(String searchFieldName, Term term) {
    super(searchFieldName, term);
  }

  public ChecklistEsField(String searchFieldName, Term term, String nestedPath) {
    super(searchFieldName, term, nestedPath);
  }

  public String getSearchFieldName(String checklistKey) {
    return String.format(getSearchFieldName(), checklistKey);
  }
}
