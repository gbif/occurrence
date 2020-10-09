package org.gbif.occurrence.test.mocks;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.registry.Identifier;
import org.gbif.registry.security.grscicoll.GrSciCollEditorAuthorizationService;

import java.util.UUID;

public class GrSciCollEditorAuthorizationServiceMock extends GrSciCollEditorAuthorizationService {


  public GrSciCollEditorAuthorizationServiceMock() {
    super(null, null, null, null);
  }
  @Override
  public boolean isIrnIdentifier(Identifier identifier) {
    return false;
  }

  @Override
  public boolean isIrnIdentifier(String entityType, UUID entityKey, int identifierKey) {
    return false;
  }

  @Override
  public boolean allowedToModifyEntity(String username, UUID key) {
    return true;
  }

  @Override
  public boolean allowedToUpdateCollection(
    String username, UUID collectionKey, Collection collectionInMessageBody
  ) {
    return true;
  }
}
