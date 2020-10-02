package org.gbif.occurrence.ws.identity;

import org.gbif.api.model.common.GbifUser;
import org.gbif.api.model.occurrence.Download;
import org.gbif.registry.identity.service.UserSuretyDelegate;
import org.gbif.registry.surety.ChallengeCodeManager;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

@Component
public class OccurrenceUserSuretyDelegate implements UserSuretyDelegate {

  private final ChallengeCodeManager<Integer> challengeCodeManager;

  public OccurrenceUserSuretyDelegate(ChallengeCodeManager<Integer> challengeCodeManager) {
    this.challengeCodeManager = challengeCodeManager;
  }

  @Override
  public boolean hasChallengeCode(Integer userKey) {
    return this.challengeCodeManager.hasChallengeCode(userKey);
  }

  @Override
  public boolean isValidChallengeCode(Integer userKey, String email, UUID challengeCode) {
    return this.challengeCodeManager.isValidChallengeCode(userKey, challengeCode, email);
  }

  @Override
  public void onNewUser(GbifUser gbifUser) {
    throw new UnsupportedOperationException("OccurrenceUserSuretyDelegate does not support this operation");
  }

  @Override
  public boolean confirmUser(GbifUser gbifUser, UUID uuid) {
    throw new UnsupportedOperationException("OccurrenceUserSuretyDelegate does not support this operation");
  }

  @Override
  public boolean confirmAndNotifyUser(GbifUser gbifUser, UUID uuid) {
    throw new UnsupportedOperationException("OccurrenceUserSuretyDelegate does not support this operation");
  }

  @Override
  public boolean confirmUserAndEmail(GbifUser gbifUser, String s, UUID uuid) {
    throw new UnsupportedOperationException("OccurrenceUserSuretyDelegate does not support this operation");
  }

  @Override
  public void onDeleteUser(GbifUser gbifUser, List<Download> list) {
    throw new UnsupportedOperationException("OccurrenceUserSuretyDelegate does not support this operation");
  }

  @Override
  public void onPasswordReset(GbifUser gbifUser) {
    throw new UnsupportedOperationException("OccurrenceUserSuretyDelegate does not support this operation");
  }

  @Override
  public void onPasswordChanged(GbifUser gbifUser) {
    throw new UnsupportedOperationException("OccurrenceUserSuretyDelegate does not support this operation");
  }

  @Override
  public void onChangeEmail(GbifUser gbifUser, String newEmail) {
    throw new UnsupportedOperationException("OccurrenceUserSuretyDelegate does not support this operation");
  }

  @Override
  public void onEmailChanged(GbifUser gbifUser, String oldEmail) {
    throw new UnsupportedOperationException("OccurrenceUserSuretyDelegate does not support this operation");
  }
}
