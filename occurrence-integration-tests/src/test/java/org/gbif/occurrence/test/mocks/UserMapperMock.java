package org.gbif.occurrence.test.mocks;

import org.gbif.api.model.common.GbifUser;
import org.gbif.api.model.common.paging.Pageable;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.UserRole;
import org.gbif.registry.persistence.mapper.UserMapper;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

public class UserMapperMock implements UserMapper {

  private Map<String,GbifUser> users = new HashMap<>();

  private AtomicInteger keyGen = new AtomicInteger();

  @Override
  public void create(GbifUser gbifUser) {
    gbifUser.setKey(keyGen.incrementAndGet());
    users.put(gbifUser.getUserName(), gbifUser);
  }

  @Override
  public GbifUser get(String userName) {
    return users.get(userName);
  }

  @Override
  public GbifUser getByKey(int key) {
    return users.values().stream().filter(user -> key == user.getKey()).findFirst().orElse(null);
  }

  @Override
  public GbifUser getByEmail(String email) {
    return users.values().stream().filter(user -> email.equals(user.getEmail())).findFirst().orElse(null);
  }

  @Override
  public GbifUser getBySystemSetting(String key, String value) {
    return users.values().stream().filter(user -> value.equals(user.getSystemSettings().get(key))).findFirst().orElse(null);
  }

  @Override
  public void updateLastLogin(int key) {
    Optional.ofNullable(getByKey(key)).ifPresent(user -> user.setLastLogin(new Date()));
  }

  @Override
  public void delete(GbifUser gbifUser) {
    Optional.ofNullable(getByKey(gbifUser.getKey())).ifPresent(user -> users.remove(user.getUserName()));
  }

  @Override
  public void deleteByKey(int key) {
    Optional.ofNullable(getByKey(key)).ifPresent(user -> users.remove(user.getUserName()));
  }

  @Override
  public void update(GbifUser gbifUser) {
    users.put(gbifUser.getUserName(), gbifUser);
  }

  @Override
  public List<GbifUser> search(
      @Nullable String s,
      @Nullable Set<UserRole> role,
      @Nullable Set<UUID> editorRightsOn,
      @Nullable Set<String> namespaceRightsOn,
      @Nullable Set<Country> countryRightsOn,
      @Nullable Pageable pageable) {
    Stream<GbifUser> userStream = users.values().stream();

    if (Objects.nonNull(pageable)) {
      userStream = userStream.skip(pageable.getOffset())
        .limit(pageable.getLimit());
    }
    return userStream.collect(Collectors.toList());
  }

  @Override
  public int count(
      @Nullable String s,
      @Nullable Set<UserRole> role,
      @Nullable Set<UUID> editorRightsOn,
      @Nullable Set<String> namespaceRightsOn,
      @Nullable Set<Country> countryRightsOn) {
    return users.size();
  }

  @Override
  public List<UUID> listEditorRights(String user) {
    return null;
  }

  @Override
  public void addEditorRight(String userName, UUID uuid) {

  }

  @Override
  public void deleteEditorRight(String userName, UUID uuid) {

  }

  @Override
  public void addNamespaceRight(String userName, String namespace) {

  }

  @Override
  public void deleteNamespaceRight(String userName, String namespace) {

  }

  @Override
  public List<String> listNamespaceRights(String userName) {
    return null;
  }

  @Override
  public void addCountryRight(String userName, Country country) {

  }

  @Override
  public void deleteCountryRight(String userName, Country country) {

  }

  @Override
  public List<Country> listCountryRights(String userName) {
    return null;
  }

  @Override
  public Integer getChallengeCodeKey(Integer key) {
    return null;
  }

  @Override
  public boolean updateChallengeCodeKey(Integer key, Integer challengeCodeKey) {
    return false;
  }


}
