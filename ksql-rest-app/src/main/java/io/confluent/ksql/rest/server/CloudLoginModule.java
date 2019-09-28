/*
 * Copyright 2017 Confluent Inc.
 */

package io.confluent.ksql.rest.server;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.cloud.security.SecretsService;
import org.eclipse.jetty.jaas.spi.AbstractLoginModule;
import org.eclipse.jetty.jaas.spi.UserInfo;
import org.eclipse.jetty.util.security.Credential;

import java.util.List;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;

import com.google.common.collect.ImmutableList;

public class CloudLoginModule extends AbstractLoginModule {


  private SecretsService secretsService;
  private List<String> roles = ImmutableList.of("ccloud");

  private static final String JAAS_ENTRY_CONFIG = "config_path";
  private static final String JAAS_ENTRY_REFRESH_MS = "refresh_ms";
  private static final String ROLE_CONFIG = "role";


  @Override
  public void initialize(
      Subject subject,
      CallbackHandler callbackHandler,
      Map<String, ?> sharedState,
      Map<String, ?> options
  ) {
    super.initialize(subject, callbackHandler, sharedState, options);
    loadSecretsFile(options);
  }

  @SuppressFBWarnings("IS2_INCONSISTENT_SYNC")
  private synchronized void loadSecretsFile(Map<String, ?> options) {
    if (secretsService == null) {
      String fileName = (String) options.get(JAAS_ENTRY_CONFIG);
      Long refreshInterval = Long.valueOf((String) options.get(JAAS_ENTRY_REFRESH_MS));
      secretsService = SecretsService.getInstance();
      secretsService.initialize(fileName, refreshInterval);
    }
    if (options.containsKey(ROLE_CONFIG)) {
      roles = new ImmutableList.Builder<String>() .addAll(roles).add(options.get(ROLE_CONFIG).toString()).build();
    }
  }

  @Override
  public UserInfo getUserInfo(String userName) throws Exception {
    Credential credential = secretsService.getCredential(userName);
    if (credential != null) {
      return new UserInfo(userName, credential, roles);
    } else {
      return null;
    }
  }
}