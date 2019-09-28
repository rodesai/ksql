package io.confluent.ksql.rest.server;

import io.confluent.common.security.jetty.JwtLoginService;
import io.confluent.common.security.jetty.JwtWithFallbackLoginService;
import io.confluent.common.security.jetty.OAuthOrBasicAuthenticator;
import io.confluent.common.security.jetty.initializer.InstallOAuthSecurityHandler;
import io.confluent.rest.RestConfig;
import io.confluent.rest.auth.AuthUtil;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.ConfigDef;
import org.eclipse.jetty.jaas.JAASLoginService;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.DefaultIdentityService;
import org.eclipse.jetty.servlet.ServletContextHandler;

import java.util.Map;
import java.util.function.Consumer;

public class HybridIntializer implements Consumer<ServletContextHandler>, Configurable {
  static final String JWT_ISSUER = "Confluent";
  static final String JWT_ROLES_CLAIM = "clusters";

  private Config config;

  private static class Config extends RestConfig {
    Config(final Map<String, ?> props) {
      super(
          baseConfigDef().define(
                  InstallOAuthSecurityHandler.OAuthConfig.JWT_PUBLIC_KEY_PATH_CONFIG,
                  ConfigDef.Type.STRING,
                  "",
                   ConfigDef.Importance.LOW, ""),
          props);
    }
  }

  @Override
  public void configure(final Map<String, ?> map) {
    config = new Config(map);
  }

  @Override
  public void accept(final ServletContextHandler context) {
    context.setSecurityHandler(createSecurityHandler());
  }

  private ConstraintSecurityHandler createSecurityHandler() {
    //Overrides the security handler provided in rest-utils which handles only Basic Auth based on
    // a config. MultiTenantSR is by default configured to be use Basic Auth + OAuth.
    //This code is similar to the one in rest-util where we set BasicAuthAuthenticator
    String realm = config.getString(RestConfig.AUTHENTICATION_REALM_CONFIG);
    final ConstraintSecurityHandler securityHandler = new ConstraintSecurityHandler();
    securityHandler.addConstraintMapping(AuthUtil.createGlobalAuthConstraint(config));
    securityHandler.setAuthenticator(new OAuthOrBasicAuthenticator());
    final String publicKeyPath = config.getString(InstallOAuthSecurityHandler.OAuthConfig.JWT_PUBLIC_KEY_PATH_CONFIG);
    JwtLoginService jwtLoginService = new JwtLoginService(
        realm,
        JWT_ISSUER,
        publicKeyPath,
        JWT_ROLES_CLAIM
    );
    securityHandler.setLoginService(
        new JwtWithFallbackLoginService(jwtLoginService, new JAASLoginService(realm)));
    //explicitly setting the DefaultIdentityService like rest-utils
    securityHandler.setIdentityService(new DefaultIdentityService());
    securityHandler.setRealmName(realm);
    AuthUtil.createUnsecuredConstraints(config).forEach(securityHandler::addConstraintMapping);
    return securityHandler;
  }
}
