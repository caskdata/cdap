/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.security.server;

import co.cask.cdap.common.ServiceBindException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.conf.SConfiguration;
import co.cask.cdap.common.discovery.ResolvingDiscoverable;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.server.ssl.SslSelectChannelConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.jboss.resteasy.plugins.server.servlet.HttpServletDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;

/**
 * Jetty service for External Authentication.
 */
public class ExternalAuthenticationServer extends AbstractIdleService {

  public static final String NAMED_EXTERNAL_AUTH = "external.auth";

  private final int port;
  private final int maxThreads;
  private final Map<String, Object> handlers;
  private final DiscoveryService discoveryService;
  private final CConfiguration configuration;
  private final SConfiguration sConfiguration;
  private final AuditLogHandler auditLogHandler;
  private Cancellable serviceCancellable;
  private final GrantAccessToken grantAccessToken;
  private final AbstractAuthenticationHandler authenticationHandler;
  private static final Logger LOG = LoggerFactory.getLogger(ExternalAuthenticationServer.class);
  private Server server;
  private InetAddress bindAddress;


  /**
   * Constants for a valid JSON response.
   */
  public static final class ResponseFields {
    public static final String TOKEN_TYPE = "token_type";
    public static final String TOKEN_TYPE_BODY = "Bearer";
    public static final String ACCESS_TOKEN = "access_token";
    public static final String EXPIRES_IN = "expires_in";
  }

  /**
   * Constants for Handler types.
   */
  public static final class HandlerType {
    public static final String AUTHENTICATION_HANDLER = "AuthenticationHandler";
    public static final String GRANT_TOKEN_HANDLER = "GrantTokenHandler";
  }

  @Inject
  public ExternalAuthenticationServer(CConfiguration configuration, SConfiguration sConfiguration,
                                      DiscoveryService discoveryService,
                                      @Named("security.handlers") Map<String, Object> handlers,
                                      @Named(NAMED_EXTERNAL_AUTH) AuditLogHandler auditLogHandler) {
    this.port = configuration.getBoolean(Constants.Security.SSL_ENABLED) ?
      configuration.getInt(Constants.Security.AuthenticationServer.SSL_PORT) :
      configuration.getInt(Constants.Security.AUTH_SERVER_BIND_PORT);
    this.maxThreads = configuration.getInt(Constants.Security.MAX_THREADS);
    this.handlers = handlers;
    this.discoveryService = discoveryService;
    this.configuration = configuration;
    this.sConfiguration = sConfiguration;
    this.grantAccessToken = (GrantAccessToken) handlers.get(HandlerType.GRANT_TOKEN_HANDLER);
    this.authenticationHandler = (AbstractAuthenticationHandler) handlers.get(HandlerType.AUTHENTICATION_HANDLER);
    this.auditLogHandler = auditLogHandler;
  }

  /**
   * Get the InetSocketAddress of the server.
   *
   * @return InetSocketAddress of server.
   */
  public InetSocketAddress getSocketAddress() {
    if (!server.isRunning()) {
      throw new IllegalStateException("Server not started yet");
    }

    // assumes we only have one connector
    final Connector connector = server.getConnectors()[0];
    return new InetSocketAddress(connector.getHost(), connector.getLocalPort());
  }

  @Override
  protected void startUp() throws Exception {
    try {
      server = new Server();

      try {
        bindAddress = InetAddress.getByName(configuration.get(Constants.Security.AUTH_SERVER_BIND_ADDRESS));
      } catch (UnknownHostException e) {
        LOG.error("Error finding host to connect to.", e);
        throw Throwables.propagate(e);
      }

      QueuedThreadPool threadPool = new QueuedThreadPool();
      threadPool.setMaxThreads(maxThreads);
      server.setThreadPool(threadPool);

      initHandlers();

      ServletContextHandler context = new ServletContextHandler();
      context.setServer(server);
      context.addServlet(HttpServletDispatcher.class, "/");
      context.addEventListener(new AuthenticationGuiceServletContextListener(handlers));
      context.setSecurityHandler(authenticationHandler);

      // Status endpoint should be handled without the authentication
      ContextHandler statusContext = new ContextHandler();
      statusContext.setContextPath(Constants.EndPoints.STATUS);
      statusContext.setServer(server);
      statusContext.setHandler(new StatusRequestHandler());

      if (configuration.getBoolean(Constants.Security.SSL_ENABLED, false)) {
        SslContextFactory sslContextFactory = new SslContextFactory();
        String keyStorePath = sConfiguration.get(Constants.Security.AuthenticationServer.SSL_KEYSTORE_PATH);
        String keyStorePassword = sConfiguration.get(Constants.Security.AuthenticationServer.SSL_KEYSTORE_PASSWORD);
        String keyStoreType = sConfiguration.get(Constants.Security.AuthenticationServer.SSL_KEYSTORE_TYPE,
                                                 Constants.Security.AuthenticationServer.DEFAULT_SSL_KEYSTORE_TYPE);
        String keyPassword = sConfiguration.get(Constants.Security.AuthenticationServer.SSL_KEYPASSWORD);

        Preconditions.checkArgument(keyStorePath != null, "Key Store Path Not Configured");
        Preconditions.checkArgument(keyStorePassword != null, "KeyStore Password Not Configured");

        sslContextFactory.setKeyStorePath(keyStorePath);
        sslContextFactory.setKeyStorePassword(keyStorePassword);
        sslContextFactory.setKeyStoreType(keyStoreType);
        if (keyPassword != null && keyPassword.length() != 0) {
          sslContextFactory.setKeyManagerPassword(keyPassword);
        }
        // TODO Figure out how to pick a certificate from key store

        SslSelectChannelConnector sslConnector = new SslSelectChannelConnector(sslContextFactory);
        sslConnector.setHost(bindAddress.getCanonicalHostName());
        sslConnector.setPort(port);
        server.setConnectors(new Connector[]{sslConnector});
      } else {
        SelectChannelConnector connector = new SelectChannelConnector();
        connector.setHost(bindAddress.getCanonicalHostName());
        connector.setPort(port);
        server.setConnectors(new Connector[]{connector});
      }

      HandlerCollection handlers = new HandlerCollection();
      handlers.addHandler(statusContext);
      handlers.addHandler(context);
      // AuditLogHandler must be last, since it needs the response that was sent to the client
      handlers.addHandler(auditLogHandler);

      server.setHandler(handlers);
    } catch (Exception e) {
      LOG.error("Error while starting Authentication Server.", e);
    }

    try {
      server.start();
    } catch (Exception e) {
      Throwable cause = Throwables.getRootCause(e);
      if (!(cause instanceof BindException)) {
        throw e;
      }
      throw new ServiceBindException("Authentication Server", bindAddress.getCanonicalHostName(), port);
    }

    // assumes we only have one connector
    final Connector connector = server.getConnectors()[0];
    serviceCancellable = discoveryService.register(ResolvingDiscoverable.of(new Discoverable() {
      @Override
      public String getName() {
        return Constants.Service.EXTERNAL_AUTHENTICATION;
      }

      @Override
      public InetSocketAddress getSocketAddress() throws RuntimeException {
        return new InetSocketAddress(connector.getHost(), connector.getLocalPort());
      }
    }));
  }

  /**
   * Initializes the handlers.
   */
  protected void initHandlers() throws Exception {
    authenticationHandler.init();
    grantAccessToken.init();
  }

  @Override
  protected Executor executor(State state) {
    final AtomicInteger id = new AtomicInteger();
    //noinspection NullableProblems
    final Thread.UncaughtExceptionHandler h = new Thread.UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
      }
    };
    return new Executor() {
      @Override
      public void execute(Runnable runnable) {
        Thread t = new Thread(runnable, String.format("ExternalAuthenticationServer-%d", id.incrementAndGet()));
        t.setUncaughtExceptionHandler(h);
        t.start();
      }
    };
  }

  @Override
  protected void shutDown() {
    try {
      serviceCancellable.cancel();
      server.stop();
      grantAccessToken.destroy();
    } catch (Exception e) {
      LOG.error("Error stopping Authentication Server.", e);
    }
  }
}
