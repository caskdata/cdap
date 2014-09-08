/*
 * Copyright 2014 Cask Data, Inc.
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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceSpecification;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.lang.InstantiatorFactory;
import co.cask.cdap.internal.app.runtime.MetricsFieldSetter;
import co.cask.cdap.internal.app.runtime.service.http.DefaultHttpServiceHandlerConfigurer;
import co.cask.cdap.internal.app.runtime.service.http.DelegatorContext;
import co.cask.cdap.internal.app.runtime.service.http.HttpHandlerFactory;
import co.cask.cdap.internal.lang.Reflections;
import co.cask.cdap.internal.service.http.DefaultHttpServiceContext;
import co.cask.cdap.internal.service.http.DefaultHttpServiceSpecification;
import co.cask.http.HttpHandler;
import co.cask.http.NettyHttpService;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnableSpecification;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Services;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * A Twill Runnable which runs a {@link NettyHttpService} with a list of {@link HttpServiceHandler}s.
 * This is the runnable which is run in the {@link HttpServiceTwillApplication}.
 */
public class HttpServiceTwillRunnable extends AbstractTwillRunnable {

  private static final Gson GSON = new Gson();
  private static final Type HANDLER_NAMES_TYPE = new TypeToken<List<String>>() { }.getType();
  private static final Type HANDLER_SPEC_TYPE = new TypeToken<List<DefaultHttpServiceSpecification>>() { }.getType();
  private static final Logger LOG = LoggerFactory.getLogger(HttpServiceTwillRunnable.class);
  private static final String CONF_RUNNABLE = "service.runnable.name";
  private static final String CONF_HANDLER = "service.runnable.handlers";
  private static final String CONF_SPEC = "service.runnable.handler.spec";
  private static final String CONF_APP = "app.name";
  private static final long HANDLER_CLEANUP_PERIOD_MS = TimeUnit.SECONDS.toMillis(60);

  // The metrics field is injected at runtime
  private Metrics metrics;
  private ClassLoader programClassLoader;
  private String serviceName;
  private String appName;
  private List<HttpServiceHandler> handlers;
  private NettyHttpService service;

  // The following two fields are for tracking GC'ed suppliers of handler and be able to call destroy on them.
  private Map<Reference<? extends Supplier<HttpServiceHandler>>, HttpServiceHandler> handlerReferences;
  private ReferenceQueue<Supplier<HttpServiceHandler>> handlerReferenceQueue;

  /**
   * Instantiates this class with a name which will be used when this service is announced
   * and a list of {@link HttpServiceHandler}s used to to handle the HTTP requests.
   *
   * @param appName the name of the app which will be used to announce the service
   * @param serviceName the name of the service which will be used to announce the service
   * @param handlers the handlers of the HTTP requests
   */
  public HttpServiceTwillRunnable(String appName, String serviceName, Iterable<? extends HttpServiceHandler> handlers) {
    this.serviceName = serviceName;
    this.handlers = ImmutableList.copyOf(handlers);
    this.appName = appName;
  }

  /**
   * Utility constructor used to instantiate the service from the program classloader.
   *
   * @param programClassLoader the classloader to instantiate the service with
   */
  public HttpServiceTwillRunnable(ClassLoader programClassLoader) {
    this.programClassLoader = programClassLoader;
  }

  /**
   * Starts the {@link NettyHttpService} and announces this runnable as well.
   */
  @Override
  public void run() {
    LOG.info("In run method in HTTP Service");
    Future<Service.State> completion = Services.getCompletionFuture(service);
    service.startAndWait();
    // announce the twill runnable
    int port = service.getBindAddress().getPort();
    Cancellable contextCancellable = getContext().announce(serviceName, port);
    LOG.info("Announced HTTP Service");

    // Create a Timer thread to periodically collect handler that are no longer in used and call destroy on it
    Timer timer = new Timer("http-handler-gc", true);
    timer.scheduleAtFixedRate(createHandlerDestroyTask(), HANDLER_CLEANUP_PERIOD_MS, HANDLER_CLEANUP_PERIOD_MS);

    try {
      completion.get();
    } catch (InterruptedException e) {
      LOG.error("Caught exception in HTTP Service run", e);
    } catch (ExecutionException e) {
      LOG.error("Caught exception in HTTP Service run", e);
    } finally {
      // once the service has been stopped, don't announce it anymore.
      contextCancellable.cancel();
      timer.cancel();

      // Go through all non-cleanup'ed handler and call destroy() upon them
      // At this point, there should be no call to any handler method, hence it's safe to call from this thread
      for (HttpServiceHandler handler : handlerReferences.values()) {
        destroyHandler(handler);
      }
    }
  }

  /**
   * Configures this runnable with the name and handler names as configs.
   *
   * @return the specification for this runnable
   */
  @Override
  public TwillRunnableSpecification configure() {
    LOG.info("In configure method in HTTP Service");
    Map<String, String> runnableArgs = Maps.newHashMap();
    runnableArgs.put(CONF_RUNNABLE, serviceName);
    List<String> handlerNames = Lists.newArrayList();
    List<HttpServiceSpecification> specs = Lists.newArrayList();
    for (HttpServiceHandler handler : handlers) {
      handlerNames.add(handler.getClass().getName());
      // call the configure method of the HTTP Handler
      DefaultHttpServiceHandlerConfigurer configurer = new DefaultHttpServiceHandlerConfigurer(handler);
      handler.configure(configurer);
      specs.add(configurer.createHttpServiceSpec());
    }
    runnableArgs.put(CONF_HANDLER, GSON.toJson(handlerNames));
    runnableArgs.put(CONF_SPEC, GSON.toJson(specs));
    runnableArgs.put(CONF_APP, appName);
    return TwillRunnableSpecification.Builder.with()
      .setName(serviceName)
      .withConfigs(ImmutableMap.copyOf(runnableArgs))
      .build();
  }

  /**
   * Initializes this runnable from the given context.
   *
   * @param context the context for initialization
   */
  @Override
  public void initialize(TwillContext context) {
    LOG.info("In initialize method in HTTP Service");
    // initialize the base class so that we can use this context later
    super.initialize(context);

    handlerReferences = Maps.newConcurrentMap();
    handlerReferenceQueue = new ReferenceQueue<Supplier<HttpServiceHandler>>();

    Map<String, String> runnableArgs = Maps.newHashMap(context.getSpecification().getConfigs());
    appName = runnableArgs.get(CONF_APP);
    serviceName = runnableArgs.get(CONF_RUNNABLE);
    handlers = Lists.newArrayList();
    List<String> handlerNames = GSON.fromJson(runnableArgs.get(CONF_HANDLER), HANDLER_NAMES_TYPE);
    List<HttpServiceSpecification> specs = GSON.fromJson(runnableArgs.get(CONF_SPEC), HANDLER_SPEC_TYPE);
    // we will need the context based on the spec when we create NettyHttpService
    List<HandlerDelegatorContext> delegatorContexts = Lists.newArrayList();
    InstantiatorFactory instantiatorFactory = new InstantiatorFactory(false);

    for (int i = 0; i < handlerNames.size(); ++i) {
      try {
        Class<?> handlerClass = programClassLoader.loadClass(handlerNames.get(i));
        @SuppressWarnings("unchecked")
        TypeToken<HttpServiceHandler> type = TypeToken.of((Class<HttpServiceHandler>) handlerClass);

        DefaultHttpServiceContext httpServiceContext =
          new DefaultHttpServiceContext(specs.get(i), context.getApplicationArguments());
        delegatorContexts.add(new HandlerDelegatorContext(type, instantiatorFactory, httpServiceContext));
      } catch (Exception e) {
        LOG.error("Could not initialize HTTP Service");
        Throwables.propagate(e);
      }
    }
    String pathPrefix = String.format("%s/apps/%s/services/%s/methods", Constants.Gateway.GATEWAY_VERSION, appName,
                                      serviceName);
    service = createNettyHttpService(context.getHost().getCanonicalHostName(), delegatorContexts, pathPrefix);
  }

  /**
   * Called when this runnable is destroyed.
   */
  @Override
  public void destroy() {
  }

  /**
   * Stops the {@link NettyHttpService} tied with this runnable.
   */
  @Override
  public void stop() {
    service.stop();
  }

  private TimerTask createHandlerDestroyTask() {
    return new TimerTask() {
      @Override
      public void run() {
        Reference<? extends Supplier<HttpServiceHandler>> ref = handlerReferenceQueue.poll();
        while (ref != null) {
          HttpServiceHandler handler = handlerReferences.remove(ref);
          if (handler != null) {
            destroyHandler(handler);
          }
          ref = handlerReferenceQueue.poll();
        }
      }
    };
  }

  private void initHandler(HttpServiceHandler handler, HttpServiceContext serviceContext) {
    try {
      handler.initialize(serviceContext);
    } catch (Throwable t) {
      LOG.error("Exception raised in HttpServiceHandler.initialize of class {}", handler.getClass(), t);
      throw Throwables.propagate(t);
    }
  }

  private void destroyHandler(HttpServiceHandler handler) {
    try {
      handler.destroy();
    } catch (Throwable t) {
      LOG.error("Exception raised in HttpServiceHandler.destroy of class {}", handler.getClass(), t);
      // Don't propagate
    }
  }

  /**
   * Creates a {@link NettyHttpService} from the given host, and list of {@link HandlerDelegatorContext}s
   *
   * @param host the host which the service will run on
   * @param delegatorContexts the list {@link HandlerDelegatorContext}
   * @param pathPrefix a string prepended to the paths which the handlers in handlerContextPairs will bind to
   * @return a NettyHttpService which delegates to the {@link HttpServiceHandler}s to handle the HTTP requests
   */
  private NettyHttpService createNettyHttpService(String host,
                                                  Iterable<HandlerDelegatorContext> delegatorContexts,
                                                  String pathPrefix) {
    // Create HttpHandlers which delegate to the HttpServiceHandlers
    HttpHandlerFactory factory = new HttpHandlerFactory(pathPrefix);
    List<HttpHandler> nettyHttpHandlers = Lists.newArrayList();
    // get the runtime args from the twill context
    for (HandlerDelegatorContext context : delegatorContexts) {
      nettyHttpHandlers.add(factory.createHttpHandler(context.getHandlerType(), context));
    }

    return NettyHttpService.builder().setHost(host)
      .setPort(0)
      .addHttpHandlers(nettyHttpHandlers)
      .build();
  }

  private final class HandlerDelegatorContext implements DelegatorContext<HttpServiceHandler> {

    private final InstantiatorFactory instantiatorFactory;
    private final ThreadLocal<Supplier<HttpServiceHandler>> handlerThreadLocal;
    private final HttpServiceContext serviceContext;
    private final TypeToken<HttpServiceHandler> handlerType;

    private HandlerDelegatorContext(TypeToken<HttpServiceHandler> handlerType,
                                    InstantiatorFactory instantiatorFactory, HttpServiceContext serviceContext) {
      this.handlerType = handlerType;
      this.instantiatorFactory = instantiatorFactory;
      this.serviceContext = serviceContext;
      this.handlerThreadLocal = new ThreadLocal<Supplier<HttpServiceHandler>>();
    }

    @Override
    public HttpServiceHandler getHandler() {
      Supplier<HttpServiceHandler> supplier = handlerThreadLocal.get();
      if (supplier != null) {
        return supplier.get();
      }
      HttpServiceHandler handler = instantiatorFactory.get(handlerType).create();
      Reflections.visit(handler, handlerType, new MetricsFieldSetter(metrics));
      initHandler(handler, serviceContext);
      supplier = Suppliers.ofInstance(handler);

      // We use GC of the supplier as a signal for us to know that a thread is gone
      // The supplier is set into the thread local, which will get GC'ed when the thread is gone.
      // Since we use a weak reference key to the supplier that points to the handler
      // (in the handlerReferences map), it won't block GC of the supplier instance.
      // We can use the weak reference, which retrieved through polling the ReferenceQueue,
      // to get back the handler and call destroy() on it.
      handlerReferences.put(new WeakReference<Supplier<HttpServiceHandler>>(supplier, handlerReferenceQueue), handler);

      handlerThreadLocal.set(supplier);
      return handler;
    }

    @Override
    public HttpServiceContext getServiceContext() {
      return serviceContext;
    }

    TypeToken<HttpServiceHandler> getHandlerType() {
      return handlerType;
    }
  }
}
