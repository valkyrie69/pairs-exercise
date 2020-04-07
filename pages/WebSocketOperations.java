/*
 * (c) 2003-2019 MuleSoft, Inc. This software is protected under international copyright
 * law. All use of this software is subject to MuleSoft's Master Subscription Agreement
 * (or other master license agreement) separately entered into in writing between you and
 * MuleSoft. If such an agreement is not in place, you may not use the software.
 */
package com.mulesoft.connectors.ws.internal.operation;

import static com.mulesoft.connectors.ws.internal.error.WsError.CONNECTIVITY;
import static com.mulesoft.connectors.ws.internal.error.WsError.NO_SUCH_SOCKET;
import static com.mulesoft.connectors.ws.internal.error.WsError.REMOTELY_CLOSED;
import static com.mulesoft.connectors.ws.internal.error.WsError.getErrorByCode;
import static com.mulesoft.connectors.ws.internal.util.WebSocketUtils.ifRepeatable;
import static com.mulesoft.connectors.ws.internal.util.WebSocketUtils.resolveFullPath;
import static java.lang.Integer.MAX_VALUE;
import static java.util.Optional.ofNullable;
import static org.mule.runtime.api.meta.model.operation.ExecutionType.BLOCKING;
import static org.mule.runtime.extension.api.annotation.param.MediaType.TEXT_PLAIN;
import static org.mule.runtime.extension.api.error.MuleErrors.SECURITY;
import static org.mule.runtime.http.api.utils.HttpEncoderDecoderUtils.encodeSpaces;

import org.mule.runtime.api.exception.MuleException;
import org.mule.runtime.api.metadata.TypedValue;
import org.mule.runtime.core.api.MuleContext;
import org.mule.runtime.core.api.retry.policy.RetryPolicyTemplate;
import org.mule.runtime.extension.api.annotation.error.Throws;
import org.mule.runtime.extension.api.annotation.execution.Execution;
import org.mule.runtime.extension.api.annotation.param.Config;
import org.mule.runtime.extension.api.annotation.param.Connection;
import org.mule.runtime.extension.api.annotation.param.Content;
import org.mule.runtime.extension.api.annotation.param.MediaType;
import org.mule.runtime.extension.api.annotation.param.NullSafe;
import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.annotation.param.ParameterGroup;
import org.mule.runtime.extension.api.annotation.param.display.Placement;
import org.mule.runtime.extension.api.exception.ModuleException;
import org.mule.runtime.extension.api.runtime.operation.Result;
import org.mule.runtime.extension.api.runtime.process.CompletionCallback;
import org.mule.runtime.http.api.client.HttpClient;
import org.mule.runtime.http.api.domain.message.request.HttpRequest;
import org.mule.runtime.http.api.domain.message.request.HttpRequestBuilder;
import org.mule.runtime.http.api.exception.InvalidStatusCodeException;
import org.mule.runtime.http.api.ws.WebSocket;
import org.mule.runtime.http.api.ws.WebSocketCloseCode;
import org.mule.runtime.http.api.ws.WebSocketProtocol;
import org.mule.runtime.http.api.ws.exception.WebSocketConnectionException;

import com.mulesoft.connectors.ws.api.BroadcastFailure;
import com.mulesoft.connectors.ws.api.BroadcastSocketType;
import com.mulesoft.connectors.ws.api.WebSocketAttributes;
import com.mulesoft.connectors.ws.api.client.WebSocketClientSettings;
import com.mulesoft.connectors.ws.api.client.WebSocketRequestBuilder;
import com.mulesoft.connectors.ws.api.exception.NoSuchSocketException;
import com.mulesoft.connectors.ws.internal.WebSocketsConnector;
import com.mulesoft.connectors.ws.internal.client.WebSocketClient;
import com.mulesoft.connectors.ws.internal.connection.FluxCapacitor;
import com.mulesoft.connectors.ws.internal.error.BroadcastErrorProvider;
import com.mulesoft.connectors.ws.internal.error.DefaultErrorProvider;
import com.mulesoft.connectors.ws.internal.error.GroupSubscriptionErrorProvider;
import com.mulesoft.connectors.ws.internal.error.OpenOutboundSocketErrorTypeProvider;
import com.mulesoft.connectors.ws.internal.error.WsError;

import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.function.Predicate;

import javax.inject.Inject;

/**
 * Connector operations
 */
public class WebSocketOperations {

  private static final WebSocketRequestBuilder DEFAULT_REQUEST_BUILDER = new WebSocketRequestBuilder();

  @Inject
  private MuleContext muleContext;

  /**
   * Sends the given {@code content} through the given {@code socket}
   *
   * @param socketId            the id of the socket through which data is to be sent
   * @param content             the content to be sent
   * @param fluxCapacitor       the acting flux capacitor
   * @param callback            completion callback
   * @param retryPolicyTemplate the reconnection policy
   */
  @Throws(DefaultErrorProvider.class)
  public void send(String socketId,
                   @Content TypedValue<InputStream> content,
                   @Connection FluxCapacitor fluxCapacitor,
                   CompletionCallback<Void, Void> callback,
                   RetryPolicyTemplate retryPolicyTemplate) {

    doSend(socketId, content, fluxCapacitor, callback, retryPolicyTemplate, retryPolicyTemplate.isEnabled());
  }

  private void doSend(String socketId,
                      TypedValue<InputStream> content,
                      FluxCapacitor fluxCapacitor,
                      CompletionCallback<Void, Void> callback,
                      RetryPolicyTemplate retryPolicyTemplate,
                      boolean reconnect) {
    try {
      InputStream stream = content.getValue();
      ifRepeatable(stream, cursor -> cursor.mark(MAX_VALUE));

      fluxCapacitor.send(socketId, stream, content.getDataType().getMediaType()).whenComplete((v, e) -> {
        if (e != null) {
          if (reconnect && e instanceof WebSocketConnectionException) {
            fluxCapacitor.reconnectOnFailure(socketId, (WebSocketConnectionException) e, retryPolicyTemplate)
                .whenComplete((r, re) -> {
                  if (re != null) {
                    callback.error(mapException(e));
                  } else {
                    ifRepeatable(stream, InputStream::reset);
                    doSend(socketId, content, fluxCapacitor, callback, retryPolicyTemplate, false);
                  }
                });
          } else {
            callback.error(mapException(e));
          }
        } else {
          callback.success(Result.<Void, Void>builder().build());
        }
      });
    } catch (Exception e) {
      callback.error(e);
    }
  }

  /**
   * Sends the given {@code content} to all the active WebSockets that match the {@code path}, {@code socketType} and
   * {@code groups} criteria. In the case of the path, the base path configured in the config will not be considered as you
   * may be targeting both inbound and outbound sockets.
   * <p>
   * The message will be send to all the matching sockets, even though it's possible that delivery to some of them fail.
   * The operation will return a list of {@link BroadcastFailure} containing details of each of the failures. If none fail, an
   * empty list is returned
   *
   * @param content             the content to be sent
   * @param fluxCapacitor       the acting {@link FluxCapacitor}
   * @param path                the path of the sockets to be matched
   * @param socketType          the type of sockets to be matched
   * @param groups              matches sockets that belong to any of these groups
   * @param completionCallback  the {@link CompletionCallback}
   * @param retryPolicyTemplate the reconnection policy
   */
  @Throws(BroadcastErrorProvider.class)
  @Execution(BLOCKING)
  public void broadcast(@Content TypedValue<InputStream> content,
                        @Connection FluxCapacitor fluxCapacitor,
                        String path,
                        @Optional(defaultValue = "ALL") BroadcastSocketType socketType,
                        @Optional @NullSafe List<String> groups,
                        RetryPolicyTemplate retryPolicyTemplate,
                        CompletionCallback<List<BroadcastFailure>, Void> completionCallback) {

    Predicate<WebSocket> filter = newSocketFilter(socketType, groups);

    List<BroadcastFailure> failures = new LinkedList<>();
    fluxCapacitor.broadcast(content, path, filter, retryPolicyTemplate, (ws, e) -> {
      synchronized (failures) {
        failures.add(new BroadcastFailure(ws, e));
      }
    })
        .whenComplete((v, e) -> {
          if (e != null) {
            completionCallback.error(e);
          } else {
            completionCallback.success(Result.<List<BroadcastFailure>, Void>builder().output(failures).build());
          }
        });
  }

  @Throws(BroadcastErrorProvider.class)
  @Execution(BLOCKING)
  public void bulkCloseSockets(@Connection FluxCapacitor fluxCapacitor,
                               @Optional(defaultValue = "ALL") BroadcastSocketType socketType,
                               @Optional @NullSafe List<String> groups,
                               @Optional(defaultValue = "NORMAL_CLOSURE") WebSocketCloseCode closeCode,
                               @Optional(defaultValue = "") String reason,
                               CompletionCallback<Void, Void> completionCallback) {

    fluxCapacitor.bulkCloseSockets(newSocketFilter(socketType, groups), closeCode, reason).whenComplete((v, e) -> {
      if (e != null) {
        completionCallback.error(e);
      } else {
        completionCallback.success(Result.<Void, Void>builder().output(null).build());
      }
    });
  }

  private Predicate<WebSocket> newSocketFilter(BroadcastSocketType socketType, List<String> groups) {
    Predicate<WebSocket> filter = socketType.asFilter();
    if (!groups.isEmpty()) {
      filter = filter.and(ws -> ws.getGroups().stream().anyMatch(groups::contains));
    }
    return filter;
  }

  /**
   * Opens a new outbound socket
   *
   * @param uriSettings              The path/url to connect to
   * @param connectionRequestBuilder the request builder
   * @param defaultGroups            the groups to which the socket should be initially subscribed
   * @param config                   the connector's config
   * @param fluxCapacitor            the acting flux capacitor
   * @param callback                 the completion callback
   */
  @MediaType(value = TEXT_PLAIN)
  @Throws(OpenOutboundSocketErrorTypeProvider.class)
  public void openOutboundSocket(@Optional String socketId,
                                 @Placement(order = 1) @ParameterGroup(name = "URI Settings") UriSettings uriSettings,
                                 @Placement(order = 2) @ParameterGroup(
                                     name = "Connection Request") WebSocketRequestBuilder connectionRequestBuilder,
                                 @Optional @NullSafe List<String> defaultGroups,
                                 @Config WebSocketsConnector config,
                                 @Connection FluxCapacitor fluxCapacitor,
                                 CompletionCallback<String, WebSocketAttributes> callback) {
    try {
      WebSocketClient webSocketClient = fluxCapacitor.unsafeGetWebSocketClient(config);

      HttpClient client = webSocketClient.getHttpClient();
      WebSocketRequestBuilder resolvedBuilder =
          connectionRequestBuilder != null ? connectionRequestBuilder : DEFAULT_REQUEST_BUILDER;

      WebSocketClientSettings clientSettings = webSocketClient.getSettings();
      String resolvedUri;
      if (uriSettings.getUrl() == null) {
        String resolvedBasePath = clientSettings.getBasePath();
        String resolvedPath = resolvedBuilder.replaceUriParams(resolveFullPath(resolvedBasePath, uriSettings.getPath()));
        resolvedUri = resolveUri(clientSettings.getProtocol(), clientSettings.getHost(), clientSettings.getPort(), resolvedPath);
      } else {
        resolvedUri = resolvedBuilder.replaceUriParams(uriSettings.getUrl());
      }

      HttpRequest request = buildHttpRequest(resolvedUri, clientSettings, connectionRequestBuilder);

      fluxCapacitor.openOutboundSocket(config, client, request, defaultGroups, ofNullable(socketId),
                                       clientSettings.getConnectionIdleTimeoutMillis())
          .whenComplete((a, e) -> {
            if (e != null) {
              callback.error(mapException(e));
            } else {
              callback.success(Result.<String, WebSocketAttributes>builder().output("").attributes(a).build());
            }
          });

    } catch (Throwable t) {
      callback.error(mapException(t));
    }
  }

  /**
   * Closes the {@code socket}
   *
   * @param socketId      the id of the socket to be closed
   * @param closeCode     the close code
   * @param reason        the closing reason phrase
   * @param fluxCapacitor the acting flux capacitor
   * @param callback      the completion callback
   */
  @Throws(DefaultErrorProvider.class)
  public void closeSocket(String socketId,
                          @Optional(defaultValue = "NORMAL_CLOSURE") WebSocketCloseCode closeCode,
                          @Optional(defaultValue = "") String reason,
                          @Connection FluxCapacitor fluxCapacitor,
                          CompletionCallback<Void, Void> callback) {
    try {
      fluxCapacitor.close(socketId, closeCode, reason).whenComplete((a, e) -> {
        if (e != null) {
          callback.error(mapException(e));
        } else {
          callback.success(Result.<Void, Void>builder().build());
        }
      });
    } catch (Exception e) {
      callback.error(mapException(e));
    }
  }

  /**
   * Subscribes the socket of the given {@code socketId} to the given {@code groups}.
   * <p>
   * This operation can be used on the same socket as many times as necessary, each use being additive over previous ones.
   * <p>
   * Repeated groups will be ignored.
   * <p>
   * Works for both types INBOUND and OUTBOUND sockets.
   *
   * @param fluxCapacitor the {@link FluxCapacitor}
   * @param socketId      the id of the socket to be subscribed
   * @param groups        the groups that the socket will be subscribed to
   */
  @Throws(GroupSubscriptionErrorProvider.class)
  public void subscribeGroups(@Connection FluxCapacitor fluxCapacitor,
                              String socketId,
                              List<String> groups)
      throws Exception {
    try {
      fluxCapacitor.subscribeGroups(fluxCapacitor.lookupWebSocket(socketId), groups);
    } catch (Exception e) {
      throw mapException(e);
    }
  }

  /**
   * Unsubscribes the socket of the given {@code socketId} from the given {@code groups}.
   * <p>
   * This operation can be used on the same socket as many times as necessary, each use being additive over previous ones.
   * <p>
   * Works for both types INBOUND and OUTBOUND sockets.
   *
   * @param fluxCapacitor the {@link FluxCapacitor}
   * @param socketId      the id of the socket to be subscribed
   * @param groups        the groups that the socket will be unsubscribed from
   */
  @Throws(GroupSubscriptionErrorProvider.class)
  public void unsubscribeGroups(@Connection FluxCapacitor fluxCapacitor,
                                String socketId,
                                List<String> groups)
      throws Exception {

    try {
      fluxCapacitor.unsubscribeGroups(fluxCapacitor.lookupWebSocket(socketId), groups);
    } catch (Exception e) {
      throw mapException(e);
    }
  }

  private Exception mapException(Throwable t) {
    if (t instanceof CompletionException) {
      t = t.getCause();
    }

    if (t instanceof ModuleException) {
      return (ModuleException) t;
    }

    WsError errorType = CONNECTIVITY;

    if (t instanceof InvalidStatusCodeException) {
      WsError error = getErrorByCode(((InvalidStatusCodeException) t).getStatus()).orElse(null);
      if (error != null) {
        errorType = error;
      }
    } else if (t instanceof NoSuchSocketException) {
      errorType = NO_SUCH_SOCKET;
    } else if ("Remotely closed".equalsIgnoreCase(t.getMessage())) {
      errorType = REMOTELY_CLOSED;
    }

    return new ModuleException(t.getMessage(), errorType, t);
  }

  private HttpRequest buildHttpRequest(String uri,
                                       WebSocketClientSettings clientSettings,
                                       WebSocketRequestBuilder requestBuilder) {
    HttpRequestBuilder builder = requestBuilder.toRequestBuilder(clientSettings).uri(uri);

    clientSettings.getDefaultHeaders().forEach(header -> builder.addHeader(header.getKey(), header.getValue()));
    clientSettings.getDefaultQueryParams().forEach(param -> builder.addQueryParam(param.getKey(), param.getValue()));

    if (clientSettings.getAuthentication() != null) {
      try {
        clientSettings.getAuthentication().authenticate(builder);
      } catch (MuleException e) {
        throw new ModuleException(SECURITY, e);
      }
    }

    return builder.build();
  }

  private String resolveUri(WebSocketProtocol protocol, String host, Integer port, String path) {
    // Encode spaces to generate a valid HTTP request.
    return protocol.getScheme() + "://" + host + ":" + port + encodeSpaces(path);
  }

}
