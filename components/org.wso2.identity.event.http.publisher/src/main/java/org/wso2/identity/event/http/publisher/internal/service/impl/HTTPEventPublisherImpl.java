/*
 * Copyright (c) 2025, WSO2 LLC. (http://www.wso2.com).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.identity.event.http.publisher.internal.service.impl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.util.EntityUtils;
import org.slf4j.MDC;
import org.wso2.carbon.context.PrivilegedCarbonContext;
import org.wso2.carbon.identity.core.ServiceURLBuilder;
import org.wso2.carbon.identity.core.URLBuilderException;
import org.wso2.carbon.identity.core.context.IdentityContext;
import org.wso2.carbon.identity.core.context.model.RootOrganization;
import org.wso2.carbon.identity.core.util.IdentityTenantUtil;
import org.wso2.carbon.identity.event.publisher.api.exception.EventPublisherException;
import org.wso2.carbon.identity.event.publisher.api.exception.EventPublisherServerException;
import org.wso2.carbon.identity.event.publisher.api.model.EventContext;
import org.wso2.carbon.identity.event.publisher.api.model.SecurityEventTokenPayload;
import org.wso2.carbon.identity.event.publisher.api.service.EventPublisher;
import org.wso2.carbon.identity.organization.management.service.exception.OrganizationManagementException;
import org.wso2.carbon.identity.webhook.management.api.exception.WebhookMgtException;
import org.wso2.carbon.identity.webhook.management.api.model.Webhook;
import org.wso2.carbon.utils.DiagnosticLog;
import org.wso2.identity.event.http.publisher.api.exception.HTTPAdapterException;
import org.wso2.identity.event.http.publisher.internal.component.ClientManager;
import org.wso2.identity.event.http.publisher.internal.component.HTTPAdapterDataHolder;
import org.wso2.identity.event.http.publisher.internal.constant.HTTPAdapterConstants;
import org.wso2.identity.event.http.publisher.internal.util.HTTPAdapterUtil;
import org.wso2.identity.event.http.publisher.internal.util.HTTPCorrelationLogUtils;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Collections.emptyMap;
import static org.wso2.carbon.CarbonConstants.LogEventConstants.TENANT_ID;
import static org.wso2.carbon.identity.application.authentication.framework.util.FrameworkUtils.CORRELATION_ID_MDC;
import static org.wso2.carbon.identity.application.authentication.framework.util.FrameworkUtils.TENANT_DOMAIN;
import static org.wso2.identity.event.http.publisher.internal.constant.ErrorMessage.ERROR_ACTIVE_WEBHOOKS_RETRIEVAL;
import static org.wso2.identity.event.http.publisher.internal.util.HTTPAdapterUtil.printPublisherDiagnosticLog;
import static org.wso2.identity.event.http.publisher.internal.util.HTTPCorrelationLogUtils.handleResponseCorrelationLog;

/**
 * OSGi service for publishing events using http adapter.
 */
public class HTTPEventPublisherImpl implements EventPublisher {

    private static final Log log = LogFactory.getLog(HTTPEventPublisherImpl.class);
    private static final Map<String, String> TRANSMITTER_ISSUERS = new ConcurrentHashMap<>();

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

    @Override
    public String getAssociatedAdapter() {

        return HTTPAdapterConstants.HTTP_ADAPTER_NAME;
    }

    @Override
    public void publish(SecurityEventTokenPayload eventPayload, EventContext eventContext)
            throws EventPublisherException {

        makeAsyncAPICall(eventPayload, eventContext);
    }

    @Override
    public boolean canHandleEvent(EventContext eventContext) throws EventPublisherException {

        try {
            final List<Webhook> activeWebhooks = HTTPAdapterDataHolder.getInstance().getWebhookManagementService()
                    .getActiveWebhooks(eventContext.getEventProfileName(), eventContext.getEventProfileVersion(),
                            eventContext.getEventUri(), eventContext.getTenantDomain());
            return !activeWebhooks.isEmpty();
        } catch (WebhookMgtException e) {
            throw new EventPublisherServerException(ERROR_ACTIVE_WEBHOOKS_RETRIEVAL.getMessage(),
                    ERROR_ACTIVE_WEBHOOKS_RETRIEVAL.getDescription(), ERROR_ACTIVE_WEBHOOKS_RETRIEVAL.getCode(), e);
        }
    }

    private void makeAsyncAPICall(SecurityEventTokenPayload eventPayload, EventContext eventContext)
            throws EventPublisherServerException {

        // Freeze immutable per-publish values; reuse across retries.
        final String correlationId = HTTPAdapterUtil.getCorrelationID(eventPayload);
        final String tenantDomain = eventContext.getTenantDomain();
        final int tenantId = IdentityTenantUtil.getTenantId(eventContext.getTenantDomain());

        final String eventProfileName = eventContext.getEventProfileName();
        final String eventProfileUri = eventContext.getEventUri();
        final String events = String.join(",", eventPayload.getEvents().keySet());

        final Map<String, String> copiedMDCSnapshot =
                MDC.getCopyOfContextMap() != null ? MDC.getCopyOfContextMap() : emptyMap();

        final List<Webhook> activeWebhooks;
        try {
            activeWebhooks = HTTPAdapterDataHolder.getInstance().getWebhookManagementService()
                    .getActiveWebhooks(eventContext.getEventProfileName(), eventContext.getEventProfileVersion(),
                            eventContext.getEventUri(), eventContext.getTenantDomain());
        } catch (WebhookMgtException e) {
            throw new EventPublisherServerException(ERROR_ACTIVE_WEBHOOKS_RETRIEVAL.getMessage(),
                    ERROR_ACTIVE_WEBHOOKS_RETRIEVAL.getDescription(), ERROR_ACTIVE_WEBHOOKS_RETRIEVAL.getCode(), e);
        }

        for (Webhook webhook : activeWebhooks) {
            final String url = webhook.getEndpoint();
            final String secret;
            try {
                secret = webhook.getDecryptedSecret();
            } catch (WebhookMgtException e) {
                log.error("Error while decrypting secret for webhook: " + webhook.getId() +
                        ". Event will not be published to the endpoint: " + url, e);
                printPublisherDiagnosticLog(eventProfileName, eventProfileUri, events, url,
                        HTTPAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT, DiagnosticLog.ResultStatus.FAILED,
                        "Failed to decrypt secret for webhook: " + webhook.getName() +
                                ". Event will not be published to the endpoint: " + url);
                continue;
            }

            // A Security Event Token names its transmitter, and the transmitter is the organization
            // that owns this webhook -- for an event raised in a descendant and delivered to an
            // ancestor's webhook, that is the ancestor. The token is therefore stamped and
            // serialised per recipient rather than once per occurrence.
            //
            // Only the issuer varies. jti identifies the occurrence and is left as the handler set
            // it, so the copies delivered to two webhooks share one identifier and a consumer behind
            // both can still recognise them as the same event. RFC 8417 requires jti to be unique
            // within an event feed, which holds: each webhook is its own feed.
            final String bodyJson;
            try {
                SecurityEventTokenPayload.Builder transmission = eventPayload.toBuilder();
                // A webhook read from the database always records its owner. A webhook built in
                // memory does not, and there the issuer the handler resolved is the best available
                // answer, so it is left alone rather than guessed at.
                if (webhook.getOwnerTenantDomain() != null) {
                    transmission.iss(resolveTransmitterIssuer(webhook.getOwnerTenantDomain()));
                }
                bodyJson = MAPPER.writeValueAsString(transmission.build());
            } catch (JsonProcessingException | OrganizationManagementException
                    | URLBuilderException e) {
                log.error("Error while preparing the event for webhook: " + webhook.getId() +
                        ". Event will not be published to the endpoint: " + url, e);
                printPublisherDiagnosticLog(eventProfileName, eventProfileUri, events, url,
                        HTTPAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT, DiagnosticLog.ResultStatus.FAILED,
                        "Failed to serialize HTTP adapter payload for webhook: " + webhook.getName());
                continue;
            }

            sendWithRetries(eventProfileName, eventProfileUri, events,
                    bodyJson, copiedMDCSnapshot, correlationId, tenantDomain, tenantId, url, secret,
                    HTTPAdapterDataHolder.getInstance().getClientManager().getMaxRetries());
        }
    }

    private void sendWithRetries(String eventProfileName, String eventProfileUri, String events, String bodyJson,
                                 Map<String, String> mdcSnapshot, String correlationId, String tenantDomain,
                                 int tenantId, String url, String secret, int retriesLeft) {

        ClientManager clientManager = HTTPAdapterDataHolder.getInstance().getClientManager();

        final HttpPost request;
        try {
            request = clientManager.createHttpPost(url, bodyJson, secret);
        } catch (HTTPAdapterException e) {
            printPublisherDiagnosticLog(eventProfileName, eventProfileUri, events, url,
                    HTTPAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT, DiagnosticLog.ResultStatus.FAILED,
                    "Failed to construct HTTP request for HTTP adapter publish.");
            log.debug("Error constructing HTTP request for HTTP adapter publish. No retries will be attempted.", e);
            return;
        }

        printPublisherDiagnosticLog(eventProfileName, eventProfileUri, events, url,
                HTTPAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT, DiagnosticLog.ResultStatus.SUCCESS,
                "Publishing event data to endpoint.");

        final long requestStartTime = System.currentTimeMillis();

        CompletableFuture<HttpResponse> future = clientManager.executeAsync(request);

        future.whenCompleteAsync((response, throwable) -> {
            try {
                MDC.clear();
                if (mdcSnapshot != null && !mdcSnapshot.isEmpty()) {
                    MDC.setContextMap(mdcSnapshot);
                }
                if (StringUtils.isNotBlank(correlationId)) {
                    MDC.put(CORRELATION_ID_MDC, correlationId);
                }
                MDC.put(TENANT_DOMAIN, tenantDomain);
                MDC.put(TENANT_ID, String.valueOf(tenantId));
                PrivilegedCarbonContext.startTenantFlow();
                PrivilegedCarbonContext.getThreadLocalCarbonContext().setTenantId(tenantId);
                PrivilegedCarbonContext.getThreadLocalCarbonContext().setTenantDomain(tenantDomain);

                if (throwable == null) {
                    int status = response.getStatusLine().getStatusCode();
                    if (status >= 200 && status < 300) {
                        handleResponseCorrelationLog(request, requestStartTime,
                                HTTPCorrelationLogUtils.RequestStatus.COMPLETED.getStatus(),
                                String.valueOf(status), response.getStatusLine().getReasonPhrase());
                        printPublisherDiagnosticLog(eventProfileName, eventProfileUri, events, url,
                                HTTPAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT,
                                DiagnosticLog.ResultStatus.SUCCESS, "Event data published to endpoint.");
                        log.debug("HTTP request completed. Response code: " + status +
                                ", Endpoint: " + url + ", Event URI: " + eventProfileUri);
                    } else if (status >= 300 && status < 400) {
                        printPublisherDiagnosticLog(eventProfileName, eventProfileUri, events, url,
                                HTTPAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT,
                                DiagnosticLog.ResultStatus.FAILED,
                                "Endpoint returned a redirection. Status code: " + status);
                        log.warn("Endpoint returned a redirection. Status code: " + status + ". Url: " + url);
                        // No retry for redirection
                    } else if (status >= 400 && status < 500) {
                        printPublisherDiagnosticLog(eventProfileName, eventProfileUri, events, url,
                                HTTPAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT,
                                DiagnosticLog.ResultStatus.FAILED,
                                "Endpoint returned a client error. Status code: " + status);
                        log.warn("Endpoint returned a client error. Status code: " + status + ". Url: " + url);
                        // No retry for client error
                    } else {
                        printPublisherDiagnosticLog(eventProfileName, eventProfileUri, events, url,
                                HTTPAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT,
                                DiagnosticLog.ResultStatus.FAILED,
                                "Received server error from endpoint. Status code: " + status +
                                        ". Retrying… (" + retriesLeft + " attempts left)");
                        log.warn("Received server error from endpoint. Status code: " + status + ". Url: " + url);
                        if (retriesLeft > 0) {
                            sendWithRetries(eventProfileName, eventProfileUri, events, bodyJson, mdcSnapshot,
                                    correlationId, tenantDomain, tenantId, url, secret, retriesLeft - 1);
                        } else {
                            handleResponseCorrelationLog(request, requestStartTime,
                                    HTTPCorrelationLogUtils.RequestStatus.FAILED.getStatus(),
                                    String.valueOf(status), response.getStatusLine().getReasonPhrase());
                            printPublisherDiagnosticLog(eventProfileName, eventProfileUri, events, url,
                                    HTTPAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT,
                                    DiagnosticLog.ResultStatus.FAILED,
                                    "Failed to publish event data to endpoint. Status code: " + status +
                                            ". Maximum retries reached.");
                            log.warn("Failed to publish event data to endpoint: " + url + ". Maximum retries reached.");
                        }
                    }
                } else {
                    // Exception handling and retry for timeouts and IO errors
                    boolean shouldRetry = false;
                    String errorMsg = "Failed to publish event data to endpoint. ";
                    if (throwable.getCause() instanceof SocketTimeoutException ||
                            throwable.getCause() instanceof ConnectTimeoutException) {
                        errorMsg += "Request timed out.";
                        shouldRetry = true;
                    } else if (throwable.getCause() instanceof IOException) {
                        errorMsg += "IO error occurred.";
                        shouldRetry = true;
                    } else if (throwable.getCause() instanceof IllegalArgumentException) {
                        errorMsg += "Invalid request.";
                    } else {
                        errorMsg += "Unexpected error: " + throwable.getMessage();
                    }

                    if (shouldRetry && retriesLeft > 0) {
                        printPublisherDiagnosticLog(eventProfileName, eventProfileUri, events, url,
                                HTTPAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT,
                                DiagnosticLog.ResultStatus.FAILED,
                                errorMsg + " Retrying… (" + retriesLeft + " attempts left)");
                        log.warn(errorMsg + " Url: " + url + " Retrying… (" + retriesLeft + " attempts left)");
                        sendWithRetries(eventProfileName, eventProfileUri, events, bodyJson, mdcSnapshot, correlationId,
                                tenantDomain, tenantId, url, secret, retriesLeft - 1);
                    } else {
                        errorMsg = errorMsg + (shouldRetry ? " Maximum retries reached." : "");
                        handleResponseCorrelationLog(request, requestStartTime,
                                HTTPCorrelationLogUtils.RequestStatus.FAILED.getStatus(),
                                throwable.getMessage());
                        printPublisherDiagnosticLog(eventProfileName, eventProfileUri, events, url,
                                HTTPAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT,
                                DiagnosticLog.ResultStatus.FAILED, errorMsg);
                        log.warn(errorMsg);
                        log.debug(errorMsg, throwable);
                    }
                }
            } finally {
                if (response != null && response.getEntity() != null) {
                    EntityUtils.consumeQuietly(response.getEntity());
                }
                if (StringUtils.isNotEmpty(correlationId)) {
                    MDC.remove(CORRELATION_ID_MDC);
                }
                MDC.remove(TENANT_DOMAIN);
                MDC.remove(TENANT_ID);
                PrivilegedCarbonContext.endTenantFlow();
                MDC.clear();
            }
        }, clientManager.getAsyncCallbackExecutor());
    }

    /**
     * Resolve the issuer that names this transmitter for a webhook's owning organization.
     * <p>
     * A Security Event Token names its transmitter, and the transmitter is the organization that owns the
     * webhook -- for an event raised in a descendant and delivered to an ancestor's webhook, that is the
     * ancestor. The issuer is that organization's public URL: the tenant alone for the root of a hierarchy,
     * and the tenant with the organization appended for anything below it.
     * <p>
     * The root of the hierarchy is read from the identity context rather than walked again, since the webhook
     * owner is always in the same hierarchy as the event being published and the context already carries it.
     * <p>
     * Cached by the owning tenant, which determines the issuer on its own: a tenant belongs to exactly one
     * hierarchy, so its root is a fixed property of it. Only an answer derived from a root organization that
     * was actually on the thread is cached. Without one the root cannot be known here and the owner stands in
     * for it, which is right for a root tenant and wrong for anything below it -- caching that would make the
     * wrong issuer stick, so it is left uncached for a later call that has the context to resolve it. The
     * cache is not bounded, which is acceptable because it holds one short string per organization that owns a
     * webhook, and the value only changes with the server's host configuration.
     *
     * @param ownerTenantDomain Tenant of the organization owning the webhook.
     * @return The issuer to stamp on the token.
     * @throws OrganizationManagementException If the owning organization cannot be resolved.
     * @throws URLBuilderException             If the public URL cannot be built.
     */
    private String resolveTransmitterIssuer(String ownerTenantDomain)
            throws OrganizationManagementException, URLBuilderException {

        String cached = TRANSMITTER_ISSUERS.get(ownerTenantDomain);
        if (cached != null) {
            return cached;
        }

        RootOrganization rootOrganization = IdentityContext.getThreadLocalIdentityContext().getRootOrganization();
        boolean rootResolved = rootOrganization != null
                && StringUtils.isNotBlank(rootOrganization.getAssociatedTenantDomain());
        String rootTenantDomain = rootResolved ? rootOrganization.getAssociatedTenantDomain() : ownerTenantDomain;

        String path;
        if (StringUtils.equals(ownerTenantDomain, rootTenantDomain)) {
            // The root of a hierarchy is addressed by its tenant alone.
            path = "/t/" + rootTenantDomain;
        } else {
            String owningOrganizationId = HTTPAdapterDataHolder.getInstance().getOrganizationManager()
                    .resolveOrganizationId(ownerTenantDomain);
            path = "/t/" + rootTenantDomain + "/o/" + owningOrganizationId;
        }

        String issuer = ServiceURLBuilder.create().addPath(path).build().getAbsolutePublicURL();
        if (rootResolved) {
            TRANSMITTER_ISSUERS.put(ownerTenantDomain, issuer);
        }
        return issuer;
    }

}
