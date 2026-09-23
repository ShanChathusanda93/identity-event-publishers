/*
 * Copyright (c) 2024-2026, WSO2 LLC. (http://www.wso2.com).
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

package org.wso2.identity.event.websubhub.publisher.service;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
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
import org.wso2.carbon.identity.topic.management.api.exception.TopicManagementException;
import org.wso2.carbon.identity.webhook.management.api.exception.WebhookMgtException;
import org.wso2.carbon.identity.webhook.management.api.model.Webhook;
import org.wso2.carbon.utils.DiagnosticLog;
import org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants;
import org.wso2.identity.event.websubhub.publisher.exception.WebSubAdapterException;
import org.wso2.identity.event.websubhub.publisher.internal.ClientManager;
import org.wso2.identity.event.websubhub.publisher.internal.WebSubHubAdapterDataHolder;
import org.wso2.identity.event.websubhub.publisher.util.WebSubHubCorrelationLogUtils;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Collections.emptyMap;
import static org.wso2.carbon.CarbonConstants.LogEventConstants.TENANT_ID;
import static org.wso2.carbon.identity.application.authentication.framework.util.FrameworkUtils.CORRELATION_ID_MDC;
import static org.wso2.carbon.identity.application.authentication.framework.util.FrameworkUtils.TENANT_DOMAIN;
import static org.wso2.carbon.identity.event.publisher.api.constant.ErrorMessage.ERROR_CODE_CONSTRUCTING_HUB_TOPIC;
import static org.wso2.carbon.identity.event.publisher.api.constant.ErrorMessage.ERROR_CODE_TOPIC_EXISTS_CHECK;
import static org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants.ErrorMessages.ERROR_ACTIVE_WEBHOOKS_RETRIEVAL;
import static org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants.Http.PUBLISH;
import static org.wso2.identity.event.websubhub.publisher.util.WebSubHubAdapterUtil.buildURL;
import static org.wso2.identity.event.websubhub.publisher.util.WebSubHubAdapterUtil.constructHubTopic;
import static org.wso2.identity.event.websubhub.publisher.util.WebSubHubAdapterUtil.getCorrelationID;
import static org.wso2.identity.event.websubhub.publisher.util.WebSubHubAdapterUtil.getWebSubBaseURL;
import static org.wso2.identity.event.websubhub.publisher.util.WebSubHubAdapterUtil.handleResponseCorrelationLog;
import static org.wso2.identity.event.websubhub.publisher.util.WebSubHubAdapterUtil.handleServerException;
import static org.wso2.identity.event.websubhub.publisher.util.WebSubHubAdapterUtil.printPublisherDiagnosticLog;

/**
 * OSGi service for publishing events using web sub hub.
 */
public class WebSubEventPublisherImpl implements EventPublisher {

    private static final Log log = LogFactory.getLog(WebSubEventPublisherImpl.class);
    private static final Map<String, String> TRANSMITTER_ISSUERS = new ConcurrentHashMap<>();
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

    @Override
    public String getAssociatedAdapter() {

        return WebSubHubAdapterConstants.WEB_SUB_HUB_ADAPTER_NAME;
    }

    @Override
    public void publish(SecurityEventTokenPayload eventPayload, EventContext eventContext)
            throws EventPublisherException {

        /*
         An event raised in a sub organization is delivered to the webhooks of the organizations that subscribed
         to it, which for an organization subscription is an ancestor organization rather than the one the event
         was raised in. A hub topic belongs to the tenant it was registered in, so the event is published to the
         topic of every organization that owns a webhook for it, and not to a topic of the organization the event
         was raised in, which has none.

         It is published once per organization rather than once per webhook: a topic serves every subscriber of
         its channel, so the hub fans the event out to the webhooks of that organization itself.
         */
        for (Map.Entry<String, String> recipient : resolveRecipientTenants(eventContext).entrySet()) {
            publishToTenant(eventPayload, eventContext, recipient.getKey(), recipient.getValue());
        }
    }

    /**
     * Publishes the event to the hub topic of one organization.
     *
     * @param eventPayload         Event to publish.
     * @param eventContext         Context of the event, which names the organization it was raised in.
     * @param topicTenantDomain    Tenant of the organization whose topic the event is published to.
     * @param ownerTenantDomain    Tenant of the organization owning the webhooks, or null when it is not recorded.
     * @throws EventPublisherException If the topic or the payload cannot be prepared.
     */
    private void publishToTenant(SecurityEventTokenPayload eventPayload, EventContext eventContext,
                                 String topicTenantDomain, String ownerTenantDomain) throws EventPublisherException {

        try {
            // Build immutable per-publish values.
            final String topic = constructHubTopic(
                    eventContext.getEventUri(),
                    eventContext.getEventProfileName(),
                    eventContext.getEventProfileVersion(),
                    topicTenantDomain);

            final String url = buildURL(topic, getWebSubBaseURL(), PUBLISH);
            final String bodyJson = MAPPER.writeValueAsString(stampTransmitter(eventPayload, ownerTenantDomain));

            final String correlationId = getCorrelationID(eventPayload);
            final String tenantDomain = eventContext.getTenantDomain();
            final int tenantId = IdentityTenantUtil.getTenantId(eventContext.getTenantDomain());

            final String eventProfileName = eventContext.getEventProfileName();
            final String eventProfileUri = eventContext.getEventUri();
            final String events = String.join(",", eventPayload.getEvents().keySet());

            final Map<String, String> copiedMDCSnapshot =
                    MDC.getCopyOfContextMap() != null ? MDC.getCopyOfContextMap() : emptyMap();

            sendWithRetries(eventProfileName, eventProfileUri, events, bodyJson, copiedMDCSnapshot, correlationId,
                    tenantDomain, tenantId, url,
                    WebSubHubAdapterDataHolder.getInstance().getClientManager().getMaxRetries());
        } catch (WebSubAdapterException e) {
            throw handleServerException(ERROR_CODE_CONSTRUCTING_HUB_TOPIC, e,
                    WebSubHubAdapterConstants.WEB_SUB_HUB_ADAPTER_NAME);
        } catch (JsonProcessingException e) {
            throw handleServerException(ERROR_CODE_CONSTRUCTING_HUB_TOPIC, e,
                    "Error serializing event payload");
        }
    }

    /**
     * Organizations the event is delivered to, as the tenant of each one and that tenant's identifier.
     * <p>
     * These are the organizations owning the webhooks active for the event, which for an organization
     * subscription includes ancestors of the organization the event was raised in. Each organization appears
     * once, however many of its webhooks subscribed.
     *
     * @param eventContext Context of the event.
     * @return Tenant domain of each such organization, mapped to its tenant identifier. The identifier is null
     * for a webhook that does not record its owner, where the organization the event was raised in is the best
     * available answer.
     * @throws EventPublisherException If the active webhooks cannot be retrieved.
     */
    private Map<String, String> resolveRecipientTenants(EventContext eventContext) throws EventPublisherException {

        List<Webhook> activeWebhooks;
        try {
            /*
             The webhooks whose subscription to the channel is live rather than the active ones: a subscriber
             adapter leaves a webhook partially active once the hub has accepted its subscriptions, and no later
             state replaces that, so the active ones would never include a webhook of this adapter.
             */
            activeWebhooks = WebSubHubAdapterDataHolder.getInstance().getWebhookManagementService()
                    .getSubscribedWebhooks(eventContext.getEventProfileName(),
                            eventContext.getEventProfileVersion(), eventContext.getEventUri(),
                            eventContext.getTenantDomain());
        } catch (WebhookMgtException e) {
            throw new EventPublisherServerException(ERROR_ACTIVE_WEBHOOKS_RETRIEVAL.getCode(),
                    ERROR_ACTIVE_WEBHOOKS_RETRIEVAL.getMessage(),
                    String.format(ERROR_ACTIVE_WEBHOOKS_RETRIEVAL.getDescription(), eventContext.getEventUri()), e);
        }
        if (activeWebhooks == null || activeWebhooks.isEmpty()) {
            return emptyMap();
        }
        Map<String, String> recipientTenants = new LinkedHashMap<>();
        for (Webhook webhook : activeWebhooks) {
            if (webhook.getOwnerTenantDomain() == null) {
                recipientTenants.putIfAbsent(eventContext.getTenantDomain(), null);
                continue;
            }
            recipientTenants.putIfAbsent(webhook.getOwnerTenantDomain(), webhook.getOwnerTenantDomain());
        }
        return recipientTenants;
    }

    /**
     * The event as the organization it is delivered to transmits it.
     * <p>
     * A Security Event Token names its transmitter, and the transmitter is the organization owning the webhooks
     * the topic serves. For an event raised in a descendant and delivered to an ancestor's topic that is the
     * ancestor, not the organization the event was raised in.
     * <p>
     * Only the issuer varies. The identifier of the occurrence is left as the handler set it, so the copies
     * delivered through two topics share one identifier and a consumer behind both can still recognise them as
     * the same event.
     *
     * @param eventPayload  Event as the handler built it.
     * @param ownerTenantDomain Tenant of the organization the event is delivered to, or null when it is not known.
     * @return The event to transmit.
     */
    private SecurityEventTokenPayload stampTransmitter(SecurityEventTokenPayload eventPayload,
                                                       String ownerTenantDomain) {

        if (ownerTenantDomain == null) {
            // The issuer the handler resolved is the best available answer, so it is left alone.
            return eventPayload;
        }
        try {
            return eventPayload.toBuilder()
                    .iss(resolveTransmitterIssuer(ownerTenantDomain))
                    .build();
        } catch (OrganizationManagementException | URLBuilderException e) {
            log.debug("Error while resolving the transmitter issuer of tenant: " + ownerTenantDomain +
                    ". The issuer resolved by the handler is published instead.", e);
            return eventPayload;
        }
    }

    @Override
    public boolean canHandleEvent(EventContext eventContext) throws EventPublisherException {

        /*
         Skip publishing when no active webhook is subscribed to this event. A WebSubHub topic can exist without
         any subscribed webhook resource, in which case publishing would result in a wasted hub round-trip.

         The topic is looked for in the tenant of each organization that owns such a webhook rather than in the
         one the event was raised in: an event raised in a sub organization is delivered to an ancestor's topic,
         and the organization it was raised in has no topic of its own.
         */
        Map<String, String> recipientTenants = resolveRecipientTenants(eventContext);
        if (recipientTenants.isEmpty()) {
            return false;
        }
        try {
            for (String topicTenantDomain : recipientTenants.keySet()) {
                if (WebSubHubAdapterDataHolder.getInstance().getTopicManagementService()
                        .isTopicExists(eventContext.getEventUri(), eventContext.getEventProfileName(),
                                eventContext.getEventProfileVersion(), topicTenantDomain)) {
                    return true;
                }
            }
            return false;
        } catch (TopicManagementException e) {
            throw handleServerException(ERROR_CODE_TOPIC_EXISTS_CHECK, e,
                    WebSubHubAdapterConstants.WEB_SUB_HUB_ADAPTER_NAME);
        }
    }

    private void sendWithRetries(String eventProfileName, String eventProfileUri, String events, String bodyJson,
                                 Map<String, String> mdcSnapshot, String correlationId, String tenantDomain,
                                 int tenantId, String url, int retriesLeft) {

        ClientManager clientManager = WebSubHubAdapterDataHolder.getInstance().getClientManager();

        final HttpPost request;
        try {
            request = clientManager.createHttpPost(url, bodyJson, correlationId);
        } catch (WebSubAdapterException e) {
            printPublisherDiagnosticLog(eventProfileName, eventProfileUri, events,
                    WebSubHubAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT, DiagnosticLog.ResultStatus.FAILED,
                    "Failed to construct HTTP request for WebSubHub publish.");
            log.error("Failed to construct HTTP request for WebSubHub publish. No retries will be attempted.", e);
            return;
        }

        printPublisherDiagnosticLog(eventProfileName, eventProfileUri, events,
                WebSubHubAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT,
                DiagnosticLog.ResultStatus.SUCCESS,
                "Publishing event data to WebSubHub.");
        log.debug("Event publishing to WebSubHub invoked.");

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
                        handleAsyncResponse(eventProfileName, eventProfileUri, events, response, request,
                                requestStartTime);
                    } else if (status >= 300 && status < 400) {
                        printPublisherDiagnosticLog(eventProfileName, eventProfileUri, events,
                                WebSubHubAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT,
                                DiagnosticLog.ResultStatus.FAILED,
                                "WebSubHub endpoint returned a redirection. Status code: " + status);
                        log.error("WebSubHub endpoint returned a redirection. Status code: " + status);
                    } else if (status >= 400 && status < 500) {
                        printPublisherDiagnosticLog(eventProfileName, eventProfileUri, events,
                                WebSubHubAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT,
                                DiagnosticLog.ResultStatus.FAILED,
                                "WebSubHub endpoint returned a client error. Status code: " + status);
                        log.error("WebSubHub endpoint returned a client error. Status code: " + status);
                    } else {
                        String msg = "Received server error from websubhub endpoint. Status code: " + status;
                        boolean canRetry = retriesLeft > 0;
                        String retryMsg = canRetry ? " Retrying… (" + retriesLeft + " attempts left)" :
                                " Maximum retries reached.";
                        printPublisherDiagnosticLog(eventProfileName, eventProfileUri, events,
                                WebSubHubAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT,
                                DiagnosticLog.ResultStatus.FAILED, msg + "." + retryMsg);
                        log.error(msg + ". " + retryMsg);
                        if (canRetry) {
                            sendWithRetries(eventProfileName, eventProfileUri, events, bodyJson, mdcSnapshot,
                                    correlationId, tenantDomain, tenantId, url, retriesLeft - 1);
                        } else {
                            handleResponseCorrelationLog(request, requestStartTime,
                                    WebSubHubCorrelationLogUtils.RequestStatus.FAILED.getStatus(),
                                    String.valueOf(status), response.getStatusLine().getReasonPhrase());
                            try {
                                if (response.getEntity() != null) {
                                    String body = EntityUtils.toString(response.getEntity());
                                    log.debug("Error response data: " + body);
                                } else {
                                    log.debug("WebSubHub event publisher received " + status +
                                            ". Response entity is null.");
                                }
                            } catch (IOException e) {
                                printPublisherDiagnosticLog(eventProfileName, eventProfileUri, events,
                                        WebSubHubAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT,
                                        DiagnosticLog.ResultStatus.FAILED,
                                        "Error while reading WebSubHub event publisher");
                                log.debug("Error while reading WebSubHub response.", e);
                            } finally {
                                if (response.getEntity() != null) {
                                    EntityUtils.consumeQuietly(response.getEntity());
                                }
                            }
                        }
                    }
                } else {
                    boolean shouldRetry = false;
                    String errorMsg = "Failed to publish event data to WebSubHub. ";
                    Throwable cause = throwable.getCause();
                    if (cause instanceof java.net.SocketTimeoutException ||
                            cause instanceof org.apache.http.conn.ConnectTimeoutException) {
                        errorMsg += "Request timed out.";
                        shouldRetry = true;
                    } else if (cause instanceof IOException) {
                        errorMsg += "IO error occurred.";
                        shouldRetry = true;
                    } else if (cause instanceof IllegalArgumentException) {
                        errorMsg += "Invalid request.";
                    } else {
                        errorMsg += "Unexpected error: " + throwable.getMessage();
                    }

                    if (shouldRetry && retriesLeft > 0) {
                        String retryMsg = ". Retrying… (" + retriesLeft + " attempts left)";
                        printPublisherDiagnosticLog(eventProfileName, eventProfileUri, events,
                                WebSubHubAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT,
                                DiagnosticLog.ResultStatus.FAILED, errorMsg + retryMsg);
                        log.error(errorMsg + retryMsg);
                        sendWithRetries(eventProfileName, eventProfileUri, events, bodyJson, mdcSnapshot, correlationId,
                                tenantDomain, tenantId, url, retriesLeft - 1);
                    } else {
                        handleResponseCorrelationLog(request, requestStartTime,
                                WebSubHubCorrelationLogUtils.RequestStatus.FAILED.getStatus(),
                                throwable.getMessage());
                        printPublisherDiagnosticLog(eventProfileName, eventProfileUri, events,
                                WebSubHubAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT,
                                DiagnosticLog.ResultStatus.FAILED,
                                errorMsg + (shouldRetry ? " Maximum retries reached." : ""));
                        log.error(errorMsg + ". " + (shouldRetry ? " Maximum retries reached." : ""));
                        log.debug(errorMsg, throwable);
                    }
                }
            } finally {
                if (response != null && response.getEntity() != null) {
                    EntityUtils.consumeQuietly(response.getEntity());
                }
                if (StringUtils.isNotBlank(correlationId)) {
                    MDC.remove(CORRELATION_ID_MDC);
                }
                MDC.remove(TENANT_DOMAIN);
                MDC.remove(TENANT_ID);
                PrivilegedCarbonContext.endTenantFlow();
                MDC.clear();
            }
        }, clientManager.getAsyncCallbackExecutor());
    }

    private static void handleAsyncResponse(String eventProfileName, String eventProfileUri, String events,
                                            HttpResponse response, HttpPost request, long requestStartTime) {

        int responseCode = response.getStatusLine().getStatusCode();
        String responsePhrase = response.getStatusLine().getReasonPhrase();

        log.debug("WebSubHub request completed. Response code: " + responseCode);

        handleResponseCorrelationLog(request, requestStartTime,
                WebSubHubCorrelationLogUtils.RequestStatus.COMPLETED.getStatus(),
                String.valueOf(responseCode), responsePhrase);

        printPublisherDiagnosticLog(eventProfileName, eventProfileUri, events,
                WebSubHubAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT,
                DiagnosticLog.ResultStatus.SUCCESS,
                "Event data published to WebSubHub. Status code: " + responseCode);
        try {
            if (response.getEntity() != null) {
                log.debug("Response data: " + EntityUtils.toString(response.getEntity()));
            } else {
                log.debug("Response entity is null.");
            }
        } catch (IOException e) {
            printPublisherDiagnosticLog(eventProfileName, eventProfileUri, events,
                    WebSubHubAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT,
                    DiagnosticLog.ResultStatus.FAILED,
                    "Error while reading WebSubHub event publisher response.");
            log.debug("Error while reading WebSubHub event publisher response.", e);
        } finally {
            if (response.getEntity() != null) {
                EntityUtils.consumeQuietly(response.getEntity());
            }
        }
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
            String owningOrganizationId = WebSubHubAdapterDataHolder.getInstance().getOrganizationManager()
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
