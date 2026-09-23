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
import org.wso2.carbon.identity.core.util.IdentityUtil;
import org.wso2.carbon.identity.event.publisher.api.exception.EventPublisherException;
import org.wso2.carbon.identity.event.publisher.api.exception.EventPublisherServerException;
import org.wso2.carbon.identity.event.publisher.api.model.EventContext;
import org.wso2.carbon.identity.event.publisher.api.model.SecurityEventTokenPayload;
import org.wso2.carbon.identity.event.publisher.api.service.EventPublisher;
import org.wso2.carbon.identity.organization.management.service.OrganizationManager;
import org.wso2.carbon.identity.organization.management.service.exception.OrganizationManagementException;
import org.wso2.carbon.identity.organization.management.service.util.OrganizationManagementUtil;
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

        String eventTenantDomain = eventContext.getTenantDomain();
        if (!isOrganizationAwarePublishing(eventTenantDomain)) {
            publishToTenant(eventPayload, eventContext, eventTenantDomain, null);
            return;
        }
        // A sub organization has no topic of its own, so the event goes to each webhook-owning organization's topic.
        EventPublisherException firstFailure = null;
        for (Map.Entry<String, String> recipient : resolveRecipientTenants(eventContext).entrySet()) {
            try {
                publishToTenant(eventPayload, eventContext, recipient.getKey(), recipient.getValue());
            } catch (EventPublisherException e) {
                // A failure for one organization must not stop the delivery to the others.
                log.error("Error while publishing the event to the topic of tenant: " + recipient.getKey(), e);
                if (firstFailure == null) {
                    firstFailure = e;
                }
            }
        }
        if (firstFailure != null) {
            throw firstFailure;
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
     * Organizations owning a webhook subscribed to the event, each once, including ancestors of the event's one.
     *
     * @param eventContext Context of the event.
     * @return Tenant domain of each such organization, mapped to the owner to stamp as the transmitter. The owner is
     * null for a webhook that does not record it, which is served from the tenant the event was raised in.
     * @throws EventPublisherException If the subscribed webhooks cannot be retrieved.
     */
    private Map<String, String> resolveRecipientTenants(EventContext eventContext) throws EventPublisherException {

        List<Webhook> activeWebhooks;
        try {
            // Not the active webhooks: a webhook of this adapter stays partially active once the hub accepts it.
            activeWebhooks = WebSubHubAdapterDataHolder.getInstance().getWebhookManagementService()
                    .getSubscriptionAcceptedWebhooksForOrganization(eventContext.getEventProfileName(),
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
     * The event stamped with the issuer of the organization owning the topic's webhooks, as it transmits it.
     * The issuer set by the event handler is kept when the owner is not recorded or its issuer cannot be resolved.
     *
     * @param eventPayload      Event as the handler built it.
     * @param ownerTenantDomain Tenant of the organization owning the webhooks, or null when it is not recorded.
     * @return The event to transmit.
     */
    private static SecurityEventTokenPayload stampTransmitter(SecurityEventTokenPayload eventPayload,
                                                              String ownerTenantDomain) {

        if (ownerTenantDomain == null) {
            return eventPayload;
        }
        try {
            return eventPayload.toBuilder()
                    .iss(resolveTransmitterIssuer(ownerTenantDomain))
                    .build();
        } catch (OrganizationManagementException | URLBuilderException e) {
            log.warn("Could not resolve the transmitter issuer of tenant: " + ownerTenantDomain +
                    ". The issuer set by the event handler is published instead.", e);
            return eventPayload;
        }
    }

    @Override
    public boolean canHandleEvent(EventContext eventContext) throws EventPublisherException {

        String eventTenantDomain = eventContext.getTenantDomain();
        if (!isOrganizationAwarePublishing(eventTenantDomain)) {
            return isTopicExists(eventContext, eventTenantDomain);
        }
        // A sub organization's event is published to the topics of the organizations owning its webhooks.
        for (String topicTenantDomain : resolveRecipientTenants(eventContext).keySet()) {
            if (isTopicExists(eventContext, topicTenantDomain)) {
                return true;
            }
        }
        return false;
    }

    private boolean isTopicExists(EventContext eventContext, String topicTenantDomain)
            throws EventPublisherException {

        try {
            return WebSubHubAdapterDataHolder.getInstance().getTopicManagementService()
                    .isTopicExists(eventContext.getEventUri(), eventContext.getEventProfileName(),
                            eventContext.getEventProfileVersion(), topicTenantDomain);
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
     * Whether the event takes the organization aware flow: child organization subscription is enabled and the event
     * was raised in a sub organization. Otherwise the event is published as before.
     *
     * @param tenantDomain Tenant the event was raised in.
     * @return true if the event is published to the organizations owning its webhooks.
     */
    private static boolean isOrganizationAwarePublishing(String tenantDomain) {

        return IdentityUtil.isChildOrganizationSubscriptionEnabled() && isSubOrganizationEvent(tenantDomain);
    }

    /**
     * Whether the event was raised in a sub organization, whose webhooks can belong to ancestor organizations.
     * Assumed when it cannot be determined, since that flow also serves the organization's own webhooks.
     *
     * @param tenantDomain Tenant the event was raised in.
     * @return true if the tenant is a sub organization, or if that cannot be determined.
     */
    private static boolean isSubOrganizationEvent(String tenantDomain) {

        try {
            return OrganizationManagementUtil.isOrganization(tenantDomain);
        } catch (OrganizationManagementException e) {
            log.warn("Could not check whether tenant: " + tenantDomain + " is an organization. Publishing the " +
                    "event as one raised in a sub organization.", e);
            return true;
        }
    }

    /**
     * Issuer naming the organization that owns a webhook: the tenant path of a root, or of a descendant within it.
     * The organization lookups it needs are cached by the organization management service.
     *
     * @param ownerTenantDomain Tenant of the organization owning the webhook.
     * @return The issuer to stamp on the token.
     * @throws OrganizationManagementException If the owning organization or its root cannot be resolved.
     * @throws URLBuilderException             If the public URL cannot be built.
     */
    private static String resolveTransmitterIssuer(String ownerTenantDomain)
            throws OrganizationManagementException, URLBuilderException {

        String rootTenantDomain = resolveRootTenantDomain(ownerTenantDomain);
        String path;
        if (StringUtils.equals(ownerTenantDomain, rootTenantDomain)) {
            path = "/t/" + rootTenantDomain;
        } else {
            String owningOrganizationId = WebSubHubAdapterDataHolder.getInstance().getOrganizationManager()
                    .resolveOrganizationId(ownerTenantDomain);
            path = "/t/" + rootTenantDomain + "/o/" + owningOrganizationId;
        }
        return ServiceURLBuilder.create().addPath(path).build().getAbsolutePublicURL();
    }

    /**
     * Root tenant of the owner's hierarchy, taken from the identity context when it carries one.
     * Otherwise, it is looked up, since an event can be published from a thread without that context.
     *
     * @param ownerTenantDomain Tenant of the organization owning the webhook.
     * @return Tenant domain of the root organization.
     * @throws OrganizationManagementException If the root organization cannot be resolved.
     */
    private static String resolveRootTenantDomain(String ownerTenantDomain) throws OrganizationManagementException {

        RootOrganization rootOrganization = IdentityContext.getThreadLocalIdentityContext().getRootOrganization();
        if (rootOrganization != null && StringUtils.isNotBlank(rootOrganization.getAssociatedTenantDomain())) {
            return rootOrganization.getAssociatedTenantDomain();
        }
        OrganizationManager organizationManager = WebSubHubAdapterDataHolder.getInstance().getOrganizationManager();
        String primaryOrganizationId = organizationManager.getPrimaryOrganizationId(
                organizationManager.resolveOrganizationId(ownerTenantDomain));
        String rootTenantDomain = StringUtils.isBlank(primaryOrganizationId) ? null :
                organizationManager.resolveTenantDomain(primaryOrganizationId);
        if (StringUtils.isBlank(rootTenantDomain)) {
            throw new OrganizationManagementException("Root organization of tenant: " + ownerTenantDomain +
                    " cannot be resolved.");
        }
        return rootTenantDomain;
    }
}
