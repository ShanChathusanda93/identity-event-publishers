/*
 * Copyright (c) 2025-2026, WSO2 LLC. (http://www.wso2.com).
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
import org.wso2.carbon.identity.core.util.IdentityUtil;
import org.wso2.carbon.identity.event.publisher.api.exception.EventPublisherException;
import org.wso2.carbon.identity.event.publisher.api.exception.EventPublisherServerException;
import org.wso2.carbon.identity.event.publisher.api.model.EventContext;
import org.wso2.carbon.identity.event.publisher.api.model.SecurityEventTokenPayload;
import org.wso2.carbon.identity.event.publisher.api.service.EventPublisher;
import org.wso2.carbon.identity.organization.management.service.OrganizationManager;
import org.wso2.carbon.identity.organization.management.service.exception.OrganizationManagementException;
import org.wso2.carbon.identity.organization.management.service.util.OrganizationManagementUtil;
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

        // An organization aware event is stamped per webhook owner; any other event is serialized once.
        final boolean organizationAwarePublishing = isOrganizationAwarePublishing(tenantDomain);
        String sharedBodyJson = null;
        if (!organizationAwarePublishing) {
            try {
                sharedBodyJson = MAPPER.writeValueAsString(eventPayload);
            } catch (JsonProcessingException e) {
                printPublisherDiagnosticLog(eventProfileName, eventProfileUri, events, null,
                        HTTPAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT, DiagnosticLog.ResultStatus.FAILED,
                        "Failed to serialize HTTP adapter payload.");
                return;
            }
        }

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

            final String bodyJson;
            if (organizationAwarePublishing) {
                try {
                    bodyJson = MAPPER.writeValueAsString(
                            stampTransmitter(eventPayload, webhook.getOwnerTenantDomain()));
                } catch (JsonProcessingException e) {
                    log.error("Error while serializing the event for webhook: " + webhook.getId() +
                            ". Event will not be published to the endpoint: " + url, e);
                    printPublisherDiagnosticLog(eventProfileName, eventProfileUri, events, url,
                            HTTPAdapterConstants.LogConstants.ActionIDs.PUBLISH_EVENT,
                            DiagnosticLog.ResultStatus.FAILED,
                            "Failed to serialize HTTP adapter payload for webhook: " + webhook.getName());
                    continue;
                }
            } else {
                bodyJson = sharedBodyJson;
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
     * The event stamped with the issuer of the organization owning the webhook, as that organization transmits it.
     * The issuer set by the event handler is kept when the owner is not recorded or its issuer cannot be resolved.
     *
     * @param eventPayload      Event as the handler built it.
     * @param ownerTenantDomain Tenant of the organization owning the webhook, or null when it is not recorded.
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
            String owningOrganizationId = HTTPAdapterDataHolder.getInstance().getOrganizationManager()
                    .resolveOrganizationId(ownerTenantDomain);
            path = "/t/" + rootTenantDomain + "/o/" + owningOrganizationId;
        }
        return ServiceURLBuilder.create().addPath(path).build().getAbsolutePublicURL();
    }

    /**
     * Root tenant of the owner's hierarchy, taken from the identity context when it carries one.
     * Otherwise it is looked up, since an event can be published from a thread without that context.
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
        OrganizationManager organizationManager = HTTPAdapterDataHolder.getInstance().getOrganizationManager();
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
