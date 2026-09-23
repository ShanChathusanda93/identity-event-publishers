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

import org.apache.http.HttpResponse;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.carbon.identity.central.log.mgt.utils.LoggerUtils;
import org.wso2.carbon.identity.core.ServiceURL;
import org.wso2.carbon.identity.core.ServiceURLBuilder;
import org.wso2.carbon.identity.core.context.IdentityContext;
import org.wso2.carbon.identity.core.context.model.RootOrganization;
import org.wso2.carbon.identity.core.util.IdentityTenantUtil;
import org.wso2.carbon.identity.core.util.IdentityUtil;
import org.wso2.carbon.identity.event.publisher.api.exception.EventPublisherException;
import org.wso2.carbon.identity.event.publisher.api.model.EventContext;
import org.wso2.carbon.identity.event.publisher.api.model.EventPayload;
import org.wso2.carbon.identity.event.publisher.api.model.SecurityEventTokenPayload;
import org.wso2.carbon.identity.organization.management.service.OrganizationManager;
import org.wso2.carbon.identity.organization.management.service.util.OrganizationManagementUtil;
import org.wso2.carbon.identity.topic.management.api.service.TopicManagementService;
import org.wso2.carbon.identity.webhook.management.api.model.Webhook;
import org.wso2.carbon.identity.webhook.management.api.service.WebhookManagementService;
import org.wso2.identity.event.websubhub.publisher.config.WebSubAdapterConfiguration;
import org.wso2.identity.event.websubhub.publisher.exception.WebSubAdapterException;
import org.wso2.identity.event.websubhub.publisher.exception.WebSubAdapterServerException;
import org.wso2.identity.event.websubhub.publisher.internal.ClientManager;
import org.wso2.identity.event.websubhub.publisher.internal.WebSubHubAdapterDataHolder;
import org.wso2.identity.event.websubhub.publisher.util.WebSubHubAdapterUtil;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import static org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants.Http.CORRELATION_ID_REQUEST_HEADER;

/**
 * Unit tests for the WebSubHubAdapterServiceImpl class.
 */
public class WebSubEventPublisherImplTest {

    private static final String CARBON_HOME = "carbon.home";
    private static final String EVENT_TENANT = "test-tenant";

    private WebSubEventPublisherImpl adapterService;
    private AutoCloseable mocks;

    @Mock
    private ClientManager mockClientManager;

    @Mock
    private WebSubAdapterConfiguration mockAdapterConfiguration;

    @Mock
    private HttpResponse mockHttpResponse;

    @Mock
    private WebhookManagementService mockWebhookManagementService;

    @Mock
    private TopicManagementService mockTopicManagementService;

    @Mock
    private OrganizationManager mockOrgManager;

    private MockedStatic<WebSubHubAdapterDataHolder> mockedStaticDataHolder;
    private MockedStatic<OrganizationManagementUtil> mockedOrganizationManagementUtil;
    private MockedStatic<IdentityUtil> mockedIdentityUtil;
    private static MockedStatic<IdentityTenantUtil> mockedStaticIdentityTenantUtil;

    @BeforeMethod
    public void setUp() throws Exception {

        // CarbonContext, the parent of IdentityContext, reads the carbon home once, so it is set before any test.
        if (System.getProperty(CARBON_HOME) == null) {
            System.setProperty(CARBON_HOME, Paths.get(System.getProperty("user.dir"), "target").toString());
        }
        mocks = MockitoAnnotations.openMocks(this);
        adapterService = spy(new WebSubEventPublisherImpl());
        mockIdentityTenantUtil();

        mockedStaticDataHolder = mockStatic(WebSubHubAdapterDataHolder.class);
        WebSubHubAdapterDataHolder mockDataHolder = mock(WebSubHubAdapterDataHolder.class);
        mockedStaticDataHolder.when(WebSubHubAdapterDataHolder::getInstance).thenReturn(mockDataHolder);

        when(mockDataHolder.getClientManager()).thenReturn(mockClientManager);
        when(mockDataHolder.getAdapterConfiguration()).thenReturn(mockAdapterConfiguration);
        when(mockAdapterConfiguration.getWebSubHubBaseUrl()).thenReturn("http://mock-websub-hub.com");

        when(mockDataHolder.getOrganizationManager()).thenReturn(mockOrgManager);
        when(mockOrgManager.resolveOrganizationId(any())).thenReturn("mock-org-id");
        when(mockDataHolder.getWebhookManagementService()).thenReturn(mockWebhookManagementService);
        when(mockDataHolder.getTopicManagementService()).thenReturn(mockTopicManagementService);

        // Events are raised in a root organization unless a test says otherwise.
        mockedOrganizationManagementUtil = mockStatic(OrganizationManagementUtil.class);
        mockedOrganizationManagementUtil.when(() -> OrganizationManagementUtil.isOrganization(anyString()))
                .thenReturn(false);
        // Child organization subscription is enabled unless a test says otherwise.
        mockedIdentityUtil = mockStatic(IdentityUtil.class);
        mockedIdentityUtil.when(IdentityUtil::isChildOrganizationSubscriptionEnabled).thenReturn(true);
    }

    @AfterMethod
    public void tearDown() throws Exception {

        if (mocks != null) {
            mocks.close();
        }
        if (mockedStaticDataHolder != null) {
            mockedStaticDataHolder.close();
        }
        if (mockedOrganizationManagementUtil != null) {
            mockedOrganizationManagementUtil.close();
        }
        if (mockedIdentityUtil != null) {
            mockedIdentityUtil.close();
        }
        if (mockedStaticIdentityTenantUtil != null) {
            mockedStaticIdentityTenantUtil.close();
        }
    }

    @Test
    public void testPublishSuccess() throws EventPublisherException, WebSubAdapterException {

        try (
                MockedStatic<LoggerUtils> mockedLoggerUtils = mockStatic(LoggerUtils.class);
                MockedStatic<WebSubHubAdapterUtil> mockedAdapterUtil = mockStatic(WebSubHubAdapterUtil.class)
        ) {
            mockedLoggerUtils.when(LoggerUtils::isDiagnosticLogsEnabled).thenReturn(false);

            // Mock static utility methods used in publish
            mockedAdapterUtil.when(() -> WebSubHubAdapterUtil.constructHubTopic(any(), any(), any(), any()))
                    .thenReturn("mock-topic");
            mockedAdapterUtil.when(WebSubHubAdapterUtil::getWebSubBaseURL)
                    .thenReturn("http://mock-websub-hub.com");
            mockedAdapterUtil.when(
                            () -> WebSubHubAdapterUtil
                                    .printPublisherDiagnosticLog(any(), any(), any(), any(), any(), any()))
                    .then(invocation -> null);

            // Mock ClientManager.getMaxRetries()
            when(mockClientManager.getMaxRetries()).thenReturn(2);

            // Mock inputs
            EventContext eventContext = EventContext.builder()
                    .tenantDomain("test-tenant")
                    .eventProfileName("WSO2")
                    .eventUri("test-uri")
                    .build();
            SecurityEventTokenPayload payload = SecurityEventTokenPayload.builder()
                    .iss("issuer")
                    .jti("jti-token")
                    .iat(System.currentTimeMillis())
                    .aud("audience")
                    .events(Collections.singletonMap("event1", new EventPayload() {
                    }))
                    .build();

            // Mock HttpPost and its header
            org.apache.http.client.methods.HttpPost mockHttpPost = mock(org.apache.http.client.methods.HttpPost.class);
            org.apache.http.Header mockHeader = mock(org.apache.http.Header.class);
            when(mockHttpPost.getFirstHeader(CORRELATION_ID_REQUEST_HEADER)).thenReturn(mockHeader);
            when(mockHeader.getValue()).thenReturn("mock-correlation-id");

            // Mock ClientManager behavior to simulate success
            CompletableFuture<HttpResponse> future = CompletableFuture.completedFuture(mockHttpResponse);
            when(mockClientManager.executeAsync(any())).thenReturn(future);
            when(mockClientManager.createHttpPost(any(), any(), any())).thenReturn(mockHttpPost);
            when(mockClientManager.getAsyncCallbackExecutor()).thenReturn((Executor) Runnable::run);

            // Execute and verify no exception is thrown
            adapterService.publish(payload, eventContext);

            // Verify interactions
            verify(mockClientManager, times(1)).executeAsync(any());
            verifyNoInteractions(mockWebhookManagementService);
        }
    }

    @Test
    public void testPublishSubOrganizationEventToTheTopicOfEachOwner() throws Exception {

        markEventTenantAsSubOrganization();
        stubSubscribedWebhooks(mockWebhook("owner-tenant-a"), mockWebhook("owner-tenant-a"),
                mockWebhook("owner-tenant-b"));
        try (MockedStatic<LoggerUtils> mockedLoggerUtils = mockStatic(LoggerUtils.class);
             MockedStatic<WebSubHubAdapterUtil> mockedAdapterUtil = mockAdapterUtil();
             MockedStatic<IdentityContext> mockedIdentityContext = mockIdentityContext("owner-tenant-a");
             MockedStatic<ServiceURLBuilder> mockedUrlBuilder = mockStatic(ServiceURLBuilder.class)) {
            mockedLoggerUtils.when(LoggerUtils::isDiagnosticLogsEnabled).thenReturn(false);
            when(mockOrgManager.resolveOrganizationId("owner-tenant-b")).thenReturn("owner-org-b");
            mockIssuerUrls(mockedUrlBuilder, "/t/owner-tenant-a", "https://localhost/t/owner-tenant-a",
                    "/t/owner-tenant-a/o/owner-org-b", "https://localhost/t/owner-tenant-a/o/owner-org-b");
            stubClientManager();

            adapterService.publish(buildPayload(), buildEventContext());

            // One publish per owning organization, to its own topic and stamped with its own issuer.
            mockedAdapterUtil.verify(() -> WebSubHubAdapterUtil.constructHubTopic(any(), any(), any(),
                    eq("owner-tenant-a")), times(1));
            mockedAdapterUtil.verify(() -> WebSubHubAdapterUtil.constructHubTopic(any(), any(), any(),
                    eq("owner-tenant-b")), times(1));
            mockedAdapterUtil.verify(() -> WebSubHubAdapterUtil.constructHubTopic(any(), any(), any(),
                    eq(EVENT_TENANT)), never());
            List<String> bodies = capturePublishedBodies(2);
            assertTrue(bodies.get(0).contains("\"iss\":\"https://localhost/t/owner-tenant-a\""));
            assertTrue(bodies.get(1).contains("\"iss\":\"https://localhost/t/owner-tenant-a/o/owner-org-b\""));
        }
    }

    @Test
    public void testPublishSubOrganizationEventContinuesPastAFailedOrganization() throws Exception {

        markEventTenantAsSubOrganization();
        stubSubscribedWebhooks(mockWebhook(null), mockWebhook("failing-owner-tenant"));
        try (MockedStatic<LoggerUtils> mockedLoggerUtils = mockStatic(LoggerUtils.class);
             MockedStatic<WebSubHubAdapterUtil> mockedAdapterUtil = mockAdapterUtil()) {
            mockedLoggerUtils.when(LoggerUtils::isDiagnosticLogsEnabled).thenReturn(false);
            mockedAdapterUtil.when(() -> WebSubHubAdapterUtil.constructHubTopic(any(), any(), any(),
                    eq("failing-owner-tenant"))).thenThrow(new WebSubAdapterServerException("error", "error"));
            stubClientManager();

            expectThrows(EventPublisherException.class,
                    () -> adapterService.publish(buildPayload(), buildEventContext()));

            // The webhook without a recorded owner is served from the event's own topic, as the handler built it.
            List<String> bodies = capturePublishedBodies(1);
            assertTrue(bodies.get(0).contains("\"iss\":\"issuer\""));
        }
    }

    @Test
    public void testCanHandleRootOrganizationEventChecksItsOwnTopic() throws Exception {

        when(mockTopicManagementService.isTopicExists(any(), any(), any(), eq(EVENT_TENANT))).thenReturn(true);

        assertTrue(adapterService.canHandleEvent(buildEventContext()));
        verifyNoInteractions(mockWebhookManagementService);
    }

    @Test
    public void testCanHandleSubOrganizationEventChecksTheTopicsOfOwners() throws Exception {

        markEventTenantAsSubOrganization();
        stubSubscribedWebhooks(mockWebhook("owner-tenant-c"));
        when(mockTopicManagementService.isTopicExists(any(), any(), any(), eq("owner-tenant-c"))).thenReturn(true);

        assertTrue(adapterService.canHandleEvent(buildEventContext()));
        verify(mockTopicManagementService, never()).isTopicExists(any(), any(), any(), eq(EVENT_TENANT));
    }

    @Test
    public void testCanHandleSubOrganizationEventChecksItsOwnTopicWithoutChildOrganizationSubscription()
            throws Exception {

        markEventTenantAsSubOrganization();
        mockedIdentityUtil.when(IdentityUtil::isChildOrganizationSubscriptionEnabled).thenReturn(false);
        when(mockTopicManagementService.isTopicExists(any(), any(), any(), eq(EVENT_TENANT))).thenReturn(true);

        assertTrue(adapterService.canHandleEvent(buildEventContext()));
        verifyNoInteractions(mockWebhookManagementService);
    }

    @Test
    public void testPublishSubOrganizationEventToItsOwnTopicWithoutChildOrganizationSubscription()
            throws Exception {

        markEventTenantAsSubOrganization();
        mockedIdentityUtil.when(IdentityUtil::isChildOrganizationSubscriptionEnabled).thenReturn(false);
        try (MockedStatic<LoggerUtils> mockedLoggerUtils = mockStatic(LoggerUtils.class);
             MockedStatic<WebSubHubAdapterUtil> mockedAdapterUtil = mockAdapterUtil()) {
            mockedLoggerUtils.when(LoggerUtils::isDiagnosticLogsEnabled).thenReturn(false);
            stubClientManager();

            adapterService.publish(buildPayload(), buildEventContext());

            mockedAdapterUtil.verify(() -> WebSubHubAdapterUtil.constructHubTopic(any(), any(), any(),
                    eq(EVENT_TENANT)), times(1));
            verifyNoInteractions(mockWebhookManagementService);
            assertTrue(capturePublishedBodies(1).get(0).contains("\"iss\":\"issuer\""));
        }
    }

    @Test
    public void testCannotHandleSubOrganizationEventWithoutSubscribedWebhooks() throws Exception {

        markEventTenantAsSubOrganization();
        stubSubscribedWebhooks();

        assertFalse(adapterService.canHandleEvent(buildEventContext()));
        verifyNoInteractions(mockTopicManagementService);
    }

    private void markEventTenantAsSubOrganization() {

        mockedOrganizationManagementUtil.when(() -> OrganizationManagementUtil.isOrganization(EVENT_TENANT))
                .thenReturn(true);
    }

    private void stubSubscribedWebhooks(Webhook... webhooks) throws Exception {

        when(mockWebhookManagementService.getSubscriptionAcceptedWebhooksForOrganization(any(), any(), any(),
                eq(EVENT_TENANT))).thenReturn(Arrays.asList(webhooks));
    }

    private static Webhook mockWebhook(String ownerTenantDomain) {

        Webhook webhook = mock(Webhook.class);
        when(webhook.getOwnerTenantDomain()).thenReturn(ownerTenantDomain);
        return webhook;
    }

    private static MockedStatic<WebSubHubAdapterUtil> mockAdapterUtil() {

        MockedStatic<WebSubHubAdapterUtil> mockedAdapterUtil =
                mockStatic(WebSubHubAdapterUtil.class, CALLS_REAL_METHODS);
        mockedAdapterUtil.when(() -> WebSubHubAdapterUtil.constructHubTopic(any(), any(), any(), any()))
                .thenReturn("mock-topic");
        mockedAdapterUtil.when(WebSubHubAdapterUtil::getWebSubBaseURL).thenReturn("http://mock-websub-hub.com");
        mockedAdapterUtil.when(() -> WebSubHubAdapterUtil
                .printPublisherDiagnosticLog(any(), any(), any(), any(), any(), any())).then(invocation -> null);
        return mockedAdapterUtil;
    }

    private void stubClientManager() throws Exception {

        when(mockClientManager.getMaxRetries()).thenReturn(0);
        when(mockClientManager.executeAsync(any())).thenReturn(CompletableFuture.completedFuture(mockHttpResponse));
        when(mockClientManager.createHttpPost(any(), any(), any()))
                .thenReturn(mock(org.apache.http.client.methods.HttpPost.class));
        when(mockClientManager.getAsyncCallbackExecutor()).thenReturn((Executor) Runnable::run);
    }

    private List<String> capturePublishedBodies(int count) throws Exception {

        ArgumentCaptor<String> body = ArgumentCaptor.forClass(String.class);
        verify(mockClientManager, times(count)).createHttpPost(any(), body.capture(), any());
        assertEquals(body.getAllValues().size(), count);
        return body.getAllValues();
    }

    private static MockedStatic<IdentityContext> mockIdentityContext(String rootTenantDomain) {

        RootOrganization rootOrganization = mock(RootOrganization.class);
        when(rootOrganization.getAssociatedTenantDomain()).thenReturn(rootTenantDomain);
        IdentityContext identityContext = mock(IdentityContext.class);
        when(identityContext.getRootOrganization()).thenReturn(rootOrganization);
        MockedStatic<IdentityContext> mockedIdentityContext = mockStatic(IdentityContext.class);
        mockedIdentityContext.when(IdentityContext::getThreadLocalIdentityContext).thenReturn(identityContext);
        return mockedIdentityContext;
    }

    private static void mockIssuerUrls(MockedStatic<ServiceURLBuilder> mockedUrlBuilder, String... pathsAndUrls)
            throws Exception {

        ServiceURLBuilder urlBuilder = mock(ServiceURLBuilder.class);
        mockedUrlBuilder.when(ServiceURLBuilder::create).thenReturn(urlBuilder);
        for (int i = 0; i < pathsAndUrls.length; i += 2) {
            ServiceURLBuilder pathBuilder = mock(ServiceURLBuilder.class);
            ServiceURL serviceURL = mock(ServiceURL.class);
            when(urlBuilder.addPath(pathsAndUrls[i])).thenReturn(pathBuilder);
            when(pathBuilder.build()).thenReturn(serviceURL);
            when(serviceURL.getAbsolutePublicURL()).thenReturn(pathsAndUrls[i + 1]);
        }
    }

    private static EventContext buildEventContext() {

        return EventContext.builder()
                .tenantDomain(EVENT_TENANT)
                .eventProfileName("WSO2")
                .eventUri("test-uri")
                .build();
    }

    private static SecurityEventTokenPayload buildPayload() {

        return SecurityEventTokenPayload.builder()
                .iss("issuer")
                .jti("jti-token")
                .iat(System.currentTimeMillis())
                .aud("audience")
                .events(Collections.singletonMap("event1", new EventPayload() {
                }))
                .build();
    }

    /**
     * Mocks the IdentityTenantUtil.
     */
     private static void mockIdentityTenantUtil() {

        if (mockedStaticIdentityTenantUtil != null && !mockedStaticIdentityTenantUtil.isClosed()) {
            mockedStaticIdentityTenantUtil.close();
        }
        mockedStaticIdentityTenantUtil = mockStatic(IdentityTenantUtil.class);
        when(IdentityTenantUtil.isTenantedSessionsEnabled()).thenReturn(false);
        when(IdentityTenantUtil.getTenantId("test-tenant")).thenReturn(1);
    }
}
