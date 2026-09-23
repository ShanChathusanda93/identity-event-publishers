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

package org.wso2.identity.event.http.publisher.service;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.carbon.identity.central.log.mgt.utils.LoggerUtils;
import org.wso2.carbon.identity.core.ServiceURL;
import org.wso2.carbon.identity.core.ServiceURLBuilder;
import org.wso2.carbon.identity.core.URLBuilderException;
import org.wso2.carbon.identity.core.context.IdentityContext;
import org.wso2.carbon.identity.core.context.model.RootOrganization;
import org.wso2.carbon.identity.core.util.IdentityTenantUtil;
import org.wso2.carbon.identity.core.util.IdentityUtil;
import org.wso2.carbon.identity.event.publisher.api.exception.EventPublisherServerException;
import org.wso2.carbon.identity.event.publisher.api.model.EventContext;
import org.wso2.carbon.identity.event.publisher.api.model.EventPayload;
import org.wso2.carbon.identity.event.publisher.api.model.SecurityEventTokenPayload;
import org.wso2.carbon.identity.organization.management.service.OrganizationManager;
import org.wso2.carbon.identity.organization.management.service.exception.OrganizationManagementException;
import org.wso2.carbon.identity.organization.management.service.util.OrganizationManagementUtil;
import org.wso2.carbon.identity.webhook.management.api.exception.WebhookMgtException;
import org.wso2.carbon.identity.webhook.management.api.model.Webhook;
import org.wso2.identity.event.http.publisher.internal.component.ClientManager;
import org.wso2.identity.event.http.publisher.internal.component.HTTPAdapterDataHolder;
import org.wso2.identity.event.http.publisher.internal.constant.HTTPAdapterConstants;
import org.wso2.identity.event.http.publisher.internal.service.impl.HTTPEventPublisherImpl;
import org.wso2.identity.event.http.publisher.internal.util.HTTPAdapterUtil;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class HTTPEventPublisherImplTest {

    private static final String CARBON_HOME = "carbon.home";

    private HTTPEventPublisherImpl adapterService;
    private AutoCloseable mocks;

    @Mock
    private ClientManager mockClientManager;

    @Mock
    private HttpResponse mockHttpResponse;

    @Mock
    private OrganizationManager mockOrganizationManager;

    private MockedStatic<HTTPAdapterDataHolder> mockedStaticDataHolder;
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
        adapterService = spy(new HTTPEventPublisherImpl());
        mockIdentityTenantUtil();

        mockedStaticDataHolder = mockStatic(HTTPAdapterDataHolder.class);
        HTTPAdapterDataHolder mockDataHolder = mock(HTTPAdapterDataHolder.class);
        mockedStaticDataHolder.when(HTTPAdapterDataHolder::getInstance).thenReturn(mockDataHolder);

        when(mockDataHolder.getClientManager()).thenReturn(mockClientManager);
        when(mockDataHolder.getWebhookManagementService()).thenReturn(
                mock(org.wso2.carbon.identity.webhook.management.api.service.WebhookManagementService.class));
        when(mockDataHolder.getOrganizationManager()).thenReturn(mockOrganizationManager);

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
    }

    @Test
    public void testPublishSuccess() throws Exception {

        try (MockedStatic<LoggerUtils> mockedLoggerUtils = mockStatic(LoggerUtils.class)) {
            mockedLoggerUtils.when(LoggerUtils::isDiagnosticLogsEnabled).thenReturn(false);

            HTTPAdapterDataHolder mockDataHolder = HTTPAdapterDataHolder.getInstance();
            Webhook webhook1 = mock(Webhook.class);
            when(webhook1.getEndpoint()).thenReturn("http://mock-endpoint-1.com");
            when(webhook1.getDecryptedSecret()).thenReturn("secret1");
            Webhook webhook2 = mock(Webhook.class);
            when(webhook2.getEndpoint()).thenReturn("http://mock-endpoint-2.com");
            when(webhook2.getDecryptedSecret()).thenReturn("secret2");
            List<Webhook> webhooks = Arrays.asList(webhook1, webhook2);
            when(mockDataHolder.getWebhookManagementService().getActiveWebhooks(any(), any(), any(), any()))
                    .thenReturn(webhooks);

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

            CompletableFuture<HttpResponse> future = CompletableFuture.completedFuture(mockHttpResponse);
            when(mockClientManager.executeAsync(any())).thenReturn(future);
            when(mockClientManager.createHttpPost(any(), any(), any())).thenReturn(mock(HttpPost.class));
            when(mockClientManager.getAsyncCallbackExecutor()).thenReturn((Executor) Runnable::run);

            adapterService.publish(payload, eventContext);

            verify(mockClientManager, times(2)).executeAsync(any());
            verify(mockClientManager, times(2)).createHttpPost(any(), any(), any());
        }
    }

    @Test
    public void testGetAssociatedAdapter() {

        Assert.assertEquals(adapterService.getAssociatedAdapter(), HTTPAdapterConstants.HTTP_ADAPTER_NAME);
    }

    @Test
    public void testPublishWithNoActiveWebhooks() throws Exception {

        HTTPAdapterDataHolder mockDataHolder = HTTPAdapterDataHolder.getInstance();
        when(mockDataHolder.getWebhookManagementService().getActiveWebhooks(any(), any(), any(), any()))
                .thenReturn(Collections.emptyList());

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

        adapterService.publish(payload, eventContext);
        verify(mockClientManager, never()).executeAsync(any());
    }

    @Test(expectedExceptions = EventPublisherServerException.class)
    public void testCanHandleEventThrowsWebhookMgtException() throws Exception {

        HTTPAdapterDataHolder mockDataHolder = HTTPAdapterDataHolder.getInstance();
        when(mockDataHolder.getWebhookManagementService().getActiveWebhooks(any(), any(), any(), any()))
                .thenThrow(new WebhookMgtException("error"));
        EventContext eventContext = EventContext.builder()
                .tenantDomain("test-tenant")
                .eventProfileName("WSO2")
                .eventUri("test-uri")
                .build();
        adapterService.canHandleEvent(eventContext);
    }

    @Test
    public void testPublishJsonSerializationFailure() throws Exception {

        try (MockedStatic<LoggerUtils> mockedLoggerUtils = mockStatic(LoggerUtils.class);
             MockedStatic<HTTPAdapterUtil> mockedAdapterUtil = mockStatic(HTTPAdapterUtil.class)) {
            mockedLoggerUtils.when(LoggerUtils::isDiagnosticLogsEnabled).thenReturn(false);

            HTTPAdapterDataHolder mockDataHolder = HTTPAdapterDataHolder.getInstance();
            Webhook webhook = mock(Webhook.class);
            when(webhook.getEndpoint()).thenReturn("http://mock-endpoint.com");
            when(webhook.getDecryptedSecret()).thenReturn("secret");
            when(mockDataHolder.getWebhookManagementService().getActiveWebhooks(any(), any(), any(), any()))
                    .thenReturn(Collections.singletonList(webhook));

            // Use a payload that Jackson cannot serialize (self-referencing)
            class SelfRef extends EventPayload {

                public SelfRef self;
            }
            SelfRef selfRef = new SelfRef();
            selfRef.self = selfRef;

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
                    .events(Collections.singletonMap("event1", selfRef))
                    .build();

            adapterService.publish(payload, eventContext);
            // Should not throw, just log and return
        }
    }

    @Test
    public void testPublishRootOrganizationEventKeepsHandlerIssuer() throws Exception {

        stubActiveWebhooks(mockWebhook("root-owner-tenant"));
        try (MockedStatic<LoggerUtils> mockedLoggerUtils = mockStatic(LoggerUtils.class);
             MockedStatic<ServiceURLBuilder> mockedUrlBuilder = mockStatic(ServiceURLBuilder.class)) {
            mockedLoggerUtils.when(LoggerUtils::isDiagnosticLogsEnabled).thenReturn(false);
            stubClientManager();

            adapterService.publish(buildPayload(), buildEventContext());

            mockedUrlBuilder.verifyNoInteractions();
            assertTrue(capturePublishedBody().contains("\"iss\":\"issuer\""));
        }
    }

    @Test
    public void testPublishSubOrganizationEventKeepsHandlerIssuerWithoutChildOrganizationSubscription()
            throws Exception {

        markEventTenantAsSubOrganization();
        mockedIdentityUtil.when(IdentityUtil::isChildOrganizationSubscriptionEnabled).thenReturn(false);
        stubActiveWebhooks(mockWebhook("root-owner-tenant"));
        try (MockedStatic<LoggerUtils> mockedLoggerUtils = mockStatic(LoggerUtils.class);
             MockedStatic<ServiceURLBuilder> mockedUrlBuilder = mockStatic(ServiceURLBuilder.class)) {
            mockedLoggerUtils.when(LoggerUtils::isDiagnosticLogsEnabled).thenReturn(false);
            stubClientManager();

            adapterService.publish(buildPayload(), buildEventContext());

            mockedUrlBuilder.verifyNoInteractions();
            assertTrue(capturePublishedBody().contains("\"iss\":\"issuer\""));
        }
    }

    @Test
    public void testPublishSubOrganizationEventStampsIssuerOfOwnerFromIdentityContext() throws Exception {

        markEventTenantAsSubOrganization();
        stubActiveWebhooks(mockWebhook("context-root-tenant"));
        try (MockedStatic<LoggerUtils> mockedLoggerUtils = mockStatic(LoggerUtils.class);
             MockedStatic<IdentityContext> mockedIdentityContext = mockIdentityContext("context-root-tenant");
             MockedStatic<ServiceURLBuilder> mockedUrlBuilder = mockStatic(ServiceURLBuilder.class)) {
            mockedLoggerUtils.when(LoggerUtils::isDiagnosticLogsEnabled).thenReturn(false);
            ServiceURLBuilder urlBuilder = mockUrlBuilder(mockedUrlBuilder, "https://localhost/t/context-root-tenant");
            stubClientManager();

            adapterService.publish(buildPayload(), buildEventContext());

            verify(urlBuilder).addPath("/t/context-root-tenant");
            assertTrue(capturePublishedBody().contains("\"iss\":\"https://localhost/t/context-root-tenant\""));
        }
    }

    @Test
    public void testPublishSubOrganizationEventResolvesRootWithoutIdentityContext() throws Exception {

        markEventTenantAsSubOrganization();
        stubActiveWebhooks(mockWebhook("lookup-owner-tenant"));
        when(mockOrganizationManager.resolveOrganizationId("lookup-owner-tenant")).thenReturn("lookup-owner-org");
        when(mockOrganizationManager.getPrimaryOrganizationId("lookup-owner-org")).thenReturn("lookup-root-org");
        when(mockOrganizationManager.resolveTenantDomain("lookup-root-org")).thenReturn("lookup-root-tenant");
        try (MockedStatic<LoggerUtils> mockedLoggerUtils = mockStatic(LoggerUtils.class);
             MockedStatic<IdentityContext> mockedIdentityContext = mockIdentityContext(null);
             MockedStatic<ServiceURLBuilder> mockedUrlBuilder = mockStatic(ServiceURLBuilder.class)) {
            mockedLoggerUtils.when(LoggerUtils::isDiagnosticLogsEnabled).thenReturn(false);
            ServiceURLBuilder urlBuilder = mockUrlBuilder(mockedUrlBuilder,
                    "https://localhost/t/lookup-root-tenant/o/lookup-owner-org");
            stubClientManager();

            adapterService.publish(buildPayload(), buildEventContext());

            verify(urlBuilder).addPath("/t/lookup-root-tenant/o/lookup-owner-org");
            assertTrue(capturePublishedBody()
                    .contains("\"iss\":\"https://localhost/t/lookup-root-tenant/o/lookup-owner-org\""));
        }
    }

    @Test
    public void testPublishSubOrganizationEventKeepsHandlerIssuerWhenIssuerIsUnresolvable() throws Exception {

        markEventTenantAsSubOrganization();
        stubActiveWebhooks(mockWebhook("unresolvable-owner-tenant"));
        try (MockedStatic<LoggerUtils> mockedLoggerUtils = mockStatic(LoggerUtils.class);
             MockedStatic<IdentityContext> mockedIdentityContext =
                     mockIdentityContext("unresolvable-owner-tenant");
             MockedStatic<ServiceURLBuilder> mockedUrlBuilder = mockStatic(ServiceURLBuilder.class)) {
            mockedLoggerUtils.when(LoggerUtils::isDiagnosticLogsEnabled).thenReturn(false);
            ServiceURLBuilder urlBuilder = mock(ServiceURLBuilder.class);
            mockedUrlBuilder.when(ServiceURLBuilder::create).thenReturn(urlBuilder);
            when(urlBuilder.addPath(any(String[].class))).thenReturn(urlBuilder);
            when(urlBuilder.build()).thenThrow(new URLBuilderException("error"));
            stubClientManager();

            adapterService.publish(buildPayload(), buildEventContext());

            verify(mockClientManager, times(1)).executeAsync(any());
            assertTrue(capturePublishedBody().contains("\"iss\":\"issuer\""));
        }
    }

    @Test
    public void testPublishSubOrganizationEventKeepsHandlerIssuerWithoutRecordedOwner() throws Exception {

        markEventTenantAsSubOrganization();
        stubActiveWebhooks(mockWebhook(null));
        try (MockedStatic<LoggerUtils> mockedLoggerUtils = mockStatic(LoggerUtils.class);
             MockedStatic<ServiceURLBuilder> mockedUrlBuilder = mockStatic(ServiceURLBuilder.class)) {
            mockedLoggerUtils.when(LoggerUtils::isDiagnosticLogsEnabled).thenReturn(false);
            stubClientManager();

            adapterService.publish(buildPayload(), buildEventContext());

            mockedUrlBuilder.verifyNoInteractions();
            assertTrue(capturePublishedBody().contains("\"iss\":\"issuer\""));
        }
    }

    @Test
    public void testPublishSubOrganizationEventResolvesTheIssuerForEachEvent() throws Exception {

        markEventTenantAsSubOrganization();
        stubActiveWebhooks(mockWebhook("cached-root-tenant"));
        try (MockedStatic<LoggerUtils> mockedLoggerUtils = mockStatic(LoggerUtils.class);
             MockedStatic<IdentityContext> mockedIdentityContext = mockIdentityContext("cached-root-tenant");
             MockedStatic<ServiceURLBuilder> mockedUrlBuilder = mockStatic(ServiceURLBuilder.class)) {
            mockedLoggerUtils.when(LoggerUtils::isDiagnosticLogsEnabled).thenReturn(false);
            mockUrlBuilder(mockedUrlBuilder, "https://localhost/t/cached-root-tenant");
            stubClientManager();

            adapterService.publish(buildPayload(), buildEventContext());
            adapterService.publish(buildPayload(), buildEventContext());

            mockedUrlBuilder.verify(ServiceURLBuilder::create, times(2));
            verify(mockClientManager, times(2)).executeAsync(any());
        }
    }

    @Test
    public void testPublishSubOrganizationEventKeepsHandlerIssuerWhenRootIsUnresolvable() throws Exception {

        markEventTenantAsSubOrganization();
        stubActiveWebhooks(mockWebhook("rootless-owner-tenant"));
        when(mockOrganizationManager.resolveOrganizationId("rootless-owner-tenant")).thenReturn("rootless-org");
        try (MockedStatic<LoggerUtils> mockedLoggerUtils = mockStatic(LoggerUtils.class);
             MockedStatic<IdentityContext> mockedIdentityContext = mockIdentityContext(null);
             MockedStatic<ServiceURLBuilder> mockedUrlBuilder = mockStatic(ServiceURLBuilder.class)) {
            mockedLoggerUtils.when(LoggerUtils::isDiagnosticLogsEnabled).thenReturn(false);
            stubClientManager();

            adapterService.publish(buildPayload(), buildEventContext());

            mockedUrlBuilder.verifyNoInteractions();
            assertTrue(capturePublishedBody().contains("\"iss\":\"issuer\""));
        }
    }

    @Test
    public void testPublishTreatsAnUncheckableTenantAsSubOrganization() throws Exception {

        mockedOrganizationManagementUtil.when(() -> OrganizationManagementUtil.isOrganization("test-tenant"))
                .thenThrow(new OrganizationManagementException("error"));
        stubActiveWebhooks(mockWebhook(null));
        try (MockedStatic<LoggerUtils> mockedLoggerUtils = mockStatic(LoggerUtils.class)) {
            mockedLoggerUtils.when(LoggerUtils::isDiagnosticLogsEnabled).thenReturn(false);
            stubClientManager();

            adapterService.publish(buildPayload(), buildEventContext());

            assertTrue(capturePublishedBody().contains("\"iss\":\"issuer\""));
        }
    }

    @Test
    public void testCanHandleEventWhenWebhooksAreActive() throws Exception {

        stubActiveWebhooks(mockWebhook(null));
        assertTrue(adapterService.canHandleEvent(buildEventContext()));
    }

    @Test
    public void testCannotHandleEventWithoutActiveWebhooks() throws Exception {

        stubActiveWebhooks();
        assertFalse(adapterService.canHandleEvent(buildEventContext()));
    }

    private void markEventTenantAsSubOrganization() {

        mockedOrganizationManagementUtil.when(() -> OrganizationManagementUtil.isOrganization("test-tenant"))
                .thenReturn(true);
    }

    private static Webhook mockWebhook(String ownerTenantDomain) throws Exception {

        Webhook webhook = mock(Webhook.class);
        when(webhook.getEndpoint()).thenReturn("http://mock-endpoint.com");
        when(webhook.getDecryptedSecret()).thenReturn("secret");
        when(webhook.getOwnerTenantDomain()).thenReturn(ownerTenantDomain);
        return webhook;
    }

    private static void stubActiveWebhooks(Webhook... webhooks) throws Exception {

        when(HTTPAdapterDataHolder.getInstance().getWebhookManagementService()
                .getActiveWebhooks(any(), any(), any(), any())).thenReturn(Arrays.asList(webhooks));
    }

    private void stubClientManager() throws Exception {

        when(mockClientManager.executeAsync(any())).thenReturn(CompletableFuture.completedFuture(mockHttpResponse));
        when(mockClientManager.createHttpPost(any(), any(), any())).thenReturn(mock(HttpPost.class));
        when(mockClientManager.getAsyncCallbackExecutor()).thenReturn((Executor) Runnable::run);
    }

    private String capturePublishedBody() throws Exception {

        ArgumentCaptor<String> body = ArgumentCaptor.forClass(String.class);
        verify(mockClientManager).createHttpPost(eq("http://mock-endpoint.com"), body.capture(), eq("secret"));
        return body.getValue();
    }

    private static MockedStatic<IdentityContext> mockIdentityContext(String rootTenantDomain) {

        IdentityContext identityContext = mock(IdentityContext.class);
        if (rootTenantDomain != null) {
            RootOrganization rootOrganization = mock(RootOrganization.class);
            when(rootOrganization.getAssociatedTenantDomain()).thenReturn(rootTenantDomain);
            when(identityContext.getRootOrganization()).thenReturn(rootOrganization);
        }
        MockedStatic<IdentityContext> mockedIdentityContext = mockStatic(IdentityContext.class);
        mockedIdentityContext.when(IdentityContext::getThreadLocalIdentityContext).thenReturn(identityContext);
        return mockedIdentityContext;
    }

    private static ServiceURLBuilder mockUrlBuilder(MockedStatic<ServiceURLBuilder> mockedUrlBuilder, String url)
            throws Exception {

        ServiceURLBuilder urlBuilder = mock(ServiceURLBuilder.class);
        ServiceURL serviceURL = mock(ServiceURL.class);
        mockedUrlBuilder.when(ServiceURLBuilder::create).thenReturn(urlBuilder);
        when(urlBuilder.addPath(any(String[].class))).thenReturn(urlBuilder);
        when(urlBuilder.build()).thenReturn(serviceURL);
        when(serviceURL.getAbsolutePublicURL()).thenReturn(url);
        return urlBuilder;
    }

    private static EventContext buildEventContext() {

        return EventContext.builder()
                .tenantDomain("test-tenant")
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

    private static void mockIdentityTenantUtil() {

        if (mockedStaticIdentityTenantUtil != null && !mockedStaticIdentityTenantUtil.isClosed()) {
            mockedStaticIdentityTenantUtil.close();
        }
        mockedStaticIdentityTenantUtil = mockStatic(IdentityTenantUtil.class);
        when(IdentityTenantUtil.isTenantedSessionsEnabled()).thenReturn(false);
        when(IdentityTenantUtil.getTenantId("test-tenant")).thenReturn(1);
    }
}
