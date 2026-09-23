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

package org.wso2.identity.event.http.publisher.internal;

import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.carbon.identity.organization.management.service.OrganizationManager;
import org.wso2.carbon.identity.webhook.management.api.service.WebhookManagementService;
import org.wso2.carbon.identity.webhook.metadata.api.service.EventAdapterMetadataService;
import org.wso2.identity.event.http.publisher.internal.component.ClientManager;
import org.wso2.identity.event.http.publisher.internal.component.HTTPAdapterDataHolder;
import org.wso2.identity.event.http.publisher.internal.config.HTTPAdapterConfiguration;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class HTTPAdapterDataHolderTest {

    private HTTPAdapterDataHolder holder;

    @BeforeMethod
    public void setUp() {

        holder = HTTPAdapterDataHolder.getInstance();
        holder.setClientManager(null);
        holder.setAdapterConfiguration(null);
        holder.setWebhookManagementService(null);
        holder.setEventAdapterMetadataService(null);
        holder.setOrganizationManager(null);
    }

    @Test
    public void testClientManager() {

        assertNull(holder.getClientManager());
        ClientManager clientManager = Mockito.mock(ClientManager.class);
        holder.setClientManager(clientManager);
        assertEquals(holder.getClientManager(), clientManager);
    }

    @Test
    public void testAdapterConfiguration() {

        assertNull(holder.getAdapterConfiguration());
        HTTPAdapterConfiguration config = Mockito.mock(HTTPAdapterConfiguration.class);
        holder.setAdapterConfiguration(config);
        assertEquals(holder.getAdapterConfiguration(), config);
    }

    @Test
    public void testWebhookManagementService() {

        assertNull(holder.getWebhookManagementService());
        WebhookManagementService service = Mockito.mock(WebhookManagementService.class);
        holder.setWebhookManagementService(service);
        assertEquals(holder.getWebhookManagementService(), service);
    }

    @Test
    public void testEventAdapterMetadataService() {

        assertNull(holder.getEventAdapterMetadataService());
        EventAdapterMetadataService service = Mockito.mock(EventAdapterMetadataService.class);
        holder.setEventAdapterMetadataService(service);
        assertEquals(holder.getEventAdapterMetadataService(), service);
    }

    @Test
    public void testOrganizationManager() {

        assertNull(holder.getOrganizationManager());
        OrganizationManager organizationManager = Mockito.mock(OrganizationManager.class);
        holder.setOrganizationManager(organizationManager);
        assertEquals(holder.getOrganizationManager(), organizationManager);
    }
}
