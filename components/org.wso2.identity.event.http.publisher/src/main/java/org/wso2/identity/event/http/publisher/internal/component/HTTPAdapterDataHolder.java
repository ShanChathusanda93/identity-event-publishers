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

package org.wso2.identity.event.http.publisher.internal.component;

import org.wso2.carbon.identity.organization.management.service.OrganizationManager;
import org.wso2.carbon.identity.webhook.management.api.service.WebhookManagementService;
import org.wso2.carbon.identity.webhook.metadata.api.service.EventAdapterMetadataService;
import org.wso2.identity.event.http.publisher.internal.config.HTTPAdapterConfiguration;

/**
 * HTTP Outbound Event Adapter service component's value holder.
 */
public class HTTPAdapterDataHolder {

    private static final HTTPAdapterDataHolder instance = new HTTPAdapterDataHolder();
    private ClientManager clientManager;
    private HTTPAdapterConfiguration adapterConfiguration;
    private EventAdapterMetadataService eventAdapterMetadataService;
    private WebhookManagementService webhookManagementService;
    private OrganizationManager organizationManager;

    private HTTPAdapterDataHolder() {

    }

    public static HTTPAdapterDataHolder getInstance() {

        return instance;
    }

    public ClientManager getClientManager() {

        return clientManager;
    }

    public void setClientManager(ClientManager clientManager) {

        this.clientManager = clientManager;
    }

    public HTTPAdapterConfiguration getAdapterConfiguration() {

        return adapterConfiguration;
    }

    public void setAdapterConfiguration(HTTPAdapterConfiguration adapterConfiguration) {

        this.adapterConfiguration = adapterConfiguration;
    }

    /**
     * Get the webhook management service.
     *
     * @return Webhook management service.
     */
    public WebhookManagementService getWebhookManagementService() {

        return webhookManagementService;
    }

    /**
     * Set the webhook management service.
     *
     * @param webhookManagementService Webhook management service.
     */
    public void setWebhookManagementService(WebhookManagementService webhookManagementService) {

        this.webhookManagementService = webhookManagementService;
    }

    /**
     * Get the event adapter metadata service.
     *
     * @return EventAdapterMetadataService instance.
     */
    public EventAdapterMetadataService getEventAdapterMetadataService() {

        return eventAdapterMetadataService;
    }

    /**
     * Set the event adapter metadata service.
     *
     * @param eventAdapterMetadataService EventAdapterMetadataService instance.
     */
    public void setEventAdapterMetadataService(EventAdapterMetadataService eventAdapterMetadataService) {

        this.eventAdapterMetadataService = eventAdapterMetadataService;
    }

    /**
     * Get the organization manager.
     *
     * @return OrganizationManager instance.
     */
    public OrganizationManager getOrganizationManager() {

        return organizationManager;
    }

    /**
     * Set the organization manager.
     *
     * @param organizationManager OrganizationManager instance.
     */
    public void setOrganizationManager(OrganizationManager organizationManager) {

        this.organizationManager = organizationManager;
    }

}
