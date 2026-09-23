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

package org.wso2.identity.event.websubhub.publisher.internal;

import org.wso2.carbon.identity.organization.management.service.OrganizationManager;
import org.wso2.carbon.identity.topic.management.api.service.TopicManagementService;
import org.wso2.carbon.identity.webhook.management.api.service.WebhookManagementService;
import org.wso2.carbon.identity.webhook.metadata.api.service.EventAdapterMetadataService;
import org.wso2.identity.event.websubhub.publisher.config.WebSubAdapterConfiguration;

import java.security.KeyStore;

/**
 * WebSubHub Outbound Event Adapter service component's value holder.
 */
public class WebSubHubAdapterDataHolder {

    private static final WebSubHubAdapterDataHolder instance = new WebSubHubAdapterDataHolder();
    private ClientManager clientManager;
    private KeyStore trustStore;
    private WebSubAdapterConfiguration adapterConfiguration;
    private OrganizationManager organizationManager;
    private TopicManagementService topicManagementService;
    private EventAdapterMetadataService eventAdapterMetadataService;
    private WebhookManagementService webhookManagementService;

    private WebSubHubAdapterDataHolder() {

    }

    public static WebSubHubAdapterDataHolder getInstance() {

        return instance;
    }

    public ClientManager getClientManager() {

        return clientManager;
    }

    public void setClientManager(ClientManager clientManager) {

        this.clientManager = clientManager;
    }

    public KeyStore getTrustStore() {

        return trustStore;
    }

    public void setTrustStore(KeyStore trustStore) {

        this.trustStore = trustStore;
    }

    public WebSubAdapterConfiguration getAdapterConfiguration() {

        return adapterConfiguration;
    }

    public void setAdapterConfiguration(WebSubAdapterConfiguration adapterConfiguration) {

        this.adapterConfiguration = adapterConfiguration;
    }

    public OrganizationManager getOrganizationManager() {

        return organizationManager;
    }

    public void setOrganizationManager(OrganizationManager organizationManager) {

        this.organizationManager = organizationManager;
    }

    /**
     * Get the topic management service.
     *
     * @return Topic management service.
     */
    public TopicManagementService getTopicManagementService() {

        return topicManagementService;
    }

    /**
     * Set the topic management service.
     *
     * @param topicManagementService Topic management service.
     */
    public void setTopicManagementService(TopicManagementService topicManagementService) {

        this.topicManagementService = topicManagementService;
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
     * Get the webhook management service.
     *
     * @return WebhookManagementService instance.
     */
    public WebhookManagementService getWebhookManagementService() {

        return webhookManagementService;
    }

    /**
     * Set the webhook management service.
     *
     * @param webhookManagementService WebhookManagementService instance.
     */
    public void setWebhookManagementService(WebhookManagementService webhookManagementService) {

        this.webhookManagementService = webhookManagementService;
    }
}
