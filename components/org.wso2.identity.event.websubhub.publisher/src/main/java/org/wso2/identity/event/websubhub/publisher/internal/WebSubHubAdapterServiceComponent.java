/*
 * Copyright (c) 2024, WSO2 LLC. (http://www.wso2.com).
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.wso2.carbon.identity.core.util.IdentityCoreInitializedEvent;
import org.wso2.carbon.identity.event.publisher.api.service.EventPublisher;
import org.wso2.carbon.identity.organization.management.service.OrganizationManager;
import org.wso2.carbon.identity.subscription.management.api.service.EventSubscriber;
import org.wso2.carbon.identity.topic.management.api.service.TopicManagementService;
import org.wso2.carbon.identity.topic.management.api.service.TopicManager;
import org.wso2.carbon.identity.webhook.management.api.service.WebhookManagementService;
import org.wso2.carbon.identity.webhook.metadata.api.exception.WebhookMetadataException;
import org.wso2.carbon.identity.webhook.metadata.api.service.EventAdapterMetadataService;
import org.wso2.identity.event.websubhub.publisher.config.WebSubAdapterConfiguration;
import org.wso2.identity.event.websubhub.publisher.service.WebSubEventPublisherImpl;
import org.wso2.identity.event.websubhub.publisher.service.WebSubEventSubscriberImpl;
import org.wso2.identity.event.websubhub.publisher.service.WebSubTopicManagerImpl;

import static org.wso2.identity.event.websubhub.publisher.constant.WebSubHubAdapterConstants.WEB_SUB_HUB_ADAPTER_NAME;

/**
 * WebSubHub Outbound Event Adapter service component.
 */
@Component(
        name = "org.wso2.identity.event.websubhub.publisher.internal.WebSubHubAdapterServiceComponent",
        immediate = true)
public class WebSubHubAdapterServiceComponent {

    private static final Log log = LogFactory.getLog(WebSubHubAdapterServiceComponent.class);
    private static final String ERROR_CODE_ADAPTER_NOT_FOUND = "WEBHOOKMETA-66011";

    @Activate
    protected void activate(ComponentContext context) {

        try {
            WebSubHubAdapterDataHolder.getInstance().setAdapterConfiguration(new WebSubAdapterConfiguration(
                    WebSubHubAdapterDataHolder.getInstance().getEventAdapterMetadataService()
                            .getAdapterByName(WEB_SUB_HUB_ADAPTER_NAME).getProperties()));
            if (WebSubHubAdapterDataHolder.getInstance().getAdapterConfiguration().isAdapterEnabled()) {
                // Register EventPublisher service
                WebSubEventPublisherImpl eventPublisherService = new WebSubEventPublisherImpl();
                context.getBundleContext().registerService(EventPublisher.class.getName(),
                        eventPublisherService, null);

                // Register WebhookSubscriber service
                WebSubEventSubscriberImpl subscriberService = new WebSubEventSubscriberImpl();
                context.getBundleContext().registerService(EventSubscriber.class.getName(),
                        subscriberService, null);

                // Register WebhookTopicManager service
                WebSubTopicManagerImpl topicManagerService = new WebSubTopicManagerImpl();
                context.getBundleContext().registerService(TopicManager.class.getName(),
                        topicManagerService, null);
                WebSubHubAdapterDataHolder.getInstance().setClientManager(new ClientManager());
                log.debug("Successfully activated the WebSubHub adapter service.");
            }
        } catch (Throwable e) {
            if (e instanceof WebhookMetadataException &&
                    ERROR_CODE_ADAPTER_NOT_FOUND.equals(((WebhookMetadataException) e).getErrorCode())) {
                log.warn("websubhub adapter is not enabled. " +
                        "Please enable the websubhub adapter in the configuration file to use the websubhub event " +
                        "publisher.");
            } else {
                log.error("Error while activating the websubhub adapter service: " + e.getMessage(), e);
            }
        }
    }

    @Reference(
            name = "identity.core.init.event.service",
            service = IdentityCoreInitializedEvent.class,
            cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.DYNAMIC,
            unbind = "unsetIdentityCoreInitializedEventService"
    )
    protected void setIdentityCoreInitializedEventService(IdentityCoreInitializedEvent identityCoreInitializedEvent) {
        /* reference IdentityCoreInitializedEvent service to guarantee that this component will wait until identity core
         is started */
    }

    protected void unsetIdentityCoreInitializedEventService(IdentityCoreInitializedEvent identityCoreInitializedEvent) {
        /* reference IdentityCoreInitializedEvent service to guarantee that this component will wait until identity core
         is started */
    }

    @Deactivate
    protected void deactivate(ComponentContext context) {

        log.debug("Successfully de-activated the WebSubHub adapter service.");
    }

    @Reference(name = "identity.organization.management.component",
            service = OrganizationManager.class,
            cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.DYNAMIC,
            unbind = "unsetOrganizationManager")
    protected void setOrganizationManager(OrganizationManager organizationManager) {

        WebSubHubAdapterDataHolder.getInstance().setOrganizationManager(organizationManager);
    }

    protected void unsetOrganizationManager(OrganizationManager organizationManager) {

        WebSubHubAdapterDataHolder.getInstance().setOrganizationManager(null);
    }

    @Reference(
            name = "topic.management.service.component",
            service = TopicManagementService.class,
            cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.DYNAMIC,
            unbind = "unsetTopicManagementService"
    )
    protected void setTopicManagementService(TopicManagementService topicManagementService) {

        WebSubHubAdapterDataHolder.getInstance().setTopicManagementService(topicManagementService);
    }

    protected void unsetTopicManagementService(TopicManagementService topicManagementService) {

        WebSubHubAdapterDataHolder.getInstance().setTopicManagementService(null);
    }

    @Reference(
            name = "identity.webhook.adapter.metadata.component",
            service = EventAdapterMetadataService.class,
            cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.DYNAMIC,
            unbind = "unsetEventAdapterMetadataService"
    )
    protected void setEventAdapterMetadataService(EventAdapterMetadataService eventAdapterMetadataService) {

        WebSubHubAdapterDataHolder.getInstance()
                .setEventAdapterMetadataService(eventAdapterMetadataService);
        log.debug("EventAdapterMetadataService set in WebSubHubAdapterDataHolder bundle.");
    }

    protected void unsetEventAdapterMetadataService(EventAdapterMetadataService eventAdapterMetadataService) {

        WebSubHubAdapterDataHolder.getInstance().setEventAdapterMetadataService(null);
        log.debug("EventAdapterMetadataService unset in WebSubHubAdapterDataHolder bundle.");
    }

    @Reference(
            name = "webhook.management.service.component",
            service = WebhookManagementService.class,
            cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.DYNAMIC,
            unbind = "unsetWebhookManagementService"
    )
    protected void setWebhookManagementService(WebhookManagementService webhookManagementService) {

        WebSubHubAdapterDataHolder.getInstance().setWebhookManagementService(webhookManagementService);
        log.debug("WebhookManagementService set in WebSubHubAdapterDataHolder bundle.");
    }

    protected void unsetWebhookManagementService(WebhookManagementService webhookManagementService) {

        WebSubHubAdapterDataHolder.getInstance().setWebhookManagementService(null);
        log.debug("WebhookManagementService unset in WebSubHubAdapterDataHolder bundle.");
    }
}
