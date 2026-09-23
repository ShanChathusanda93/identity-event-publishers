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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.wso2.carbon.identity.event.publisher.api.service.EventPublisher;
import org.wso2.carbon.identity.organization.management.service.OrganizationManager;
import org.wso2.carbon.identity.webhook.management.api.service.WebhookManagementService;
import org.wso2.carbon.identity.webhook.metadata.api.exception.WebhookMetadataException;
import org.wso2.carbon.identity.webhook.metadata.api.service.EventAdapterMetadataService;
import org.wso2.identity.event.http.publisher.internal.config.HTTPAdapterConfiguration;
import org.wso2.identity.event.http.publisher.internal.service.impl.HTTPEventPublisherImpl;

import static org.wso2.identity.event.http.publisher.internal.constant.HTTPAdapterConstants.HTTP_ADAPTER_NAME;

/**
 * HTTP Outbound Event Adapter service component.
 */
@Component(
        name = "org.wso2.identity.event.http.publisher.internal.component.HTTPAdapterServiceComponent",
        immediate = true)
public class HTTPAdapterServiceComponent {

    private static final Log log = LogFactory.getLog(HTTPAdapterServiceComponent.class);
    private static final String ERROR_CODE_ADAPTER_NOT_FOUND = "WEBHOOKMETA-66011";

    @Activate
    protected void activate(ComponentContext context) {

        try {
            HTTPAdapterDataHolder.getInstance().setAdapterConfiguration(new HTTPAdapterConfiguration(
                    HTTPAdapterDataHolder.getInstance().getEventAdapterMetadataService()
                            .getAdapterByName(HTTP_ADAPTER_NAME).getProperties()));
            if (HTTPAdapterDataHolder.getInstance().getAdapterConfiguration().isAdapterEnabled()) {
                // Register EventPublisher service
                HTTPEventPublisherImpl eventPublisherService = new HTTPEventPublisherImpl();
                context.getBundleContext().registerService(EventPublisher.class.getName(),
                        eventPublisherService, null);
                HTTPAdapterDataHolder.getInstance().setClientManager(new ClientManager());
                log.debug("Successfully activated the HTTP adapter service.");
            }
        } catch (Throwable e) {
            if (e instanceof WebhookMetadataException &&
                    ERROR_CODE_ADAPTER_NOT_FOUND.equals(((WebhookMetadataException) e).getErrorCode())) {
                log.warn("HTTP adapter is not enabled. " +
                        "Please enable the HTTP adapter in the configuration file to use the HTTP event publisher.");
            } else {
                log.error("Error while activating the HTTP adapter service: " + e.getMessage(), e);
            }
        }
    }

    @Deactivate
    protected void deactivate(ComponentContext context) {

        log.debug("Successfully de-activated the HTTP adapter service.");
    }

    @Reference(
            name = "webhook.management.service.component",
            service = WebhookManagementService.class,
            cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.DYNAMIC,
            unbind = "unsetWebhookManagementService"
    )
    protected void setWebhookManagementService(WebhookManagementService webhookManagementService) {

        HTTPAdapterDataHolder.getInstance().setWebhookManagementService(webhookManagementService);
    }

    protected void unsetWebhookManagementService(WebhookManagementService webhookManagementService) {

        HTTPAdapterDataHolder.getInstance().setWebhookManagementService(null);
    }

    @Reference(
            name = "identity.webhook.adapter.metadata.component",
            service = EventAdapterMetadataService.class,
            cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.DYNAMIC,
            unbind = "unsetEventAdapterMetadataService"
    )
    protected void setEventAdapterMetadataService(EventAdapterMetadataService eventAdapterMetadataService) {

        HTTPAdapterDataHolder.getInstance()
                .setEventAdapterMetadataService(eventAdapterMetadataService);
        log.debug("EventAdapterMetadataService set in HTTPAdapterDataHolder bundle.");
    }

    protected void unsetEventAdapterMetadataService(EventAdapterMetadataService eventAdapterMetadataService) {

        HTTPAdapterDataHolder.getInstance().setEventAdapterMetadataService(null);
        log.debug("EventAdapterMetadataService unset in HTTPAdapterDataHolder bundle.");
    }

    @Reference(
            name = "organization.management.service",
            service = OrganizationManager.class,
            cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.DYNAMIC,
            unbind = "unsetOrganizationManager"
    )
    protected void setOrganizationManager(OrganizationManager organizationManager) {

        HTTPAdapterDataHolder.getInstance().setOrganizationManager(organizationManager);
        log.debug("OrganizationManager set in HTTPAdapterDataHolder bundle.");
    }

    protected void unsetOrganizationManager(OrganizationManager organizationManager) {

        HTTPAdapterDataHolder.getInstance().setOrganizationManager(null);
        log.debug("OrganizationManager unset in HTTPAdapterDataHolder bundle.");
    }

}
