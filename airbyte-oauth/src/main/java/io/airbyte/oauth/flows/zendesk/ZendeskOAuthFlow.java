/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows.zendesk;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.oauth.BaseOAuthFlow;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZendeskOAuthFlow extends BaseOAuthFlow {

  private static final Logger LOGGER = LoggerFactory.getLogger(ZendeskOAuthFlow.class);

  public ZendeskOAuthFlow(ConfigRepository configRepository) {
    super(configRepository, TOKEN_REQUEST_CONTENT_TYPE.JSON);
  }

  /**
   * Depending on the OAuth flow implementation, the URL to grant user's consent may differ,
   * especially in the query parameters to be provided. This function should generate such consent URL
   * accordingly.
   *
   * @param definitionId SourceDefinitionId
   * @param clientId The ClientId configured for this OAuthClient
   * @param redirectUrl The RedirectURL configured for this OAuthClient
   */
  @Override
  protected String formatConsentUrl(UUID definitionId, String clientId, String redirectUrl) throws IOException {
    final JsonNode oAuthParamConfig;
    try {
      oAuthParamConfig = getSourceOAuthParamConfig(this.getWorkspaceId(), definitionId);
      return formatConsentUrl(getClientIdUnsafe(oAuthParamConfig), redirectUrl,
          MessageFormat.format("{0}.zendesk.com", getSubdomain(oAuthParamConfig)),
          "oauth/authorizations/new", "read", "code");
    } catch (ConfigNotFoundException e) {
      // Let's wrap it into a unchecked exception to avoid further refactorings without a clear cost
      // benefit
      throw new IllegalArgumentException("Undefined parameter 'subdomain' necessary for the Zendesk OAuth Flow.", e);
    }
  }

  /**
   * Returns the URL where to retrieve the access token from.
   */
  @Override
  protected String getAccessTokenUrl(JsonNode oAuthParamConfig) {
    // see comment on completeSourceOAuth
    return MessageFormat.format("https://{0}.zendesk.com/oauth/tokens", getSubdomain(oAuthParamConfig));
  }

  /**
   * Query parameters to provide the access token url with.
   *
   * @param clientId The configured clientId for this OAuth client
   * @param clientSecret The clientSecret configured for this OAuthClient
   * @param authCode The AuthCode obtained from the consent flow
   * @param redirectUrl the configured redirectUrl
   */
  @Override
  protected Map<String, String> getAccessTokenQueryParameters(String clientId, String clientSecret, String authCode, String redirectUrl) {
    return ImmutableMap.<String, String>builder()
        // required
        .put("client_id", clientId)
        .put("redirect_uri", redirectUrl)
        .put("client_secret", clientSecret)
        .put("grant_type", "authorization_code")
        .put("code", authCode)
        .put("scope", "read")
        .build();
  }

}
