package pl.allegro.tech.hermes.consumers.consumer.sender.http;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.hermes.api.EndpointAddress;
import pl.allegro.tech.hermes.api.EndpointAddressResolverMetadata;
import pl.allegro.tech.hermes.api.Subscription;
import pl.allegro.tech.hermes.api.SubscriptionMode;
import pl.allegro.tech.hermes.common.config.ConfigFactory;
import pl.allegro.tech.hermes.common.config.Configs;
import pl.allegro.tech.hermes.consumers.consumer.sender.MessageSender;
import pl.allegro.tech.hermes.consumers.consumer.sender.ProtocolMessageSenderProvider;
import pl.allegro.tech.hermes.consumers.consumer.sender.http.auth.HttpAuthorizationProviderFactory;
import pl.allegro.tech.hermes.consumers.consumer.sender.resolver.EndpointAddressResolver;
import pl.allegro.tech.hermes.consumers.consumer.sender.resolver.ResolvableEndpointAddress;
import pl.allegro.tech.hermes.consumers.consumer.trace.MetadataAppender;

import javax.inject.Inject;
import java.util.Optional;

import static java.util.Optional.empty;

public class JettyHttpMessageSenderProvider implements ProtocolMessageSenderProvider {
    private static final Logger logger = LoggerFactory.getLogger(JettyHttpMessageSenderProvider.class);

    private final HttpClient httpClient;
    private final Optional<HttpClient> http2Client;
    private final EndpointAddressResolver endpointAddressResolver;
    private final MetadataAppender<Request> metadataAppender;
    private final HttpAuthorizationProviderFactory authorizationProviderFactory;

    @Inject
    public JettyHttpMessageSenderProvider(
            ConfigFactory config,
            HttpClientFactory httpClientFactory,
            EndpointAddressResolver endpointAddressResolver,
            MetadataAppender<Request> metadataAppender,
            HttpAuthorizationProviderFactory authorizationProviderFactory) {
        this.httpClient = httpClientFactory.provide();
        this.http2Client = config.getBooleanProperty(Configs.CONSUMER_HTTP2_ENABLED) ?
                Optional.of(httpClientFactory.createClientForHttp2()) : empty();
        this.endpointAddressResolver = endpointAddressResolver;
        this.metadataAppender = metadataAppender;
        this.authorizationProviderFactory = authorizationProviderFactory;
    }

    @Override
    public MessageSender create(Subscription subscription) {
        EndpointAddress endpoint = subscription.getEndpoint();
        EndpointAddressResolverMetadata endpointAddressResolverMetadata = subscription.getEndpointAddressResolverMetadata();
        ResolvableEndpointAddress resolvableEndpoint = new ResolvableEndpointAddress(endpoint,
                endpointAddressResolver, endpointAddressResolverMetadata);
        HttpRequestFactory requestFactory = httpRequestFactory(subscription);

        if (subscription.getMode() == SubscriptionMode.BROADCAST) {
            return new JettyBroadCastMessageSender(requestFactory, resolvableEndpoint);
        } else {
            return new JettyMessageSender(requestFactory, resolvableEndpoint);
        }
    }

    private HttpRequestFactory httpRequestFactory(Subscription subscription) {
        int requestTimeout = subscription.getSerialSubscriptionPolicy().getRequestTimeout();
        HttpClient client = subscription.isHttp2Enabled() ? http2Client.orElseGet(() -> {
            logger.info("Http2 delivery is not enabled on this server. Falling back to http1.");
            return httpClient;
        }) : httpClient;
        return new HttpRequestFactory(http2Client.get(), requestTimeout, metadataAppender, authorizationProviderFactory.create(subscription));
    }


    @Override
    public void start() throws Exception {
        startClient(httpClient);
        if (http2Client.isPresent()) {
            startClient(http2Client.get());
        }
    }

    private void startClient(HttpClient client) throws Exception {
        if (client.isStopped()) {
            client.start();
        }
    }

    @Override
    public void stop() throws Exception {
        stopClient(httpClient);
        if (http2Client.isPresent()) {
            stopClient(http2Client.get());
        }

    }

    private void stopClient(HttpClient client) throws Exception {
        if (client.isRunning()) {
            client.stop();
        }
    }
}
