package com.vitalstream.api.security;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.grpc.server.GlobalServerInterceptor;
import org.springframework.grpc.server.security.AuthenticationProcessInterceptor;
import org.springframework.grpc.server.security.GrpcSecurity;
import org.springframework.security.oauth2.jwt.JwtDecoder;

/**
 * Who may call which gRPC method. The gRPC counterpart of SecurityConfig: the client sends the same kind of
 * Keycloak JWT, as call metadata "authorization: Bearer <token>" (gRPC metadata works like HTTP headers).
 *
 * Adding Spring Security to the project made Spring gRPC require a token on every call automatically;
 * this bean replaces that default with explicit per-method rules.
 *
 *   no/invalid token -> status UNAUTHENTICATED     (REST: 401)
 *   wrong role       -> status PERMISSION_DENIED   (REST: 403)
 */
@Configuration
public class GrpcSecurityConfig {

    // @GlobalServerInterceptor attaches this interceptor to every gRPC service. Without it the bean exists but
    // is never called, and because it also switches off Spring gRPC's default (token required everywhere),
    // the result is a server with NO security at all. That happened while building this: every call
    // succeeded, even without a token. Always test that the "should be refused" cases are refused.
    @Bean
    @GlobalServerInterceptor
    public AuthenticationProcessInterceptor grpcCallSecurity(GrpcSecurity grpc, JwtDecoder jwtDecoder) throws Exception {
        return grpc
                .authorizeRequests(calls -> calls
                        // Patterns match the full method name, "<package>.<Service>/<Method>"; * is a wildcard.
                        .methods("vitalstream.ingest.v1.IngestService/*").hasAuthority("ROLE_device")
                        // Standard gRPC health checks must work without a token (load balancers, Kubernetes).
                        .methods("grpc.health.v1.Health/*").permitAll()
                        // Reflection only describes the API (as grpcurl list/describe show), no data; convenient
                        // in development. A production server would usually turn it off or require a token.
                        .methods("grpc.reflection.v1.ServerReflection/*").permitAll()
                        .allRequests().denyAll())
                // Same token checks as REST: the JwtDecoder bean built from spring.security.oauth2.resourceserver
                // (issuer, audience, signature, expiry) and the same Keycloak role mapping.
                .oauth2ResourceServer(resourceServer -> resourceServer.jwt(jwt -> jwt
                        .decoder(jwtDecoder)
                        .jwtAuthenticationConverter(SecurityConfig.keycloakRoles())))
                .build();
    }
}
