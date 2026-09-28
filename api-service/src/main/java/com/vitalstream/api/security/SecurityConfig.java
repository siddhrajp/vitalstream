package com.vitalstream.api.security;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.http.HttpMethod;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.security.web.SecurityFilterChain;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Who may call what. Every request must carry "Authorization: Bearer <JWT from Keycloak>"; Spring Security
 * validates it (signature, issuer, audience, expiry: see spring.security.oauth2.resourceserver in
 * application.yml) and then applies the rules below.
 *
 *   401 Unauthorized = no token, or a token that isn't valid ("we don't know who you are")
 *   403 Forbidden    = valid token, but its roles don't allow this request ("we know, and you may not")
 */
@Configuration
public class SecurityConfig {

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        http
                .authorizeHttpRequests(auth -> auth
                        // Rules are checked top to bottom and the first match wins, so specific ones come first.
                        .requestMatchers("/actuator/health/**", "/actuator/info").permitAll()
                        .requestMatchers(HttpMethod.POST, "/api/devices/*/readings").hasRole("device")
                        .requestMatchers(HttpMethod.GET, "/api/patients/**", "/api/devices/**")
                            .hasAnyRole("admin", "clinician")
                        .requestMatchers("/api/patients/**", "/api/devices/**").hasRole("admin")
                        .requestMatchers("/actuator/**").hasRole("admin")
                        // Anything not listed above is refused, so a new endpoint is locked until given a rule.
                        .anyRequest().denyAll())
                .oauth2ResourceServer(oauth -> oauth.jwt(jwt -> jwt.jwtAuthenticationConverter(keycloakRoles())))
                // Each request is authenticated by its own token: no login session, no session cookie...
                .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                // ...and CSRF protection guards against a browser sending cookies to a site on its own; with no
                // cookies to steal, it has nothing to protect here.
                .csrf(csrf -> csrf.disable());
        return http.build();
    }

    /**
     * Keycloak puts realm roles in the token as  "realm_access": { "roles": ["clinician"] }.
     * Spring Security expects "authorities"; hasRole("clinician") checks for the authority "ROLE_clinician".
     * This converter bridges the two. The authenticated user's name is the "preferred_username" claim
     * (e.g. "alice") instead of the default "sub" (a UUID), which makes logs readable.
     */
    static Converter<Jwt, AbstractAuthenticationToken> keycloakRoles() {
        return jwt -> new JwtAuthenticationToken(jwt, realmRoles(jwt), jwt.getClaimAsString("preferred_username"));
    }

    public static Collection<GrantedAuthority> realmRoles(Jwt jwt) {
        Map<String, Object> realmAccess = jwt.getClaimAsMap("realm_access");
        if (realmAccess == null || !(realmAccess.get("roles") instanceof List<?> roles)) {
            return List.of();
        }
        return roles.stream()
                .map(role -> (GrantedAuthority) new SimpleGrantedAuthority("ROLE_" + role))
                .toList();
    }
}
