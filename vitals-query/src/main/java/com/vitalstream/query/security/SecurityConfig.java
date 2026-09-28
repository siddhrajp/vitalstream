package com.vitalstream.query.security;

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
 * The query API holds patient vitals, so only people who look after patients may read it: clinicians and
 * admins. Devices only ever write readings (to api-service) and get nothing here.
 * Same token checks and Keycloak role mapping as api-service (the services share no code, so the small
 * converter is repeated here on purpose).
 */
@Configuration
public class SecurityConfig {

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        http
                .authorizeHttpRequests(auth -> auth
                        .requestMatchers("/actuator/health/**", "/actuator/info").permitAll()
                        .requestMatchers(HttpMethod.GET, "/api/**").hasAnyRole("admin", "clinician")
                        .requestMatchers("/actuator/**").hasRole("admin")
                        .anyRequest().denyAll())
                .oauth2ResourceServer(oauth -> oauth.jwt(jwt -> jwt.jwtAuthenticationConverter(keycloakRoles())))
                .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .csrf(csrf -> csrf.disable());
        return http.build();
    }

    /** Keycloak's "realm_access": {"roles": [...]} -> Spring authorities "ROLE_<role>". */
    static Converter<Jwt, AbstractAuthenticationToken> keycloakRoles() {
        return jwt -> new JwtAuthenticationToken(jwt, realmRoles(jwt), jwt.getClaimAsString("preferred_username"));
    }

    static Collection<GrantedAuthority> realmRoles(Jwt jwt) {
        Map<String, Object> realmAccess = jwt.getClaimAsMap("realm_access");
        if (realmAccess == null || !(realmAccess.get("roles") instanceof List<?> roles)) {
            return List.of();
        }
        return roles.stream()
                .map(role -> (GrantedAuthority) new SimpleGrantedAuthority("ROLE_" + role))
                .toList();
    }
}
