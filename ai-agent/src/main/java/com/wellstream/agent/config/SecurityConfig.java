package com.wellstream.agent.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;

/**
 * SOC2-aligned RBAC — three roles mapping to petroleum operations hierarchy:
 *
 *   HSE_ENGINEER   — full access: trigger analysis, read reports, access Swagger UI
 *                    (Health, Safety & Environment team — owns incident response)
 *
 *   FIELD_OPERATOR — trigger analysis + read reports
 *                    (Well site operators who act on alarms)
 *
 *   AUDITOR        — read-only access to incident reports
 *                    (Compliance / regulatory audit access)
 *
 * Production hardening (not in this demo):
 *   - Replace InMemoryUserDetailsManager with LDAP/Active Directory (common in Aramco/SLB)
 *   - Add JWT token authentication for service-to-service calls from SCADA
 *   - Enable HTTPS — in this demo HTTP is used; production uses TLS with well-site PKI
 *   - Add audit logging of all /analyze calls (SOC2 CC6.8 — access activity logging)
 */
@Configuration
@EnableWebSecurity
public class SecurityConfig {

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http
            .authorizeHttpRequests(auth -> auth
                .requestMatchers("/actuator/health").permitAll()
                .requestMatchers("/swagger-ui/**", "/api-docs/**", "/swagger-ui.html")
                    .hasRole("HSE_ENGINEER")
                .requestMatchers("/api/v1/anomalies/analyze")
                    .hasAnyRole("HSE_ENGINEER", "FIELD_OPERATOR")
                .requestMatchers("/api/v1/incidents/**")
                    .hasAnyRole("HSE_ENGINEER", "FIELD_OPERATOR", "AUDITOR")
                .anyRequest().authenticated()
            )
            .httpBasic(Customizer.withDefaults())
            // CSRF disabled for stateless REST API — production uses JWT + CSRF tokens
            .csrf(AbstractHttpConfigurer::disable);
        return http.build();
    }

    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    @Bean
    public UserDetailsService userDetailsService(PasswordEncoder encoder) {
        UserDetails hseEngineer = User.builder()
            .username("hse_engineer")
            .password(encoder.encode("wellstream_hse"))
            .roles("HSE_ENGINEER")
            .build();

        UserDetails fieldOperator = User.builder()
            .username("field_operator")
            .password(encoder.encode("wellstream_ops"))
            .roles("FIELD_OPERATOR")
            .build();

        UserDetails auditor = User.builder()
            .username("auditor")
            .password(encoder.encode("wellstream_audit"))
            .roles("AUDITOR")
            .build();

        return new InMemoryUserDetailsManager(hseEngineer, fieldOperator, auditor);
    }
}
