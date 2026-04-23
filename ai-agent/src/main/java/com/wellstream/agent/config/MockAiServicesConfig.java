package com.wellstream.agent.config;

import com.wellstream.agent.model.AnomalyClassification;
import com.wellstream.agent.model.IncidentReport;
import com.wellstream.agent.service.ExpertAgentService;
import com.wellstream.agent.service.IncidentRagService;
import com.wellstream.agent.service.RouterAgentService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.time.Instant;
import java.util.List;

/**
 * Mock AI service beans — active when the "ai" Spring profile is NOT set.
 *
 * Used in CI (no GPU, no Ollama) and local development without Ollama running.
 * Each bean returns a hardcoded but realistic response so the REST API and
 * orchestrator logic can be exercised without inference infrastructure.
 *
 * To switch to real SLM inference:
 *   SPRING_PROFILES_ACTIVE=ai  (requires Ollama + Qdrant via docker-compose.ai.yml)
 */
@Configuration
@Profile("!ai")
public class MockAiServicesConfig {

    private static final Logger log = LoggerFactory.getLogger(MockAiServicesConfig.class);

    @Bean
    public RouterAgentService routerAgentService() {
        return event -> {
            log.info("[MOCK ROUTER] Returning hardcoded classification (start with --spring.profiles.active=ai for real Phi-3 inference)");
            return new AnomalyClassification(
                "MECHANICAL_FAILURE",
                0.91,
                "PHI=22.4 with temp rise 38°F — sustained off-BEP operation",
                "BEP deviation analysis — check impeller geometry and wear ring clearance"
            );
        };
    }

    @Bean
    public ExpertAgentService expertAgentService() {
        return (event, classification, ragContext) -> {
            log.info("[MOCK EXPERT] Returning hardcoded incident report (start with --spring.profiles.active=ai for real Mistral inference)");
            return new IncidentReport(
                event.wellId(),
                event.facilityId(),
                classification.anomalyType(),
                "Impeller cavitation caused by sustained operation at 23% below BEP flow rate",
                "Impeller stage 3 — wear ring clearance exceeded API 610 limit",
                "Reduce production rate by 15%, dispatch field team within 4h, prepare impeller stack replacement",
                "14–21 days at current degradation rate",
                0.78,
                event.methanePpm() > 500 ? "REGULATORY_ACTION_REQUIRED" : "COMPLIANT",
                Instant.now().toString()
            );
        };
    }

    @Bean
    public IncidentRagService incidentRagService() {
        return new IncidentRagService() {
            @Override
            public List<String> retrieveSimilarIncidents(
                    com.wellstream.agent.model.WellAnomalyEvent event,
                    AnomalyClassification classification) {
                log.info("[MOCK RAG] Returning empty context (Qdrant not running in mock profile)");
                return List.of();
            }

            @Override
            public void storeIncident(IncidentReport report) {
                log.info("[MOCK RAG] storeIncident no-op in mock profile");
            }
        };
    }
}
