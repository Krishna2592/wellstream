package com.wellstream.agent.service;

import com.wellstream.agent.model.AnomalyClassification;
import com.wellstream.agent.model.IncidentReport;
import com.wellstream.agent.model.WellAnomalyEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;

/**
 * Orchestrates the 2-stage agentic workflow:
 *
 *   Step 1 — ROUTER  (Phi-3 mini):
 *     Classifies the anomaly type from raw sensor signals.
 *     Structured output → AnomalyClassification POJO via BeanOutputConverter.
 *
 *   Step 2 — RAG:
 *     Retrieves top-3 similar past incidents from Qdrant using
 *     nomic-embed-text vector embeddings (768-dim cosine similarity).
 *
 *   Step 3 — EXPERT  (Mistral-7B):
 *     Performs root cause analysis using classification + RAG context.
 *     Structured output → IncidentReport POJO via BeanOutputConverter.
 *
 *   Step 4 — INDEX:
 *     Stores the confirmed incident in Qdrant to improve future RAG quality.
 *
 * The console log output produced here — with explicit step labels and
 * model hand-off lines — is the "agent hand-off" sequence visible in demos.
 */
@Service
public class AnomalyWorkflowOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(AnomalyWorkflowOrchestrator.class);

    private final RouterAgentService router;
    private final ExpertAgentService expert;
    private final IncidentRagService rag;

    public AnomalyWorkflowOrchestrator(RouterAgentService router,
                                        ExpertAgentService expert,
                                        IncidentRagService rag) {
        this.router = router;
        this.expert = expert;
        this.rag    = rag;
    }

    public IncidentReport analyze(WellAnomalyEvent event) {
        log.info("╔══════════════════════════════════════════════════════════╗");
        log.info("║  WELLSTREAM AGENTIC WORKFLOW — Well: {}  Facility: {}  ║", event.wellId(), event.facilityId());
        log.info("╚══════════════════════════════════════════════════════════╝");

        // ── Step 1: Router Agent (Phi-3 mini) ──────────────────────────────
        log.info("[STEP 1 ▶ ROUTER] Phi-3 mini classifying anomaly...");
        long t1 = System.currentTimeMillis();
        AnomalyClassification classification = router.classify(event);
        log.info("[STEP 1 ✓ ROUTER] type={} | confidence={:.0f}% | indicator=\"{}\"",
            classification.anomalyType(),
            classification.confidenceScore() * 100,
            classification.primaryIndicator());
        log.info("[STEP 1 ✓ ROUTER] completed in {}ms", System.currentTimeMillis() - t1);

        // ── Step 2: RAG — retrieve similar past incidents ──────────────────
        log.info("[STEP 2 ▶ RAG] Querying Qdrant for similar incidents...");
        long t2 = System.currentTimeMillis();
        List<String> ragContext = rag.retrieveSimilarIncidents(event, classification);
        log.info("[STEP 2 ✓ RAG] {} similar incidents retrieved in {}ms",
            ragContext.size(), System.currentTimeMillis() - t2);

        // ── Step 3: Expert Agent (Mistral-7B) ──────────────────────────────
        log.info("[STEP 3 ▶ EXPERT] Handing off to Mistral-7B for root cause analysis...");
        log.info("[STEP 3 ▶ EXPERT] Context: classification={}, ragDocs={}", classification.anomalyType(), ragContext.size());
        long t3 = System.currentTimeMillis();
        IncidentReport report = expert.diagnose(event, classification, ragContext);
        log.info("[STEP 3 ✓ EXPERT] rootCause=\"{}\" | component=\"{}\" | RUL={} | severity={:.2f}",
            report.rootCause(), report.affectedComponent(),
            report.estimatedRul(), report.severity());
        log.info("[STEP 3 ✓ EXPERT] compliance={} | completed in {}ms",
            report.complianceFlag(), System.currentTimeMillis() - t3);

        // ── Step 4: Index incident in Qdrant ───────────────────────────────
        log.info("[STEP 4 ▶ INDEX] Storing incident in Qdrant for future RAG...");
        rag.storeIncident(report);

        log.info("╔══════════════════════════════════════════════════════════╗");
        log.info("║  WORKFLOW COMPLETE — action: \"{}\"", report.immediateAction());
        log.info("╚══════════════════════════════════════════════════════════╝");

        return report;
    }
}
