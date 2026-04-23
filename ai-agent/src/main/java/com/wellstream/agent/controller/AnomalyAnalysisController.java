package com.wellstream.agent.controller;

import com.wellstream.agent.model.IncidentReport;
import com.wellstream.agent.model.WellAnomalyEvent;
import com.wellstream.agent.service.AnomalyWorkflowOrchestrator;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/anomalies")
@Tag(name = "Anomaly Analysis", description = "2-stage agentic SLM workflow — Phi-3 Router → Mistral-7B Expert")
@SecurityRequirement(name = "basicAuth")
public class AnomalyAnalysisController {

    private final AnomalyWorkflowOrchestrator orchestrator;

    public AnomalyAnalysisController(AnomalyWorkflowOrchestrator orchestrator) {
        this.orchestrator = orchestrator;
    }

    @PostMapping("/analyze")
    @Operation(
        summary = "Run agentic anomaly analysis",
        description = """
            Executes the 2-stage agentic workflow on a well anomaly event:

            **Step 1 — Phi-3 mini (Router):** Classifies the anomaly type from sensor signals.
            Returns a structured `AnomalyClassification` with type, confidence, and primary indicator.

            **Step 2 — Qdrant RAG:** Retrieves the top-3 most similar past incidents by
            vector cosine similarity (nomic-embed-text, 768-dim). Grounds the expert prompt
            in real historical cases from this well pad.

            **Step 3 — Mistral-7B (Expert):** Performs API 610 root cause analysis using the
            classification and RAG context. Returns a SOC2-compliant `IncidentReport`.

            **Step 4 — Index:** Stores the new incident in Qdrant to improve future RAG quality.

            Requires `FIELD_OPERATOR` or `HSE_ENGINEER` role.
            Watch the Spring Boot console for step-by-step agent hand-off logs.
            """
    )
    public ResponseEntity<IncidentReport> analyzeAnomaly(@RequestBody WellAnomalyEvent event) {
        return ResponseEntity.ok(orchestrator.analyze(event));
    }
}
