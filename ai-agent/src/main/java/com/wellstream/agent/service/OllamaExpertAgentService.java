package com.wellstream.agent.service;

import com.wellstream.agent.model.AnomalyClassification;
import com.wellstream.agent.model.IncidentReport;
import com.wellstream.agent.model.WellAnomalyEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.ai.converter.BeanOutputConverter;
import org.springframework.ai.ollama.OllamaChatModel;
import org.springframework.ai.ollama.api.OllamaOptions;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Agent 2 — Expert (Root Cause Analyst).
 *
 * Uses Mistral-7B (Q4 quantized, ~4.1GB VRAM) via Ollama to perform
 * multi-step root cause analysis, incorporating Qdrant RAG context from
 * similar past incidents on this well pad.
 *
 * Why Mistral-7B for expert analysis:
 *   - Multi-step technical reasoning over structured sensor data requires
 *     a larger context window and stronger instruction-following than Phi-3.
 *   - Q4 quantization reduces VRAM from 14GB → 4.1GB with < 3% accuracy loss
 *     on structured technical benchmarks (MMLU Engineering subset).
 *   - RAG grounds the response in actual historical incidents — prevents the
 *     model from hallucinating failure modes that never occur on this well type.
 *   - Temperature 0.2 allows slight variation in language while keeping
 *     technical conclusions deterministic.
 */
@Service
@Profile("ai")
public class OllamaExpertAgentService implements ExpertAgentService {

    private static final Logger log = LoggerFactory.getLogger(OllamaExpertAgentService.class);

    private final OllamaChatModel chatModel;
    private final BeanOutputConverter<IncidentReport> converter;

    public OllamaExpertAgentService(OllamaChatModel chatModel) {
        this.chatModel = chatModel;
        this.converter = new BeanOutputConverter<>(IncidentReport.class);
    }

    @Override
    public IncidentReport diagnose(WellAnomalyEvent event,
                                   AnomalyClassification classification,
                                   List<String> ragContext) {
        String ragSection = ragContext.isEmpty()
            ? "No similar past incidents found in Qdrant — base diagnosis on sensor data only."
            : "SIMILAR PAST INCIDENTS (Qdrant RAG — top 3 by vector similarity):\n"
              + String.join("\n---\n", ragContext);

        String format = converter.getFormat();

        String promptText = """
            You are a senior petroleum engineer performing API 610 root cause analysis.

            ROUTER CLASSIFICATION (from Phi-3 Agent):
            - Anomaly type: %s  (confidence: %.0f%%)
            - Primary indicator: %s
            - Expert focus area: %s

            FULL SENSOR DATA:
            - Well: %s  |  Facility: %s
            - PHI: %.1f/100  |  Pressure: %.1f psi  |  Flow deviation: %.1f%% from BEP
            - Temperature rise: %.1f°F  |  Methane: %.1f ppm  (Z-score: %.2f)
            - Alarm level: %s

            %s

            TASK — Perform root cause analysis:
            1. Identify the specific affected component (e.g., "impeller stage 3", "inboard bearing race")
            2. Explain the physical failure mechanism using BEP deviation analysis
            3. State the immediate field action with response time (per API 610 Annex K)
            4. Estimate remaining useful life based on PHI trend and failure mode
            5. Set complianceFlag to REGULATORY_ACTION_REQUIRED if methane > 500 ppm (EPA 40 CFR Part 60)
               otherwise COMPLIANT

            Return ONLY valid JSON — no markdown, no preamble:
            %s
            """.formatted(
                classification.anomalyType(),
                classification.confidenceScore() * 100,
                classification.primaryIndicator(),
                classification.recommendedExpertFocus(),
                event.wellId(), event.facilityId(),
                event.pumpHealthIndex(), event.pressurePsi(), event.flowDeviationPct(),
                event.tempRiseF(), event.methanePpm(), event.methaneZscore(),
                event.currentAlarmLevel(),
                ragSection,
                format
            );

        OllamaOptions options = new OllamaOptions();
        options.setModel("mistral");
        options.setTemperature(0.2);

        ChatResponse response = chatModel.call(new Prompt(promptText, options));
        String raw = response.getResult().getOutput().getContent();
        log.info("[AGENT-2 MISTRAL RAW] {}", raw);
        return converter.convert(raw);
    }
}
