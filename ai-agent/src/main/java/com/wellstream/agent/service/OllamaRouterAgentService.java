package com.wellstream.agent.service;

import com.wellstream.agent.model.AnomalyClassification;
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

/**
 * Agent 1 — Router.
 *
 * Uses Phi-3 mini (3.8B params, ~2.3GB VRAM) via Ollama to classify
 * the incoming anomaly into one of seven known petroleum failure types.
 *
 * Why Phi-3 mini for routing:
 *   - Classification is a bounded, low-ambiguity task — smaller models excel here.
 *   - ~0.8–1.5s inference on an RTX 3060 vs 15–30s for GPT-4o via API.
 *   - Temperature 0.1 forces deterministic, low-variance output — critical for
 *     safety-classification tasks where hallucinated anomaly types cause false alarms.
 */
@Service
@Profile("ai")
public class OllamaRouterAgentService implements RouterAgentService {

    private static final Logger log = LoggerFactory.getLogger(OllamaRouterAgentService.class);

    private final OllamaChatModel chatModel;
    private final BeanOutputConverter<AnomalyClassification> converter;

    public OllamaRouterAgentService(OllamaChatModel chatModel) {
        this.chatModel = chatModel;
        this.converter = new BeanOutputConverter<>(AnomalyClassification.class);
    }

    @Override
    public AnomalyClassification classify(WellAnomalyEvent event) {
        String format = converter.getFormat();

        String promptText = """
            You are a petroleum engineering diagnostic system. Classify the anomaly below.

            SENSOR SNAPSHOT:
            - Well: %s  |  Facility: %s
            - Methane Z-score: %.2f  |  Methane: %.1f ppm  (EPA limit: 500 ppm)
            - Pump Health Index (PHI): %.1f / 100  (critical threshold: 25)
            - Pressure: %.1f psi  |  Flow deviation from BEP: %.1f%%
            - Temperature rise above design: %.1f°F  (bearing failure onset: >35°F)
            - Current alarm: %s  |  Emission risk: %s

            CLASSIFICATION RULES (petroleum engineering diagnostics):
            - SCALE_WAX_BUILDUP         →  ↓ flow AND ↑ pressure simultaneously
            - BEARING_DEGRADATION       →  ↑ temperature AND ↑ vibration (temp rise > 30°F)
            - SEAL_FAILURE              →  ↑ temperature, stable flow and pressure
            - GAS_INTERFERENCE_CAVITATION → methane > 400 ppm AND flow deviation < -10%%
            - EMISSION_EXCEEDANCE       →  methane Z-score > 3.5 OR methane > 500 ppm
            - MECHANICAL_FAILURE        →  ↓ flow AND ↓ pressure (impeller geometry loss)
            - NORMAL_VARIANCE           →  no significant deviation across all signals

            Return ONLY valid JSON matching this schema — no explanation, no markdown:
            %s
            """.formatted(
                event.wellId(), event.facilityId(),
                event.methaneZscore(), event.methanePpm(),
                event.pumpHealthIndex(), event.pressurePsi(), event.flowDeviationPct(),
                event.tempRiseF(),
                event.currentAlarmLevel(), event.emissionRiskLevel(),
                format
            );

        OllamaOptions options = new OllamaOptions();
        options.setModel("phi3:mini");
        options.setTemperature(0.1);

        ChatResponse response = chatModel.call(new Prompt(promptText, options));
        String raw = response.getResult().getOutput().getContent();
        log.info("[AGENT-1 PHI3 RAW] {}", raw);
        return converter.convert(raw);
    }
}
