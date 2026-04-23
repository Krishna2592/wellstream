package com.wellstream.agent.model;

/**
 * Structured output from Agent 1 (Phi-3 mini Router).
 * BeanOutputConverter serialises this record's schema into the prompt so the
 * LLM returns valid JSON that maps directly to this type.
 */
public record AnomalyClassification(
    String anomalyType,           // MECHANICAL_FAILURE | GAS_INTERFERENCE_CAVITATION | SCALE_WAX_BUILDUP |
                                  // BEARING_DEGRADATION | SEAL_FAILURE | EMISSION_EXCEEDANCE | NORMAL_VARIANCE
    double confidenceScore,       // 0.0 – 1.0
    String primaryIndicator,      // human-readable description of the leading signal
    String recommendedExpertFocus // hint to the Expert Agent on what to focus the RCA
) {}
