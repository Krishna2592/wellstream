package com.wellstream.agent.model;

/**
 * Structured output from Agent 2 (Mistral-7B Expert).
 * SOC2-compliant incident record stored in Qdrant for future RAG retrieval.
 */
public record IncidentReport(
    String wellId,
    String facilityId,
    String anomalyType,
    String rootCause,             // physical failure mechanism (e.g., "impeller cavitation at 23% BEP deviation")
    String affectedComponent,     // specific part (e.g., "impeller stage 3", "bearing race B")
    String immediateAction,       // field action with response time (e.g., "reduce flow rate, dispatch within 4h")
    String estimatedRul,          // remaining useful life estimate
    double severity,              // 0.0 – 1.0 (maps to L1–L4 alarm scale)
    String complianceFlag,        // COMPLIANT | REGULATORY_ACTION_REQUIRED (SOC2 / EPA field)
    String generatedAt            // ISO-8601 timestamp
) {}
