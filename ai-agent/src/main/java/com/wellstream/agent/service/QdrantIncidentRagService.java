package com.wellstream.agent.service;

import com.wellstream.agent.model.AnomalyClassification;
import com.wellstream.agent.model.IncidentReport;
import com.wellstream.agent.model.WellAnomalyEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.document.Document;
import org.springframework.ai.vectorstore.SearchRequest;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * RAG layer — Qdrant vector store of historical incident reports.
 *
 * Every confirmed incident is indexed as a vector embedding (nomic-embed-text, 768-dim).
 * Before the Expert Agent analyses a new anomaly, the top-3 most similar past
 * incidents are retrieved and injected into the Mistral-7B prompt as context.
 *
 * This prevents the LLM from hallucinating failure modes that never occur on
 * this well type, and grounds the RUL estimate in observed historical degradation rates.
 *
 * Production evolution:
 *   - Add well-specific metadata filters (well type, tubing diameter, fluid gravity)
 *     so RAG retrieves incidents from similar wells, not just similar anomaly text.
 *   - Index production reports, workover summaries, and pump manufacturer bulletins
 *     as additional RAG sources.
 */
@Service
@Profile("ai")
public class QdrantIncidentRagService implements IncidentRagService {

    private static final Logger log = LoggerFactory.getLogger(QdrantIncidentRagService.class);
    private static final double SIMILARITY_THRESHOLD = 0.70;
    private static final int TOP_K = 3;

    private final VectorStore vectorStore;

    public QdrantIncidentRagService(VectorStore vectorStore) {
        this.vectorStore = vectorStore;
    }

    @Override
    public List<String> retrieveSimilarIncidents(WellAnomalyEvent event, AnomalyClassification classification) {
        String query = classification.anomalyType() + " "
            + classification.primaryIndicator() + " "
            + classification.recommendedExpertFocus();

        log.info("[RAG] Querying Qdrant: \"{}\" (topK={}, threshold={})", query, TOP_K, SIMILARITY_THRESHOLD);

        List<Document> hits = vectorStore.similaritySearch(
            SearchRequest.query(query)
                .withTopK(TOP_K)
                .withSimilarityThreshold(SIMILARITY_THRESHOLD)
        );

        log.info("[RAG] Retrieved {} similar incidents", hits.size());
        return hits.stream().map(Document::getContent).collect(Collectors.toList());
    }

    @Override
    public void storeIncident(IncidentReport report) {
        String content = String.format(
            "Well: %s | Facility: %s | Type: %s | Component: %s | Cause: %s | Action: %s | RUL: %s | Severity: %.2f",
            report.wellId(), report.facilityId(), report.anomalyType(),
            report.affectedComponent(), report.rootCause(),
            report.immediateAction(), report.estimatedRul(), report.severity()
        );

        Document doc = new Document(content, Map.of(
            "wellId",      report.wellId(),
            "facilityId",  report.facilityId(),
            "anomalyType", report.anomalyType(),
            "severity",    String.valueOf(report.severity()),
            "timestamp",   report.generatedAt()
        ));

        vectorStore.add(List.of(doc));
        log.info("[RAG] Incident indexed in Qdrant for future retrieval: {} / {}", report.wellId(), report.anomalyType());
    }
}
