package com.wellstream.agent.service;

import com.wellstream.agent.model.AnomalyClassification;
import com.wellstream.agent.model.IncidentReport;
import com.wellstream.agent.model.WellAnomalyEvent;

import java.util.List;

public interface IncidentRagService {
    List<String> retrieveSimilarIncidents(WellAnomalyEvent event, AnomalyClassification classification);
    void storeIncident(IncidentReport report);
}
