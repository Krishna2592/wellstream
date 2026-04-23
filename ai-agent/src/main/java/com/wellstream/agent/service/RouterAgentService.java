package com.wellstream.agent.service;

import com.wellstream.agent.model.AnomalyClassification;
import com.wellstream.agent.model.WellAnomalyEvent;

public interface RouterAgentService {
    AnomalyClassification classify(WellAnomalyEvent event);
}
