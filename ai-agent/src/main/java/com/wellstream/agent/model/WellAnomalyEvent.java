package com.wellstream.agent.model;

public record WellAnomalyEvent(
    String wellId,
    String facilityId,
    double methaneZscore,
    double methanePpm,
    double pumpHealthIndex,
    double pressurePsi,
    double tempRiseF,
    double flowDeviationPct,
    String emissionRiskLevel,
    String currentAlarmLevel
) {}
