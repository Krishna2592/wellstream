package com.wellstream.agent;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@OpenAPIDefinition(info = @Info(
    title = "WellStream AI Agent",
    version = "1.0",
    description = "On-premise agentic SLM inference for petroleum well anomaly diagnosis. " +
                  "2-stage pipeline: Phi-3 mini (anomaly router) → Mistral-7B (expert RCA) with Qdrant RAG. " +
                  "No data leaves the well site."
))
public class AiAgentApplication {
    public static void main(String[] args) {
        SpringApplication.run(AiAgentApplication.class, args);
    }
}
