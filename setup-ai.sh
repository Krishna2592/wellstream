#!/usr/bin/env bash
# =====================================================================
# WellStream AI Setup — pulls SLM models into the Ollama container
#
# Run AFTER: docker compose -f docker-compose.ai.yml up -d ollama
#
# Models pulled (~7GB total download, cached in Docker volume):
#   phi3:mini          3.8B params   ~2.3GB   Router Agent (classification)
#   mistral            7B params Q4  ~4.1GB   Expert Agent (root cause analysis)
#   nomic-embed-text   137M params   ~274MB   RAG embeddings (Qdrant)
#
# Hardware requirements:
#   GPU mode  — NVIDIA GPU with 8GB+ VRAM (RTX 3060 or better)
#   CPU mode  — 16GB+ RAM (inference will be 5–10× slower but functional)
# =====================================================================
set -euo pipefail

OLLAMA_URL="${OLLAMA_URL:-http://localhost:11434}"
CONTAINER="${OLLAMA_CONTAINER:-wellstream-ollama}"

echo "==================================================================="
echo "  WellStream AI — SLM Model Setup"
echo "==================================================================="
echo ""

# Wait for Ollama to be ready
echo "Waiting for Ollama to be ready at $OLLAMA_URL ..."
until curl -sf "$OLLAMA_URL/api/tags" > /dev/null 2>&1; do
    echo "  ... not ready yet, retrying in 3s"
    sleep 3
done
echo "Ollama is ready."
echo ""

pull_model() {
    local model="$1"
    local desc="$2"
    echo "Pulling $model  ($desc)..."
    docker exec "$CONTAINER" ollama pull "$model"
    echo "  ✓ $model ready"
    echo ""
}

pull_model "phi3:mini"         "Router Agent — anomaly classifier, ~2.3GB"
pull_model "mistral"           "Expert Agent — root cause analyst, ~4.1GB Q4"
pull_model "nomic-embed-text"  "RAG embeddings for Qdrant, ~274MB"

echo "==================================================================="
echo "  All models loaded. Current Ollama model list:"
echo "==================================================================="
docker exec "$CONTAINER" ollama list
echo ""
echo "Start the full AI stack:"
echo "  docker compose -f docker-compose.ai.yml up -d"
echo ""
echo "Swagger UI (HSE_ENGINEER role):"
echo "  http://localhost:8081/swagger-ui.html"
echo "  Username: hse_engineer  |  Password: wellstream_hse"
echo ""
echo "Test the agentic workflow (field operator credentials):"
echo '  curl -s -u field_operator:wellstream_ops \'
echo '       -X POST http://localhost:8081/api/v1/anomalies/analyze \'
echo '       -H "Content-Type: application/json" \'
echo '       -d '"'"'{"wellId":"WELL-18","facilityId":"FAC-8","methaneZscore":4.2,'
echo '            "methanePpm":678.0,"pumpHealthIndex":22.4,"pressurePsi":312.0,'
echo '            "tempRiseF":38.5,"flowDeviationPct":-23.1,'
echo '            "emissionRiskLevel":"HIGH_EMISSION","currentAlarmLevel":"L3_CRITICAL"}'"'"' | jq .'
