#!/bin/bash
# Run data pipeline if cache is empty (first deploy or fresh container)
if [ ! -d "data/cache/google_trends" ] || [ -z "$(ls -A data/cache/google_trends 2>/dev/null)" ]; then
    echo "[entrypoint] No pipeline data found — running backfill..."
    python -m src.main --backfill || echo "[entrypoint] Pipeline backfill had errors (continuing anyway)"
fi

# Warm AI caches in background (so Streamlit starts immediately)
echo "[entrypoint] Starting cache warm in background..."
python warm_cache.py &

# Start Streamlit on the Cloud Run PORT
echo "[entrypoint] Starting Streamlit on port ${PORT:-8080}..."
exec streamlit run dashboard.py \
    --server.port="${PORT:-8080}" \
    --server.address=0.0.0.0 \
    --server.headless=true \
    --browser.gatherUsageStats=false \
    --server.enableCORS=false \
    --server.enableXsrfProtection=false
