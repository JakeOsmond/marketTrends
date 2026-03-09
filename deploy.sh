#!/bin/bash
set -e

# ============================================================================
# HX Insurance Pulse — Google Cloud Run Deployment
# ============================================================================
# Prerequisites:
#   1. Google Cloud account with billing enabled (free tier — no charges)
#   2. gcloud CLI installed: https://cloud.google.com/sdk/docs/install
#   3. Logged in: gcloud auth login
#
# Usage:
#   chmod +x deploy.sh
#   ./deploy.sh              # First-time setup + deploy
#   ./deploy.sh --update     # Re-deploy after code changes
# ============================================================================

PROJECT_ID="${GCP_PROJECT_ID:-hx-insurance-pulse}"
REGION="us-central1"  # Required for free tier
SERVICE_NAME="hx-dashboard"
SCHEDULER_TZ="Europe/London"  # Auto-handles BST/GMT

# Colours for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "${GREEN}[deploy]${NC} $1"; }
warn() { echo -e "${YELLOW}[deploy]${NC} $1"; }

# --------------------------------------------------------------------------
# Check prerequisites
# --------------------------------------------------------------------------
if ! command -v gcloud &> /dev/null; then
    echo "ERROR: gcloud CLI not found. Install from https://cloud.google.com/sdk/docs/install"
    exit 1
fi

# Check for required secrets
if [ -z "$OPENAI_API_KEY" ]; then
    if [ -f .env ]; then
        export $(grep -v '^#' .env | xargs)
    fi
fi

if [ -z "$OPENAI_API_KEY" ]; then
    echo "ERROR: OPENAI_API_KEY not set. Export it or add to .env file."
    exit 1
fi

WEBHOOK_URL="${APPS_SCRIPT_WEBHOOK_URL:-}"

# --------------------------------------------------------------------------
# First-time setup (skip with --update)
# --------------------------------------------------------------------------
if [ "$1" != "--update" ]; then
    log "Setting up Google Cloud project..."

    # Create project if it doesn't exist
    if ! gcloud projects describe "$PROJECT_ID" &> /dev/null; then
        log "Creating project: $PROJECT_ID"
        gcloud projects create "$PROJECT_ID" --name="HX Insurance Pulse"
    fi

    gcloud config set project "$PROJECT_ID"

    # Enable required APIs
    log "Enabling APIs (Cloud Run, Artifact Registry, Cloud Scheduler)..."
    gcloud services enable \
        run.googleapis.com \
        artifactregistry.googleapis.com \
        cloudscheduler.googleapis.com \
        cloudbuild.googleapis.com \
        2>/dev/null || true

    # Create Artifact Registry repo for Docker images
    if ! gcloud artifacts repositories describe hx-repo --location="$REGION" &> /dev/null 2>&1; then
        log "Creating Artifact Registry repository..."
        gcloud artifacts repositories create hx-repo \
            --repository-format=docker \
            --location="$REGION" \
            --description="HX Dashboard Docker images"
    fi
fi

# --------------------------------------------------------------------------
# Build and deploy
# --------------------------------------------------------------------------
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/hx-repo/${SERVICE_NAME}:latest"

log "Building container image with Cloud Build..."
gcloud builds submit --tag "$IMAGE" --timeout=600

log "Deploying to Cloud Run..."
ENV_VARS="OPENAI_API_KEY=${OPENAI_API_KEY}"
if [ -n "$WEBHOOK_URL" ]; then
    ENV_VARS="${ENV_VARS},APPS_SCRIPT_WEBHOOK_URL=${WEBHOOK_URL}"
fi

gcloud run deploy "$SERVICE_NAME" \
    --image "$IMAGE" \
    --region "$REGION" \
    --platform managed \
    --allow-unauthenticated \
    --memory 1Gi \
    --cpu 1 \
    --timeout 300 \
    --concurrency 10 \
    --min-instances 0 \
    --max-instances 2 \
    --set-env-vars "$ENV_VARS" \
    --port 8080

# Get the service URL
SERVICE_URL=$(gcloud run services describe "$SERVICE_NAME" \
    --region "$REGION" --format='value(status.url)')

log "Dashboard deployed at: $SERVICE_URL"

# --------------------------------------------------------------------------
# Set up Cloud Scheduler (daily refresh + keep-alive during work hours)
# --------------------------------------------------------------------------
if [ "$1" != "--update" ]; then
    log "Setting up Cloud Scheduler..."

    # Job 1: Daily cache warm at 6am UK time (handles BST/GMT automatically)
    if gcloud scheduler jobs describe hx-daily-warm --location="$REGION" &> /dev/null 2>&1; then
        gcloud scheduler jobs update http hx-daily-warm \
            --location="$REGION" \
            --schedule="0 6 * * *" \
            --time-zone="$SCHEDULER_TZ" \
            --uri="${SERVICE_URL}" \
            --http-method=GET \
            --attempt-deadline=60s
    else
        gcloud scheduler jobs create http hx-daily-warm \
            --location="$REGION" \
            --schedule="0 6 * * *" \
            --time-zone="$SCHEDULER_TZ" \
            --uri="${SERVICE_URL}" \
            --http-method=GET \
            --attempt-deadline=60s \
            --description="Wake dashboard and warm caches at 6am UK time"
    fi

    # Job 2: Keep-alive pings every 10 min during work hours (Mon-Fri 6am-6pm UK)
    # This prevents the container from scaling to zero while the team might need it
    if gcloud scheduler jobs describe hx-keepalive --location="$REGION" &> /dev/null 2>&1; then
        gcloud scheduler jobs update http hx-keepalive \
            --location="$REGION" \
            --schedule="*/10 6-18 * * 1-5" \
            --time-zone="$SCHEDULER_TZ" \
            --uri="${SERVICE_URL}" \
            --http-method=GET \
            --attempt-deadline=30s
    else
        gcloud scheduler jobs create http hx-keepalive \
            --location="$REGION" \
            --schedule="*/10 6-18 * * 1-5" \
            --time-zone="$SCHEDULER_TZ" \
            --uri="${SERVICE_URL}" \
            --http-method=GET \
            --attempt-deadline=30s \
            --description="Keep container warm during UK work hours (Mon-Fri 6am-6pm)"
    fi

    log "Scheduler configured:"
    log "  - Daily warm: 6:00 AM ${SCHEDULER_TZ}"
    log "  - Keep-alive: Every 10 min, Mon-Fri 6am-6pm ${SCHEDULER_TZ}"
fi

# --------------------------------------------------------------------------
# Done
# --------------------------------------------------------------------------
echo ""
echo "============================================"
echo "  HX Insurance Pulse — Deployed!"
echo "============================================"
echo ""
echo "  Dashboard: $SERVICE_URL"
echo ""
echo "  Schedule:"
echo "    - Cache warm:  6:00 AM UK (daily)"
echo "    - Keep-alive:  Every 10 min, Mon-Fri 6am-6pm UK"
echo ""
echo "  To re-deploy after code changes:"
echo "    ./deploy.sh --update"
echo ""
echo "  To view logs:"
echo "    gcloud run services logs read $SERVICE_NAME --region=$REGION"
echo ""
echo "  Free tier limits:"
echo "    - 2M requests/month (you'll use ~2K)"
echo "    - 180K vCPU-seconds/month (~50 hours)"
echo "    - 360K GiB-seconds/month (~100 hours)"
echo "============================================"
