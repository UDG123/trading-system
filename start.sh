#!/bin/bash
if [ "$SERVICE_ROLE" = "worker" ]; then
    python -m app.worker
else
    uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}
fi
