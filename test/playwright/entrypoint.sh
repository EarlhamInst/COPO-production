#!/bin/bash
set -e

timeout=60
elapsed=0

# Start virtual display stack
Xvfb :99 -screen 0 1920x1080x24 &
export DISPLAY=:99

fluxbox &
x11vnc -display :99 -forever -shared -nopw -rfbport 5900 &

# Wait for display to be ready
until xdpyinfo -display :99 >/dev/null 2>&1; do
  sleep 0.2
done

# Wait for web service to start up. No dedicated health endpoint exists;
# match the plain root-URL check the copo_web service's own healthcheck uses.
until curl -f "${BASE_URL}/"; do
  echo "Waiting for copo_web service to start..."
  sleep 2
  elapsed=$((elapsed+2))

  if [ "$elapsed" -ge "$timeout" ]; then
    echo "copo_web service not ready...exiting"
    exit 1
  fi
done

# Run command e.g. copo test playwright debug
exec "$@"