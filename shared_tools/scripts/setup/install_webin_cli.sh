#!/usr/bin/env bash
#
# Download The European Nucleotide Archive's (ENA's) Webin-CLI into the 
# project folder for local Docker containers
# (i.e. `local.copo.compose.mac.yaml` and `local.copo.compose.linux.yaml`).
#
# The local Docker compose files mount the project over `/copo` but they hide the `jar` file
# that the Dockerfile placed there and `.gitignore` ignores the file, as a result,
# the webin-cli `jar` file will be fetched using this script
#
# NB: If upgrading the ENA Webin CLI version, update the version in the 
# following places as well:
# `WEBIN_CLI_VERSION` (in this `install_webin_cli.sh` script)
# deployment/web/Dockerfile
# deployment/web/Dockerfile_circleci
# deployment/web/Dockerfile_local_linux
# deployment/web/Dockerfile_local_mac
#
# Reference: ENA Webin-CLI GitHub releases: https://github.com/enasequence/webin-cli/releases
#
set -euo pipefail

WEBIN_CLI_VERSION=11.0.0 # Replace this with the desired ENA Webin-CLI version if needed
WEBIN_CLI_DIR=/copo/tools/reposit/ena_cli
VERSION_FILE="${WEBIN_CLI_DIR}/VERSION"

if [ "$(cat "${VERSION_FILE}" 2>/dev/null)" = "${WEBIN_CLI_VERSION}" ] && [ -f "${WEBIN_CLI_DIR}/webin-cli.jar" ]; then
  exit 0
fi

mkdir -p "${WEBIN_CLI_DIR}"
curl -fsSL "https://github.com/enasequence/webin-cli/releases/download/${WEBIN_CLI_VERSION}/webin-cli-${WEBIN_CLI_VERSION}.jar" \
     -o "${WEBIN_CLI_DIR}/webin-cli.jar.tmp"
mv "${WEBIN_CLI_DIR}/webin-cli.jar.tmp" "${WEBIN_CLI_DIR}/webin-cli.jar"
echo "${WEBIN_CLI_VERSION}" > "${VERSION_FILE}"
echo "Installed ENA Webin-CLI ${WEBIN_CLI_VERSION}"
