#!/usr/bin/env bash
# install.sh — download the pre-built delta-server binary for the current platform.
#
# Usage:
#   ./install.sh                      # installs to ./delta-server
#   INSTALL_DIR=/usr/local/bin ./install.sh
#   VERSION=v0.2.0 ./install.sh
#
set -euo pipefail

REPO="ghazibendahmane/go-delta-rs"
INSTALL_DIR="${INSTALL_DIR:-.}"
VERSION="${VERSION:-latest}"

# ── Detect platform ──────────────────────────────────────────────────────────
OS="$(uname -s)"
ARCH="$(uname -m)"

case "$OS" in
  Linux)
    case "$ARCH" in
      x86_64)  ARTIFACT="delta-server-linux-amd64" ;;
      aarch64) ARTIFACT="delta-server-linux-arm64" ;;
      *)       echo "Unsupported architecture: $ARCH" >&2; exit 1 ;;
    esac
    ;;
  Darwin)
    case "$ARCH" in
      x86_64)  ARTIFACT="delta-server-darwin-amd64" ;;
      arm64)   ARTIFACT="delta-server-darwin-arm64" ;;
      *)       echo "Unsupported architecture: $ARCH" >&2; exit 1 ;;
    esac
    ;;
  MINGW*|MSYS*|CYGWIN*)
    ARTIFACT="delta-server-windows-amd64.exe"
    ;;
  *)
    echo "Unsupported OS: $OS" >&2
    exit 1
    ;;
esac

# ── Resolve version ──────────────────────────────────────────────────────────
if [ "$VERSION" = "latest" ]; then
  echo "Fetching latest release version..."
  VERSION=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" \
    | grep '"tag_name"' | sed -E 's/.*"([^"]+)".*/\1/')
fi

echo "Installing delta-server ${VERSION} (${ARTIFACT})..."

BASE_URL="https://github.com/${REPO}/releases/download/${VERSION}"
DEST="${INSTALL_DIR}/delta-server"

# ── Download binary ──────────────────────────────────────────────────────────
curl -fsSL "${BASE_URL}/${ARTIFACT}" -o "${DEST}"
chmod +x "${DEST}"

# ── Verify checksum ──────────────────────────────────────────────────────────
if command -v sha256sum &>/dev/null; then
  CHECKSUM_FILE=$(mktemp)
  curl -fsSL "${BASE_URL}/${ARTIFACT}.sha256" -o "$CHECKSUM_FILE"
  # sha256sum file format: "<hash>  <filename>" — replace filename with local path
  sed -i "s|${ARTIFACT}|${DEST}|g" "$CHECKSUM_FILE"
  sha256sum -c "$CHECKSUM_FILE" && echo "Checksum verified."
  rm "$CHECKSUM_FILE"
elif command -v shasum &>/dev/null; then
  CHECKSUM_FILE=$(mktemp)
  curl -fsSL "${BASE_URL}/${ARTIFACT}.sha256" -o "$CHECKSUM_FILE"
  EXPECTED=$(awk '{print $1}' "$CHECKSUM_FILE")
  ACTUAL=$(shasum -a 256 "${DEST}" | awk '{print $1}')
  if [ "$EXPECTED" != "$ACTUAL" ]; then
    echo "Checksum mismatch! Expected $EXPECTED, got $ACTUAL" >&2
    rm -f "${DEST}" "$CHECKSUM_FILE"
    exit 1
  fi
  echo "Checksum verified."
  rm "$CHECKSUM_FILE"
fi

echo ""
echo "Installed: ${DEST}"
echo ""
echo "Use it in Go:"
echo '  sidecar := deltago.NewSidecar(deltago.SidecarOptions{'
echo "    BinaryPath: \"${DEST}\","
echo '  })'
