#!/bin/zsh
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:$PATH"

# Try to discover Node from common nvm installs when launched outside Terminal.
if ! command -v node >/dev/null 2>&1; then
  if [ -d "$HOME/.nvm/versions/node" ]; then
    LATEST_NVM_NODE_BIN="$(ls -1d "$HOME"/.nvm/versions/node/*/bin 2>/dev/null | sort -V | tail -n 1 || true)"
    if [ -n "${LATEST_NVM_NODE_BIN:-}" ] && [ -x "$LATEST_NVM_NODE_BIN/node" ]; then
      export PATH="$LATEST_NVM_NODE_BIN:$PATH"
    fi
  fi
fi

if ! command -v node >/dev/null 2>&1; then
  echo "__NODE_MISSING__"
  exit 0
fi

export FUND_NODE_BIN="$(command -v node)"

NEED_INSTALL=0
if [ ! -d "$PROJECT_DIR/node_modules" ]; then
  NEED_INSTALL=1
else
  INSTALLED_CHEERIO="$("$FUND_NODE_BIN" -e "try{const p=require(process.argv[1] + '/node_modules/cheerio/package.json');process.stdout.write(p.version||'')}catch(e){}" "$PROJECT_DIR" 2>/dev/null || true)"
  if [ "$INSTALLED_CHEERIO" != "1.0.0-rc.12" ]; then
    NEED_INSTALL=1
  fi
fi

if [ "$NEED_INSTALL" -eq 1 ]; then
  if ! npm --prefix "$PROJECT_DIR" install >/tmp/fund-analytics-install.log 2>&1; then
    echo "__NPM_INSTALL_FAILED__"
    exit 0
  fi
fi

# Launch desktop app in background, without opening Terminal window
nohup npm --prefix "$PROJECT_DIR" run app >/tmp/fund-analytics-app.log 2>&1 &

echo "__OK__"
