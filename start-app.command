#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")"

if ! command -v node >/dev/null 2>&1; then
  osascript -e 'display alert "Node.js не найден" message "Установите Node.js LTS с сайта nodejs.org, затем запустите снова." as critical'
  exit 1
fi

if [ ! -d "node_modules" ]; then
  npm install
fi

npm run app

