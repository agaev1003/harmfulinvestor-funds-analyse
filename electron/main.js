const { app, BrowserWindow, dialog } = require("electron");
const http = require("http");
const net = require("net");
const path = require("path");
const { spawn } = require("child_process");

const DEFAULT_PORT = Number(process.env.PORT || 0);
const NODE_BIN = process.env.FUND_NODE_BIN || "node";
const ROOT_DIR = path.join(__dirname, "..");
const HIDE_DOCK_ICON =
  process.platform === "darwin" && String(process.env.HIDE_DOCK_ICON || "1") !== "0";

let serverProcess = null;
let serverPort = DEFAULT_PORT;

function getServerUrl() {
  return `http://127.0.0.1:${serverPort}`;
}

function resolveServerPort() {
  if (Number.isFinite(serverPort) && serverPort > 0) return Promise.resolve();

  return new Promise((resolve, reject) => {
    const probe = net.createServer();
    probe.unref();
    probe.on("error", reject);
    probe.listen(0, "127.0.0.1", () => {
      const address = probe.address();
      if (!address || typeof address === "string") {
        probe.close(() => reject(new Error("Не удалось выбрать свободный порт")));
        return;
      }
      serverPort = address.port;
      probe.close(resolve);
    });
  });
}

async function startLocalServer() {
  if (serverProcess) return;
  await resolveServerPort();
  const serverPath = path.join(ROOT_DIR, "server.js");
  await new Promise((resolve, reject) => {
    const child = spawn(NODE_BIN, [serverPath], {
      cwd: ROOT_DIR,
      env: {
        ...process.env,
        PORT: String(serverPort),
      },
      stdio: ["ignore", "pipe", "pipe"],
    });

    let settled = false;
    child.once("spawn", () => {
      settled = true;
      serverProcess = child;
      resolve();
    });
    child.once("error", (error) => {
      if (!settled) {
        settled = true;
        reject(error);
      } else {
        process.stderr.write(
          `[server] process error: ${String(error.message || error)}\n`
        );
      }
    });

    child.stdout.on("data", (chunk) => {
      process.stdout.write(`[server] ${chunk}`);
    });

    child.stderr.on("data", (chunk) => {
      process.stderr.write(`[server] ${chunk}`);
    });

    child.on("exit", (code, signal) => {
      if (serverProcess) {
        process.stderr.write(
          `[server] exited (code=${code ?? "null"}, signal=${signal ?? "null"})\n`
        );
      }
      serverProcess = null;
    });
  });
}

function stopLocalServer() {
  if (!serverProcess) return;
  serverProcess.kill("SIGTERM");
  serverProcess = null;
}

function pingServer() {
  return new Promise((resolve) => {
    const req = http.get(getServerUrl(), (res) => {
      res.resume();
      resolve(res.statusCode && res.statusCode < 500);
    });
    req.on("error", () => resolve(false));
    req.setTimeout(1000, () => {
      req.destroy();
      resolve(false);
    });
  });
}

async function waitForServer(timeoutMs = 20000) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    // eslint-disable-next-line no-await-in-loop
    const ok = await pingServer();
    if (ok) return true;
    // eslint-disable-next-line no-await-in-loop
    await new Promise((r) => setTimeout(r, 350));
  }
  return false;
}

function createWindow() {
  const win = new BrowserWindow({
    width: 1600,
    height: 980,
    minWidth: 1200,
    minHeight: 760,
    title: "Fund Dashboard",
    autoHideMenuBar: true,
    skipTaskbar: HIDE_DOCK_ICON,
    backgroundColor: "#1a1a2e",
    webPreferences: {
      contextIsolation: true,
      sandbox: true,
    },
  });

  win.loadURL(getServerUrl());
}

app.whenReady().then(async () => {
  if (HIDE_DOCK_ICON && app.dock && typeof app.dock.hide === "function") {
    app.dock.hide();
  }

  await startLocalServer();
  const ok = await waitForServer();

  if (!ok) {
    dialog.showErrorBox(
      "Ошибка запуска",
      "Не удалось поднять локальный сервер. Проверьте Node.js и повторите."
    );
    stopLocalServer();
    app.quit();
    return;
  }

  createWindow();

  app.on("activate", () => {
    if (BrowserWindow.getAllWindows().length === 0) createWindow();
  });
});

app.on("before-quit", () => {
  stopLocalServer();
});

app.on("window-all-closed", () => {
  if (process.platform !== "darwin" || HIDE_DOCK_ICON) app.quit();
});
