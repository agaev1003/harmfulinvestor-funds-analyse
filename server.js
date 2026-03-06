const express = require("express");
const path = require("path");
const provider = require("./data-provider");

const app = express();
const PORT = Number(process.env.PORT || 3000);

function parseFromTo(query) {
  const normalizeIsoDate = (value) => {
    if (!value) return null;
    const s = String(value).trim();
    return /^\d{4}-\d{2}-\d{2}$/.test(s) ? s : null;
  };
  let from = normalizeIsoDate(query.from);
  let to = normalizeIsoDate(query.to);
  if (from && to && from > to) {
    const tmp = from;
    from = to;
    to = tmp;
  }
  return { from, to };
}

app.get("/api/funds", async (req, res) => {
  try {
    const data = await provider.getFunds();
    res.json(data);
  } catch (error) {
    res.status(500).json({
      error: "Не удалось сформировать список фондов",
      details: String(error.message || error),
    });
  }
});

app.get("/api/healthz", (req, res) => {
  res.json({
    ok: true,
    ts: new Date().toISOString(),
  });
});

app.get("/api/returns", async (req, res) => {
  try {
    const data = await provider.getReturns();
    res.json(data);
  } catch (error) {
    res.status(500).json({
      error: "Не удалось сформировать доходности фондов",
      details: String(error.message || error),
    });
  }
});

app.get("/api/compositions", async (req, res) => {
  try {
    const forceRefresh =
      req.query.refresh != null &&
      String(req.query.refresh).toLowerCase() !== "0" &&
      String(req.query.refresh).toLowerCase() !== "false";
    const data = await provider.getFundCompositions(forceRefresh);
    res.json(data);
  } catch (error) {
    res.status(500).json({
      error: "Не удалось сформировать составы фондов",
      details: String(error.message || error),
    });
  }
});

app.get("/api/market", async (req, res) => {
  try {
    const fundType = req.query.fund_type ? String(req.query.fund_type) : "all";
    const data = await provider.getMarketData(fundType);
    res.json(data);
  } catch (error) {
    res.status(500).json({
      error: "Не удалось сформировать данные вкладки Рынок",
      details: String(error.message || error),
    });
  }
});

app.get("/api/mc-detail", async (req, res) => {
  try {
    const mc = req.query.mc ? String(req.query.mc) : "";
    const fundType = req.query.fund_type ? String(req.query.fund_type) : "all";
    const data = await provider.getMCDetail(mc, fundType);
    res.json(data);
  } catch (error) {
    res.status(500).json({
      error: "Не удалось сформировать данные вкладки УК",
      details: String(error.message || error),
    });
  }
});

app.get("/api/nav/:id", async (req, res) => {
  try {
    const fundId = Number(req.params.id);
    if (!Number.isFinite(fundId)) {
      res.status(400).json({ error: "Некорректный id фонда" });
      return;
    }

    const { from, to } = parseFromTo(req.query);
    const data = await provider.getNavByFundId(fundId, from, to);

    if (!data) {
      res.status(404).json({ error: "Фонд не найден" });
      return;
    }

    res.json({ data });
  } catch (error) {
    res.status(500).json({
      error: "Не удалось загрузить историю фонда",
      details: String(error.message || error),
    });
  }
});

app.get("/api/status", async (req, res) => {
  try {
    const ds = await provider.getDataset();
    res.json({
      ok: true,
      generatedAt: ds.generatedAt,
      funds: ds.funds.length,
      source: ds.source,
    });
  } catch (error) {
    res.status(500).json({
      ok: false,
      error: String(error.message || error),
    });
  }
});

app.use(express.static(path.join(__dirname, "public")));

app.get("*", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

function startServer(port = PORT) {
  const server = app.listen(port, () => {
    console.log(`Fund dashboard (independent): http://localhost:${port}`);
  });
  return server;
}

if (require.main === module) {
  startServer();
}

module.exports = {
  app,
  startServer,
};
