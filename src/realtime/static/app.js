const ws = new WebSocket(`ws://${window.location.host}/ws`);

const klineCtx = document.getElementById("klineChart").getContext("2d");
const volumeCtx = document.getElementById("volumeChart").getContext("2d");
const orderbookCtx = document.getElementById("orderbookChart").getContext("2d");
const exchangeSelect = document.getElementById("exchangeSelect");
const symbolSelect = document.getElementById("symbolSelect");
const timeframeSelect = document.getElementById("timeframeSelect");
const candleCountInput = document.getElementById("candleCount");

const state = {
  exchanges: new Map(),
  selectedExchange: null,
  selectedSymbol: null,
  latestTrades: new Map(),
  latestOrderbooks: new Map(),
  tradeBuffers: new Map(),
};

const klineChart = new Chart(klineCtx, {
  type: "candlestick",
  data: {
    datasets: [
      {
        label: "K线",
        data: [],
        color: {
          up: "#16a34a",
          down: "#ef4444",
          unchanged: "#94a3b8",
        },
      },
    ],
  },
  options: {
    animation: false,
    responsive: true,
    scales: {
      x: {
        display: true,
        ticks: { color: "#9ca3af" },
        grid: { color: "rgba(148, 163, 184, 0.1)" },
      },
      y: {
        ticks: { color: "#9ca3af" },
        grid: { color: "rgba(148, 163, 184, 0.1)" },
      },
    },
  },
});

const volumeChart = new Chart(volumeCtx, {
  type: "bar",
  data: {
    labels: [],
    datasets: [
      {
        label: "成交量",
        data: [],
        backgroundColor: "rgba(59, 130, 246, 0.6)",
      },
    ],
  },
  options: {
    animation: false,
    responsive: true,
    scales: {
      x: {
        ticks: { color: "#9ca3af" },
        grid: { display: false },
      },
      y: {
        ticks: { color: "#9ca3af" },
        grid: { color: "rgba(148, 163, 184, 0.1)" },
      },
    },
  },
});
volumeChart.data.labels = [];
volumeChart.data.datasets[0].data = [];

const orderbookChart = new Chart(orderbookCtx, {
  type: "line",
  data: {
    datasets: [
      {
        label: "Bid Depth",
        data: [],
        borderColor: "#22c55e",
        backgroundColor: "rgba(34, 197, 94, 0.15)",
        pointRadius: 0,
        fill: true,
      },
      {
        label: "Ask Depth",
        data: [],
        borderColor: "#ef4444",
        backgroundColor: "rgba(239, 68, 68, 0.15)",
        pointRadius: 0,
        fill: true,
      },
    ],
  },
  options: {
    animation: false,
    responsive: true,
    scales: {
      x: {
        type: "linear",
        ticks: { color: "#9ca3af" },
        grid: { color: "rgba(148, 163, 184, 0.1)" },
      },
      y: {
        ticks: { color: "#9ca3af" },
        grid: { color: "rgba(148, 163, 184, 0.1)" },
      },
    },
  },
});

const latestTrade = document.getElementById("latestTrade");
const latestOrderbook = document.getElementById("latestOrderbook");
const maxTradePoints = 100;

function keyFor(exchange, symbol) {
  return `${exchange}::${symbol}`;
}

function updateExchangeOptions(exchange) {
  if (!state.exchanges.has(exchange)) {
    state.exchanges.set(exchange, new Set());
    const option = document.createElement("option");
    option.value = exchange;
    option.textContent = exchange;
    exchangeSelect.appendChild(option);
  }
}

function updateSymbolOptions(exchange) {
  symbolSelect.innerHTML = "";
  const symbols = state.exchanges.get(exchange);
  if (!symbols) {
    return;
  }
  for (const symbol of symbols) {
    const option = document.createElement("option");
    option.value = symbol;
    option.textContent = symbol;
    symbolSelect.appendChild(option);
  }
  if (!state.selectedSymbol && symbols.size > 0) {
    state.selectedSymbol = [...symbols][0];
  }
  symbolSelect.value = state.selectedSymbol;
}

function setSelection(exchange, symbol) {
  state.selectedExchange = exchange;
  state.selectedSymbol = symbol;
  exchangeSelect.value = exchange;
  updateSymbolOptions(exchange);
  renderSelected();
}

function renderSelected() {
  if (!state.selectedExchange || !state.selectedSymbol) {
    return;
  }
  const key = keyFor(state.selectedExchange, state.selectedSymbol);
  const trades = state.tradeBuffers.get(key) || [];
  updateKline(trades);
  const tradePayload = state.latestTrades.get(key);
  if (tradePayload) {
    latestTrade.textContent = `${tradePayload.exchange_id} ${tradePayload.symbol} ${tradePayload.side} @ ${tradePayload.price} (${tradePayload.amount})`;
  }
  const orderbookPayload = state.latestOrderbooks.get(key);
  if (orderbookPayload) {
    updateOrderbookChart(orderbookPayload, false);
  }
}

function updateKline(trades) {
  const timeframe = Number(timeframeSelect.value || 60) * 1000;
  const candleCount = Number(candleCountInput.value || 80);
  if (!trades.length) {
    klineChart.data.labels = [];
    klineChart.data.datasets[0].data = [];
    volumeChart.data.labels = [];
    volumeChart.data.datasets[0].data = [];
    klineChart.update();
    volumeChart.update();
    return;
  }
  const sortedTrades = [...trades].sort((a, b) => a.timestamp - b.timestamp);
  const buckets = new Map();
  sortedTrades.forEach((trade) => {
    const bucket = Math.floor(trade.timestamp / timeframe) * timeframe;
    if (!buckets.has(bucket)) {
      buckets.set(bucket, {
        timestamp: bucket,
        open: trade.price,
        high: trade.price,
        low: trade.price,
        close: trade.price,
        volume: trade.amount || 0,
      });
    } else {
      const candle = buckets.get(bucket);
      candle.high = Math.max(candle.high, trade.price);
      candle.low = Math.min(candle.low, trade.price);
      candle.close = trade.price;
      candle.volume += trade.amount || 0;
    }
  });
  const sorted = Array.from(buckets.values()).sort(
    (a, b) => a.timestamp - b.timestamp
  );
  const sliced = sorted.slice(-candleCount);
  klineChart.data.labels = sliced.map((c) =>
    new Date(c.timestamp).toLocaleTimeString()
  );
  klineChart.data.datasets[0].data = sliced.map((c) => ({
    x: new Date(c.timestamp).toLocaleTimeString(),
    o: c.open,
    h: c.high,
    l: c.low,
    c: c.close,
  }));
  klineChart.update();

  volumeChart.data.labels = sliced.map((c) =>
    new Date(c.timestamp).toLocaleTimeString()
  );
  volumeChart.data.datasets[0].data = sliced.map((c) => c.volume);
  volumeChart.update();
}

function updateOrderbookChart(payload, updateLatest = true) {
  const bids = payload.bids.slice(0, 20);
  const asks = payload.asks.slice(0, 20);
  let bidCumulative = 0;
  let askCumulative = 0;
  const bidDepth = bids
    .sort((a, b) => b[0] - a[0])
    .map(([price, size]) => {
      bidCumulative += size;
      return { x: price, y: bidCumulative };
    })
    .reverse();
  const askDepth = asks
    .sort((a, b) => a[0] - b[0])
    .map(([price, size]) => {
      askCumulative += size;
      return { x: price, y: askCumulative };
    });
  orderbookChart.data.datasets[0].data = bidDepth;
  orderbookChart.data.datasets[1].data = askDepth;
  orderbookChart.update();
  if (updateLatest) {
    latestOrderbook.textContent = `${payload.exchange_id} ${payload.symbol || ""} Bids:${bids.length} Asks:${asks.length}`;
  }
}

ws.onmessage = (event) => {
  const message = JSON.parse(event.data);
  if (message.type === "config") {
    for (const [exchange, symbols] of Object.entries(message.exchanges)) {
      updateExchangeOptions(exchange);
      const symbolSet = state.exchanges.get(exchange);
      symbols.forEach((symbol) => symbolSet.add(symbol));
    }
    if (!state.selectedExchange) {
      const firstExchange = exchangeSelect.options[0]?.value;
      const firstSymbol = state.exchanges.get(firstExchange)?.values().next()
        .value;
      if (firstExchange && firstSymbol) {
        setSelection(firstExchange, firstSymbol);
      }
    } else {
      updateSymbolOptions(state.selectedExchange);
    }
    return;
  }
  if (message.type === "trade") {
    const { exchange_id, symbol, payload } = message;
    updateExchangeOptions(exchange_id);
    const symbols = state.exchanges.get(exchange_id);
    symbols.add(symbol);
    if (!state.selectedExchange) {
      setSelection(exchange_id, symbol);
    }
    const key = keyFor(exchange_id, symbol);
    const enriched = { ...payload, exchange_id };
    if (!state.tradeBuffers.has(key)) {
      state.tradeBuffers.set(key, []);
    }
    const buffer = state.tradeBuffers.get(key);
    buffer.push(enriched);
    if (buffer.length > maxTradePoints) {
      buffer.shift();
    }
    state.latestTrades.set(key, enriched);
    latestTrade.textContent = `${enriched.exchange_id} ${enriched.symbol} ${enriched.side} @ ${enriched.price} (${enriched.amount})`;
    if (
      exchange_id === state.selectedExchange &&
      symbol === state.selectedSymbol
    ) {
      updateKline(buffer);
    }
  }
  if (message.type === "orderbook") {
    const { exchange_id, symbol, payload } = message;
    updateExchangeOptions(exchange_id);
    const symbols = state.exchanges.get(exchange_id);
    symbols.add(symbol);
    if (!state.selectedExchange) {
      setSelection(exchange_id, symbol);
    }
    const key = keyFor(exchange_id, symbol);
    const enriched = { ...payload, exchange_id, symbol };
    state.latestOrderbooks.set(key, enriched);
    if (
      exchange_id === state.selectedExchange &&
      symbol === state.selectedSymbol
    ) {
      updateOrderbookChart(enriched);
    }
  }
};

ws.onopen = () => {
  ws.send("hello");
};

exchangeSelect.addEventListener("change", (event) => {
  const exchange = event.target.value;
  state.selectedExchange = exchange;
  state.selectedSymbol = null;
  updateSymbolOptions(exchange);
  renderSelected();
});

symbolSelect.addEventListener("change", (event) => {
  state.selectedSymbol = event.target.value;
  renderSelected();
});

timeframeSelect.addEventListener("change", () => {
  renderSelected();
});

candleCountInput.addEventListener("input", () => {
  renderSelected();
});
