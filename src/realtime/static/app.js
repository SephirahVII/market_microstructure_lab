const ws = new WebSocket(`ws://${window.location.host}/ws`);

const klineCtx = document.getElementById("klineChart").getContext("2d");
const volumeCtx = document.getElementById("volumeChart").getContext("2d");
const orderbookCtx = document.getElementById("orderbookChart").getContext("2d");
const exchangeSelect = document.getElementById("exchangeSelect");
const symbolSelect = document.getElementById("symbolSelect");
const timeframeSelect = document.getElementById("timeframeSelect");
const candleCountInput = document.getElementById("candleCount");
const candleCountValue = document.getElementById("candleCountValue");

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
        type: "timeseries",
        time: { unit: "minute" },
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
  type: "bar",
  data: {
    labels: [],
    datasets: [
      {
        label: "Bid Qty",
        data: [],
        borderColor: "#22c55e",
        backgroundColor: "rgba(34, 197, 94, 0.5)",
        borderWidth: 1,
      },
      {
        label: "Ask Qty",
        data: [],
        borderColor: "#ef4444",
        backgroundColor: "rgba(239, 68, 68, 0.5)",
        borderWidth: 1,
      },
    ],
  },
  options: {
    animation: false,
    responsive: true,
    indexAxis: "y",
    scales: {
      x: {
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
      const tradePrice = Number(trade.price);
      const tradeAmount = Number(trade.amount || 0);
      buckets.set(bucket, {
        timestamp: bucket,
        open: tradePrice,
        high: tradePrice,
        low: tradePrice,
        close: tradePrice,
        volume: tradeAmount,
      });
    } else {
      const candle = buckets.get(bucket);
      const tradePrice = Number(trade.price);
      const tradeAmount = Number(trade.amount || 0);
      candle.high = Math.max(candle.high, tradePrice);
      candle.low = Math.min(candle.low, tradePrice);
      candle.close = tradePrice;
      candle.volume += tradeAmount;
    }
  });
  const sorted = Array.from(buckets.values()).sort(
    (a, b) => a.timestamp - b.timestamp
  );
  const sliced = sorted.slice(-candleCount);
  klineChart.data.labels = sliced.map((c) => new Date(c.timestamp));
  klineChart.data.datasets[0].data = sliced.map((c) => ({
    x: new Date(c.timestamp),
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
  const bids = payload.bids.slice(0, 15).map(([p, q]) => [Number(p), Number(q)]);
  const asks = payload.asks.slice(0, 15).map(([p, q]) => [Number(p), Number(q)]);

  const labels = Array.from(
    new Set([...bids.map(([p]) => p), ...asks.map(([p]) => p)])
  ).sort((a, b) => b - a);

  const bidMap = new Map(bids);
  const askMap = new Map(asks);

  orderbookChart.data.labels = labels.map((p) => p.toFixed(2));
  orderbookChart.data.datasets[0].data = labels.map((p) => bidMap.get(p) || 0);
  orderbookChart.data.datasets[1].data = labels.map((p) => askMap.get(p) || 0);
  orderbookChart.update();
  if (updateLatest) {
    const topBid = bids[0] ? `${bids[0][0]} 买量 ${bids[0][1]}` : "-";
    const topAsk = asks[0] ? `${asks[0][0]} 卖量 ${asks[0][1]}` : "-";
    latestOrderbook.textContent = `${payload.exchange_id} ${payload.symbol || ""} | ${topBid} | ${topAsk}`;
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
    const enriched = {
      ...payload,
      exchange_id,
      price: Number(payload.price),
      amount: Number(payload.amount),
      timestamp: Number(payload.timestamp),
    };
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
  candleCountValue.textContent = candleCountInput.value;
  renderSelected();
});

candleCountValue.textContent = candleCountInput.value;
