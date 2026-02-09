const ws = new WebSocket(`ws://${window.location.host}/ws`);

const tradeCtx = document.getElementById("tradeChart").getContext("2d");
const orderbookCtx = document.getElementById("orderbookChart").getContext("2d");
const exchangeSelect = document.getElementById("exchangeSelect");
const symbolSelect = document.getElementById("symbolSelect");

const state = {
  exchanges: new Map(),
  selectedExchange: null,
  selectedSymbol: null,
  latestTrades: new Map(),
  latestOrderbooks: new Map(),
};

const tradeChart = new Chart(tradeCtx, {
  type: "line",
  data: {
    labels: [],
    datasets: [
      {
        label: "Trade Price",
        data: [],
        borderColor: "#2a9d8f",
        backgroundColor: "rgba(42, 157, 143, 0.15)",
        tension: 0.2,
      },
    ],
  },
  options: {
    animation: false,
    responsive: true,
    scales: {
      x: { display: false },
    },
  },
});

const orderbookChart = new Chart(orderbookCtx, {
  type: "bar",
  data: {
    labels: [],
    datasets: [
      {
        label: "Bid Depth",
        data: [],
        backgroundColor: "rgba(42, 157, 143, 0.5)",
      },
      {
        label: "Ask Depth",
        data: [],
        backgroundColor: "rgba(231, 111, 81, 0.5)",
      },
    ],
  },
  options: {
    animation: false,
    responsive: true,
    scales: {
      x: { stacked: true },
      y: { stacked: true },
    },
  },
});
orderbookChart.data.labels = [];
orderbookChart.data.datasets[0].data = [];
orderbookChart.data.datasets[1].data = [];

const latestTrade = document.getElementById("latestTrade");
const latestOrderbook = document.getElementById("latestOrderbook");

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
  tradeChart.data.labels = [];
  tradeChart.data.datasets[0].data = [];
  const tradePayload = state.latestTrades.get(key);
  if (tradePayload) {
    updateTradeChart(tradePayload, true);
  }
  const orderbookPayload = state.latestOrderbooks.get(key);
  if (orderbookPayload) {
    updateOrderbookChart(orderbookPayload, false);
  }
}

function updateTradeChart(payload, append = true) {
  if (append) {
    const timeLabel = new Date(payload.timestamp).toLocaleTimeString();
    tradeChart.data.labels.push(timeLabel);
    tradeChart.data.datasets[0].data.push(payload.price);
    if (tradeChart.data.labels.length > 50) {
      tradeChart.data.labels.shift();
      tradeChart.data.datasets[0].data.shift();
    }
  }
  tradeChart.update();
  latestTrade.textContent = `${payload.exchange_id} ${payload.symbol} ${payload.side} @ ${payload.price} (${payload.amount})`;
}

function updateOrderbookChart(payload, updateLatest = true) {
  const bids = payload.bids.slice(0, 10);
  const asks = payload.asks.slice(0, 10);
  const bidLabels = bids.map((item) => item[0]);
  const askLabels = asks.map((item) => item[0]);
  const labels = [...bidLabels, ...askLabels];
  orderbookChart.data.labels = labels;
  orderbookChart.data.datasets[0].data = bids.map((item) => item[1]);
  orderbookChart.data.datasets[1].data = asks.map((item) => item[1]);
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
    state.latestTrades.set(key, enriched);
    if (
      exchange_id === state.selectedExchange &&
      symbol === state.selectedSymbol
    ) {
      updateTradeChart(enriched);
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
