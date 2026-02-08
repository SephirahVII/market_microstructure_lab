const ws = new WebSocket(`ws://${window.location.host}/ws`);

const tradeCtx = document.getElementById("tradeChart").getContext("2d");
const orderbookCtx = document.getElementById("orderbookChart").getContext("2d");

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

const latestTrade = document.getElementById("latestTrade");
const latestOrderbook = document.getElementById("latestOrderbook");

function updateTradeChart(payload) {
  const timeLabel = new Date(payload.timestamp).toLocaleTimeString();
  tradeChart.data.labels.push(timeLabel);
  tradeChart.data.datasets[0].data.push(payload.price);
  if (tradeChart.data.labels.length > 50) {
    tradeChart.data.labels.shift();
    tradeChart.data.datasets[0].data.shift();
  }
  tradeChart.update();
  latestTrade.textContent = `${payload.symbol} ${payload.side} @ ${payload.price} (${payload.amount})`;
}

function updateOrderbookChart(payload) {
  const bids = payload.bids.slice(0, 10);
  const asks = payload.asks.slice(0, 10);
  const bidLabels = bids.map((item) => item[0]);
  const askLabels = asks.map((item) => item[0]);
  const labels = [...bidLabels, ...askLabels];
  orderbookChart.data.labels = labels;
  orderbookChart.data.datasets[0].data = bids.map((item) => item[1]);
  orderbookChart.data.datasets[1].data = asks.map((item) => item[1]);
  orderbookChart.update();
  latestOrderbook.textContent = `${payload.symbol || ""} Bids:${bids.length} Asks:${asks.length}`;
}

ws.onmessage = (event) => {
  const message = JSON.parse(event.data);
  if (message.type === "trade") {
    updateTradeChart(message.payload);
  }
  if (message.type === "orderbook") {
    updateOrderbookChart(message.payload);
  }
};

ws.onopen = () => {
  ws.send("hello");
};
