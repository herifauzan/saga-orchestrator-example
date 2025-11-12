
import express from "express";
import bodyParser from "body-parser";
import fetch from "node-fetch";

const app = express();
app.use(bodyParser.json());

const ORCH_URL = process.env.ORCH_URL || "http://orchestrator-service:4000";
const orders = new Map();

app.post("/orders", async (req, res) => {
  const { orderId, userId, items, amount } = req.body || {};
  if (!orderId) return res.status(400).json({ error: "orderId required" });
  const order = { orderId, userId, items, amount, status: "PENDING", history: [] };
  orders.set(orderId, order);
  try {
    const r = await fetch(`${ORCH_URL}/sagas/start`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ orderId, userId, items, amount })
    });
    const j = await r.json();
    order.status = j.status || order.status;
    order.history.push(j);
    res.json({ ok: true, orderId, orchestrator: j });
  } catch (e) {
    res.status(500).json({ error: "orchestrator unreachable", detail: e.message });
  }
});

app.get("/orders/:id", (req,res)=>{
  const o = orders.get(req.params.id);
  if (!o) return res.status(404).json({ error: "not found" });
  res.json(o);
});

const port = process.env.PORT || 3000;
app.listen(port, ()=>console.log(`order-service listening ${port}`));
