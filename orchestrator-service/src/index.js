
import express from "express";
import bodyParser from "body-parser";
import { kafkaClient, jsonEvent } from "./kafka-util.js";
import { v4 as uuidv4 } from "uuid";

const app = express();
app.use(bodyParser.json());

const kafka = kafkaClient("orchestrator");
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: "orchestrator-group" });

const TIMEOUT = 15000;
const waiters = new Map();

async function startKafka(){
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: "inventory-responses", fromBeginning: false });
  await consumer.subscribe({ topic: "payment-responses", fromBeginning: false });
  await consumer.subscribe({ topic: "shipping-responses", fromBeginning: false });

  consumer.run({
    eachMessage: async ({ topic, message }) => {
      try {
        const evt = JSON.parse(message.value.toString());
        const key = evt.orderId;
        if (!waiters.has(key)) waiters.set(key, []);
        waiters.get(key).push(evt);
      } catch (e) { console.error("orchestrator consumer error", e); }
    }
  });
}

function waitForEvent(orderId, predicate, timeout=TIMEOUT){
  return new Promise((resolve, reject)=>{
    const deadline = Date.now() + timeout;
    const check = () => {
      const arr = waiters.get(orderId) || [];
      for (let i=0;i<arr.length;i++){
        const e = arr[i];
        if (predicate(e)){
          arr.splice(i,1);
          return resolve(e);
        }
      }
      if (Date.now() > deadline) return reject(new Error("timeout"));
      setTimeout(check, 200);
    };
    check();
  });
}

await startKafka();

app.post("/sagas/start", async (req, res) => {
  const { orderId, userId, items, amount } = req.body || {};
  if (!orderId) return res.status(400).json({ error: "orderId required" });
  const sagaId = uuidv4();
  try {
    // Reserve inventory
    await producer.send({ topic: "inventory-requests", messages: [{ key: orderId, value: JSON.stringify(jsonEvent("ReserveInventory", orderId, { items, sagaId })) }] });
    const invResp = await waitForEvent(orderId, e => e.type === "InventoryReserved" || e.type === "InventoryFailed");
    if (invResp.type === "InventoryFailed") throw new Error("inventory_failed");
    // Charge payment
    await producer.send({ topic: "payment-requests", messages: [{ key: orderId, value: JSON.stringify(jsonEvent("ChargePayment", orderId, { amount, sagaId })) }] });
    const payResp = await waitForEvent(orderId, e => e.type === "PaymentCompleted" || e.type === "PaymentFailed");
    if (payResp.type === "PaymentFailed") throw new Error("payment_failed");
    // Create shipment
    await producer.send({ topic: "shipping-requests", messages: [{ key: orderId, value: JSON.stringify(jsonEvent("CreateShipment", orderId, { items, sagaId })) }] });
    const shipResp = await waitForEvent(orderId, e => e.type === "ShipmentCreated" || e.type === "ShippingFailed");
    if (shipResp.type === "ShippingFailed") throw new Error("shipping_failed");
    // success: emit OrderCompleted
    await producer.send({ topic: "orders", messages: [{ key: orderId, value: JSON.stringify(jsonEvent("OrderCompleted", orderId, { sagaId })) }] });
    return res.json({ status: "COMPLETED", sagaId });
  } catch (err) {
    // compensate via events
    try {
      await producer.send({ topic: "payment-requests", messages: [{ key: orderId, value: JSON.stringify(jsonEvent("RefundPayment", orderId, { sagaId })) }] });
      await producer.send({ topic: "inventory-requests", messages: [{ key: orderId, value: JSON.stringify(jsonEvent("ReleaseInventory", orderId, { sagaId })) }] });
      await producer.send({ topic: "shipping-requests", messages: [{ key: orderId, value: JSON.stringify(jsonEvent("CancelShipment", orderId, { sagaId })) }] });
    } catch(e) { console.error("compensation publish error", e); }
    return res.status(500).json({ status: "COMPENSATED", reason: err.message });
  }
});

app.get("/health", (req,res)=>res.json({ ok: true }));

const port = process.env.PORT || 4000;
app.listen(port, ()=>console.log(`orchestrator listening ${port}`));
