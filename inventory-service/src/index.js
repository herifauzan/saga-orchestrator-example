
import { kafkaClient, jsonEvent } from "./kafka-util.js";
const kafka = kafkaClient("inventory");
const consumer = kafka.consumer({ groupId: "inventory-group" });
const producer = kafka.producer();
const stock = new Map([["sku-1", 10], ["sku-2", 0]]);
const reservations = new Map();

await consumer.connect();
await producer.connect();
await consumer.subscribe({ topic: "inventory-requests", fromBeginning: false });
consumer.run({
  eachMessage: async ({ message }) => {
    try {
      const evt = JSON.parse(message.value.toString());
      const orderId = evt.orderId;
      if (evt.type === "ReserveInventory") {
        const items = evt.payload.items || [];
        const ok = items.every(i => (stock.get(i.sku)||0) >= i.qty);
        if (!ok) {
          await producer.send({ topic: "inventory-responses", messages: [{ key: orderId, value: JSON.stringify(jsonEvent("InventoryFailed", orderId, { reason: "OUT_OF_STOCK" })) }] });
          return;
        }
        items.forEach(i => stock.set(i.sku, (stock.get(i.sku)||0) - i.qty));
        reservations.set(orderId, items);
        await producer.send({ topic: "inventory-responses", messages: [{ key: orderId, value: JSON.stringify(jsonEvent("InventoryReserved", orderId, { items })) }] });
      } else if (evt.type === "ReleaseInventory") {
        const items = reservations.get(orderId) || evt.payload.items || [];
        items.forEach(i => stock.set(i.sku, (stock.get(i.sku)||0) + i.qty));
        reservations.delete(orderId);
        await producer.send({ topic: "inventory-responses", messages: [{ key: orderId, value: JSON.stringify(jsonEvent("InventoryReleased", orderId, { items })) }] });
      }
    } catch(e){ console.error("inventory error", e); }
  }
});
console.log("inventory-service started");
