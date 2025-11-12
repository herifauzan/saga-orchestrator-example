
import { kafkaClient, jsonEvent } from "./kafka-util.js";
const kafka = kafkaClient("shipping");
const consumer = kafka.consumer({ groupId: "shipping-group" });
const producer = kafka.producer();
const shipments = new Map();

await consumer.connect();
await producer.connect();
await consumer.subscribe({ topic: "shipping-requests", fromBeginning: false });
consumer.run({
  eachMessage: async ({ message }) => {
    try {
      const evt = JSON.parse(message.value.toString());
      const orderId = evt.orderId;
      if (evt.type === "CreateShipment") {
        const tracking = "TRK-" + Math.random().toString(36).slice(2,9).toUpperCase();
        shipments.set(orderId, { tracking, items: evt.payload.items });
        await producer.send({ topic: "shipping-responses", messages: [{ key: orderId, value: JSON.stringify(jsonEvent("ShipmentCreated", orderId, { tracking })) }] });
      } else if (evt.type === "CancelShipment") {
        shipments.delete(orderId);
        await producer.send({ topic: "shipping-responses", messages: [{ key: orderId, value: JSON.stringify(jsonEvent("ShippingCancelled", orderId, {})) }] });
      }
    } catch(e){ console.error("shipping error", e); }
  }
});
console.log("shipping-service started");
