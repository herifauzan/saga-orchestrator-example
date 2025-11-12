
import { kafkaClient, jsonEvent } from "./kafka-util.js";
const kafka = kafkaClient("payment");
const consumer = kafka.consumer({ groupId: "payment-group" });
const producer = kafka.producer();
const charges = new Map();

await consumer.connect();
await producer.connect();
await consumer.subscribe({ topic: "payment-requests", fromBeginning: false });
consumer.run({
  eachMessage: async ({ message }) => {
    try {
      const evt = JSON.parse(message.value.toString());
      const orderId = evt.orderId;
      if (evt.type === "ChargePayment") {
        if ((orderId||"").includes("fail")) {
          await producer.send({ topic: "payment-responses", messages: [{ key: orderId, value: JSON.stringify(jsonEvent("PaymentFailed", orderId, { reason: "CARD_DECLINED" })) }] });
          return;
        }
        charges.set(orderId, evt.payload.amount);
        await producer.send({ topic: "payment-responses", messages: [{ key: orderId, value: JSON.stringify(jsonEvent("PaymentCompleted", orderId, { amount: evt.payload.amount })) }] });
      } else if (evt.type === "RefundPayment") {
        const charged = charges.get(orderId);
        if (charged) {
          charges.delete(orderId);
          await producer.send({ topic: "payment-responses", messages: [{ key: orderId, value: JSON.stringify(jsonEvent("PaymentRefunded", orderId, { amount: charged })) }] });
        } else {
          await producer.send({ topic: "payment-responses", messages: [{ key: orderId, value: JSON.stringify(jsonEvent("PaymentNotCharged", orderId, {})) }] });
        }
      }
    } catch(e){ console.error("payment error", e); }
  }
});
console.log("payment-service started");
