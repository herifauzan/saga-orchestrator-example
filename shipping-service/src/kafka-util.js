
import { Kafka } from "kafkajs";

export function kafkaClient(clientId = undefined){
  const brokers = (process.env.KAFKA_BROKERS || "kafka:9092").split(",");
  return new Kafka({ clientId: clientId || ("svc-"+Math.random().toString(16).slice(2)), brokers });
}

export function jsonEvent(type, orderId, payload={}){
  return { type, orderId, payload, timestamp: new Date().toISOString() };
}
