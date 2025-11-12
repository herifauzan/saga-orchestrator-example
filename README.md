## 🧩 Microservices Architecture — Kafka Saga Orchestrator Pattern
## Overview

This project demonstrates a microservices-based architecture implementing the Saga pattern (Orchestrator variant) using Kafka as the event broker.
The goal is to ensure distributed transaction consistency across multiple services (Order, Inventory, Payment, Shipping) in a resilient, event-driven manner.

```text
+---------------+       +----------------+       +----------------+       +----------------+
|  Order Svc    |       |  Inventory Svc |       |  Payment Svc   |       |  Shipping Svc  |
| (HTTP + Kafka)|       | (Kafka)        |       | (Kafka)        |       | (Kafka)        |
+-------+-------+       +-------+--------+       +-------+--------+       +-------+--------+
        |                       |                        |                        |
        |  REST /orders         |                        |                        |
        |---------------------->|                        |                        |
        |     start saga via    |                        |                        |
        |     Orchestrator      |                        |                        |
        |                       |                        |                        |
        |                       |<------------------------|                        |
        |                       |  InventoryReserved      |                        |
        |                       |------------------------>|                        |
        |                       |                        | PaymentCompleted        |
        |                       |                        |----------------------->|
        |                       |                        |                        | ShippingScheduled
        |<------------------------------------------------<-----------------------|
        | OrderCompleted (SUCCESS)                                               |
        +------------------------------------------------------------------------+
```
