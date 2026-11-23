## Kafka-style bus and zero-bus: comparison for the Olist order-events pipeline

Kafka:

producers -> broker topic (partitions, retention) -> consumers
                                   -> ingestion job (Spark / connector) -> Delta bronze -> silver
                                   -> other consumers (fraud service, search index, notifications)

                        
Zero-bus:

producers -> Zerobus Ingest (serverless endpoint) -> Unity Catalog Delta table (bronze) -> MERGE -> silver



Zerobus is a push-based API with a single sink: the lakehouse.

A bus supports multiple independent consumers and buffers traffic during sink outages. Zerobus is single-sink 
and removes the ingestion infrastructure.


Instead of planning partitions and rebalancing consumers, Zerobus is scaled by by running more producers.



_In this lab:_


Producer pushed Olist order events into olist_order_events_bronze with no broker or ingestion job in between.

Injected deliberate duplicates into bronze to simulate retries

Re-running the same events grew bronze, silver was unchanged.

For Olist order events: the only consumer of the raw events is the analytics platform, so the zero-bus design 
is the simpler and cheaper fit. A bus would only be needed if real-time services  had to read the same event stream.
