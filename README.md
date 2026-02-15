### REAL TIME STOCK MARKET ANALYSIS STREAMING PIPELINE 

In this lightweight streaming pipeline project; a pipeline that extracts stock data from the Alpha Vantage API is implemented, which is streamed through Apache Kafka and processed with Spark with the processed data loaded into a postgres database. 

All services are containerized with Docker for easy deployment. 

### PIPELINE ARCHITECTURE

![Data Pipeline Architecture](./img/overview.png)


Project Tech Stack and Flow
- `Kafka UI : For inspecting topics and messaging`
- `API : produces JSON events into kafka`
- `Spark : consumes from kafka and writes to postgres`
- `pgAdmin : manage postgres visually`
- `Postgres : Stores results for analytics`
- `Power BI ; connects to postgres for data visualization/analytics`