# UE28 Big Data — Project Checklist

## Spark / Code
- [ ] Always write/save outputs with `.write` (Parquet or Delta format preferred)
- [ ] Define schemas explicitly when reading data
- [ ] Think about partitioning
- [ ] Code must be Spark-native, readable, and commented
- [ ] Use advanced Spark transformations/actions, not just basic ones

## Data Ingestion (AA3)
- [ ] Define a schema at read time
- [ ] Include data cleaning steps (don't assume data is clean)
- [ ] Data must flow: Spark → exported → consumed by the dashboard
- [ ] Dashboard must NOT load data from SQLite or any non-Spark source

## Dashboard
- [ ] Must be analytically linked to what you do in Spark
- [ ] Must include at least 2 relevant visualizations coherent with your problem statement
- [ ] Add a page/section showing data processed by Spark

## Performance (AA4)
- [ ] Include at least one performance measurement (execution time, dataset size) with commentary
- [ ] Ideally use Spark UI

## Report
- [ ] Must go beyond a chronological description
- [ ] Include critical reflection on your choices
- [ ] Mention what you'd do differently and propose concrete improvements

## Deliverables (Data engineering — due 10/05)
- [ ] Production-ready project (code refactored out of notebook)
- [ ] Demo ready
