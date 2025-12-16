## Gold Layer

The Gold layer contains curated, analytics-ready datasets designed for reporting and business intelligence use cases.

### Responsibilities
- Publish trusted datasets from Silver layer
- Apply business rules and data quality validations
- Enrich data with derived attributes
- Ensure schema consistency and reliability
- Support downstream BI and analytics workloads

### Design Approach
- Streaming Delta reads from Silver layer
- Declarative-style transformation logic
- Data quality rules applied before publishing
- Separate curated tables for lookup and fact-style datasets

### Technologies
- PySpark
- Delta Lake
- Azure Databricks
