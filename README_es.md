# Data Lake con arquitecturas escalables

## Descripción
Repositorio para el diseño e implementación de un Data Lake escalable para datos semiestructurados y no estructurados, utilizando Azure Data Lake y Delta Lake.

## Objetivos
- Diseñar capas de ingesta y consulta.
- Implementar buenas prácticas de particionado y escalabilidad.
- Proporcionar ejemplos de lectura y escritura con Spark y Python.
- Incluir monitorización, alertas y validación de calidad de datos.

## Tech stack
- **Almacenamiento**: Azure Data Lake Storage Gen2
- **Procesamiento**: Apache Spark (pyspark)
- **Formato**: Delta Lake / Parquet
- **Lenguaje**: Python 3.x
- **Validación**: Great Expectations, AWS Deequ
- **Monitorización**: Azure Monitor con Kusto

## Estructura del repositorio
\`\`\`
.
├── README_es.md
├── src/
│   ├── ingestion/
│   │   ├── batch_ingestion.py
│   │   └── streaming_ingestion.py
│   ├── transformations/
│   │   └── transform.py
│   ├── validation/
│   │   ├── great_expectations/
│   │   └── deequ/
│   └── monitoring/
│       └── alerts.kql
├── docs/
│   └── arquitectura.md
└── config/
    ├── spark_conf.yaml
    └── azure_storage.json
\`\`\`

## Datasets
- **Alquileres**: \`dld_rent_contracts-open\` de Dubai Land Department (Ejari).
- **Transacciones**: \`dld_transactions-open\` de Dubai Land Department.

## Esquema de particionado
- Particionado por: fecha (\`year\`, \`month\`, \`day\`), \`emirate\`, \`property_type\`
- Formato: Delta Lake / Parquet

## Pipelines de ingesta
### Batch
- Frecuencia: diaria a las 00:30 AM UTC+2.
- Script: \`src/ingestion/batch_ingestion.py\`
- Descripción: descarga datasets, aplica esquemas, escribe particiones.

### Streaming (micro-batches)
- Trigger: cada 24 horas.
- Script: \`src/ingestion/streaming_ingestion.py\`
- Descripción: captura nuevos registros y escribe al final del día.

## Monitorización y alertas
- Métricas: número de registros, latencia, errores por job.
- Alertas: Azure Monitor con KQL.
- Ejemplo de alerta (duration > 1h, errores > 0) en \`config/alerts.kql\`.

## Validación de datos
- **Great Expectations**: validaciones de esquema y valores nulos en \`src/validation/great_expectations/\`.
- **Deequ**: checks de calidad en \`src/validation/deequ/\`.

## Ejemplos de código
### Lectura en Spark (Python)
\`\`\`python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("DataLakeIngestion")
         .getOrCreate())

df = (spark.read.format("delta")
      .load("abfss://datalake@youraccount.dfs.core.windows.net/path/to/delta/dld_rent_contracts-open")
      .filter("year = 2025 and month = 4 and emirate = 'Dubai'"))
df.show(5)
\`\`\`

### Escritura en Spark
\`\`\`python
(df.write.format("delta")
    .mode("append")
    .partitionBy("year", "month", "day", "emirate", "property_type")
    .save("abfss://datalake@youraccount.dfs.core.windows.net/path/to/delta/dld_transactions-open"))
\`\`\`

## Despliegue
- Configura credenciales en \`config/azure_storage.json\`.
- Ajusta parámetros de Spark en \`config/spark_conf.yaml\`.
- Ejecuta \`src/ingestion/batch_ingestion.py\` en Azure Databricks o Synapse.
