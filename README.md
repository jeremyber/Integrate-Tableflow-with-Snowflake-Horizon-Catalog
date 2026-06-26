# Tableflow + Snowflake Horizon Catalog: Integration Guide

Tableflow publishes an Iceberg REST Catalog (IRC) endpoint for each Confluent Cloud environment. This guide connects Snowflake to that endpoint, registers your Kafka topic as an Iceberg table in Snowflake's Horizon Catalog, and gets you to a working SQL query against live Kafka data.

**Before you start:** Complete [the prerequisite setup](00-setup.md) in full.

Complete these steps:

1. Find your Tableflow credentials
2. Create the catalog integration in Snowflake
3. Register your Kafka topic as an Iceberg table
4. Query your data

## What is Snowflake Horizon Catalog?

Horizon Catalog is Snowflake's default catalog. Your Iceberg tables land there automatically — nothing extra to buy or enable.

## How it works

```
Apache Kafka® Topic (Avro)
         │
         ▼
    Tableflow
         │ writes Iceberg files
         ▼
BYOS Bucket (S3 / ADLS Gen2 / GCS)
         │                     │
         │ metadata              │ data files (Parquet)
         ▼                     ▼
Iceberg REST Catalog       External Volume
   (IRC endpoint)          (IAM credentials)
         │                     │
         └─────────┬───────────┘
                   ▼
    Snowflake Catalog Integration
                   │
                   ▼
    Iceberg Table in Snowflake Horizon Catalog
                   │
                   ▼
               SQL Query
```

The IRC provides Snowflake with table metadata — schema, snapshot history, and file paths. The external volume provides the IAM credentials to open those files. Both are required: the IRC alone cannot read data; the external volume alone cannot locate tables.

---

## Prerequisites

- Tableflow enabled on the `stock_trades` topic — see `00-setup.md`, Step 6
- External volume `tableflow_external_volume` created — see `00-setup.md`, Step 8
- `tableflow_db.confluent` schema exists in Snowflake — see `00-setup.md`, Step 7
- ACCOUNTADMIN role in Snowflake

---

## Step 1: Find your Tableflow credentials

Grab three things from Confluent Cloud before writing any Snowflake SQL: the IRC endpoint URL, a Tableflow API key, and your cluster ID.

### IRC endpoint URL

1. Inside your cluster, click **Tableflow** in the left sidebar.
2. Under **Tableflow API Access**, locate the **REST Catalog Endpoint**.

It will look like this:

```
https://tableflow.<region>.aws.confluent.cloud/iceberg/catalog/organizations/<org-id>/environments/<env-id>
```

**Note**
This endpoint is currently documented for AWS. If your cluster is on Azure or GCP, verify the current endpoint format before proceeding. For more information, see [Query Iceberg Tables with Snowflake and Tableflow](https://docs.confluent.io/cloud/current/topics/tableflow/how-to-guides/query-engines/query-with-snowflake.html).

### Tableflow API key

Snowflake authenticates to the IRC by using OAuth2 client credentials. Create a Tableflow-scoped API key by completing the following steps.

1. Click the hamburger menu **(≡)** → **API Keys**.
2. Click **+ Add API key**.
3. Select **Tableflow** as the resource type.
4. Select the environment where your topic lives.
5. Click **Create API key**.
6. Copy both the **Key** (Client ID) and the **Secret** (Client Secret) immediately. The secret is shown only once.

### Cluster ID

1. Inside your cluster, click **Cluster Settings** in the left sidebar.
2. Copy the **Cluster ID** — it starts with `lkc-`.

Save these values before continuing:

| Value | Example |
|---|---|
| IRC endpoint URL | `https://tableflow.us-east-1.aws.confluent.cloud/iceberg/catalog/organizations/<org-id>/environments/<env-id>` |
| Client ID | `ABCDEF123456` |
| Client Secret | `abc/xyz+...` |
| Cluster ID | `lkc-abc123` |
| Topic name | `stock_trades` |

---

## Step 2: Create the catalog integration in Snowflake

A catalog integration connects Snowflake to an external Iceberg catalog. Run the following query as `ACCOUNTADMIN` in a Snowflake SQL worksheet. Replace the placeholder values with your credentials from Step 1.

```sql
CREATE OR REPLACE CATALOG INTEGRATION tableflow_catalog_integration
  CATALOG_SOURCE    = ICEBERG_REST
  TABLE_FORMAT      = ICEBERG
  CATALOG_NAMESPACE = '<cluster-id>'
  REST_CONFIG = (
    CATALOG_URI      = '<irc-endpoint-url>'
    CATALOG_API_TYPE = PUBLIC
  )
  REST_AUTHENTICATION = (
    TYPE                 = OAUTH
    OAUTH_CLIENT_ID      = '<client-id>'
    OAUTH_CLIENT_SECRET  = '<client-secret>'
    OAUTH_ALLOWED_SCOPES = ('catalog')
  )
  ENABLED = true;
```

| Placeholder | Value |
|---|---|
| `<cluster-id>` | Cluster ID from Step 1 (for example, `lkc-abc123`) |
| `<irc-endpoint-url>` | IRC endpoint URL from Step 1 |
| `<client-id>` | API Key from Step 1 |
| `<client-secret>` | API Secret from Step 1 |

**Note**
`CATALOG_NAMESPACE` is set to your Kafka cluster ID. In Tableflow's IRC, the cluster ID is the Iceberg namespace and each topic in that cluster is a table.

Run the following to confirm the integration was created:

```sql
SHOW INTEGRATIONS LIKE 'tableflow%';
```

Expected output: one row with `name = TABLEFLOW_CATALOG_INTEGRATION` and `category = CATALOG`.

### Grant privileges to your working role

Run the following as `ACCOUNTADMIN`. Replace `SYSADMIN` with whichever role you use for queries.

```sql
-- Access the catalog integration
GRANT USAGE ON INTEGRATION tableflow_catalog_integration
  TO ROLE SYSADMIN;

-- Access the external volume
GRANT USAGE ON EXTERNAL VOLUME tableflow_external_volume
  TO ROLE SYSADMIN;

-- Create Iceberg tables in the schema
GRANT CREATE ICEBERG TABLE ON SCHEMA tableflow_db.confluent
  TO ROLE SYSADMIN;
```

---

## Step 3: Register your Kafka topic as an Iceberg table

Snowflake doesn't auto-discover tables from the catalog integration — register each topic manually with one `CREATE ICEBERG TABLE` statement.

**Note**
Wait about 1 minute after enabling Tableflow before running this step. Tableflow writes its first Iceberg snapshot within ~1 minute. The data files may take a few minutes longer — if your first `SELECT` returns 0 rows, run `ALTER ICEBERG TABLE <name> REFRESH` once data appears in your bucket. You do not need to recreate the table.

Run the following as `SYSADMIN`:

```sql
USE ROLE SYSADMIN;
USE DATABASE tableflow_db;
USE SCHEMA confluent;

CREATE OR REPLACE ICEBERG TABLE stock_trades
  EXTERNAL_VOLUME    = 'tableflow_external_volume'
  CATALOG            = 'tableflow_catalog_integration'
  CATALOG_TABLE_NAME = 'stock_trades';
```

| Parameter | Value |
|---|---|
| `EXTERNAL_VOLUME` | External volume from `00-setup.md`, Step 8 |
| `CATALOG` | Catalog integration from Step 2 |
| `CATALOG_TABLE_NAME` | Kafka topic name exactly as it appears in Confluent Cloud (case-sensitive) |

**Important**
`CATALOG_TABLE_NAME` must match the Kafka topic name exactly, including case and hyphens. `stock_trades` and `Stock_Trades` are treated as different tables.

Run the following to confirm the table was created:

```sql
SHOW ICEBERG TABLES IN SCHEMA tableflow_db.confluent;
```

Expected output: one row with `name = STOCK_TRADES` and `table_format = ICEBERG`.

### Register additional topics

To register another Kafka topic, repeat this step with a different table name and `CATALOG_TABLE_NAME`. One `CREATE ICEBERG TABLE` per topic.

---

## Step 4: Query your data

Run the following query to confirm data is flowing end to end:

```sql
SELECT *
FROM tableflow_db.confluent.stock_trades
LIMIT 10;
```

You should see rows with fields including `side`, `quantity`, `symbol`, `price`, and `account`.

### Refresh table metadata

Snowflake caches Iceberg metadata at query time and won't see new snapshots until you refresh. Run the following before querying fresh data:

```sql
ALTER ICEBERG TABLE tableflow_db.confluent.stock_trades REFRESH;
```

### Configure auto-refresh (recommended for production)

For production, configure Snowflake to refresh automatically when new Iceberg snapshots arrive. This requires additional cloud configuration — S3 event notifications (AWS), Azure Event Grid (Azure), or GCP Pub/Sub (GCP).

For more information, see the [Snowflake Iceberg auto-refresh documentation](https://docs.snowflake.com/en/user-guide/tables-iceberg-auto-refresh).

For demos and development, manual refresh is sufficient.

---

## Troubleshooting

| Problem | Solution |
|---|---|
| `invalid_grant` when creating the catalog integration | Ensure you created a Tableflow-scoped API key, not a general Confluent Cloud API key. Navigate to **Administration → API Keys**, select **Tableflow** as the resource type, and create a new key. |
| `CREATE ICEBERG TABLE` fails with `Error assuming AWS_ROLE` | The Snowflake IAM user ARN in the role trust policy is wrong or missing. Run `DESC EXTERNAL VOLUME tableflow_external_volume`, copy the exact `STORAGE_AWS_IAM_USER_ARN`, and verify it matches `Principal.AWS` in the role's trust policy. Also confirm `sts:ExternalId` matches `STORAGE_AWS_EXTERNAL_ID`. |
| `SELECT` returns 0 rows | Iceberg metadata is ready but data files haven't landed yet. Wait a few minutes, run `ALTER ICEBERG TABLE <name> REFRESH`, then query again. You do not need to recreate the table. |
| Stale data after new records arrive | Run `ALTER ICEBERG TABLE stock_trades REFRESH`. For production, configure auto-refresh. |
| IRC endpoint not visible in Confluent Cloud | Tableflow must be enabled on at least one topic before the IRC endpoint appears. Complete Step 6 in `00-setup.md` first. |
| `SHOW ICEBERG TABLES` is empty after creating the integration | `CREATE ICEBERG TABLE` was not run. The catalog integration does not auto-create tables — run it explicitly for each topic (Step 3). |
| 401 Unauthorized when testing the IRC endpoint manually | The OAuth token expires after 15 minutes. Re-fetch the token. Snowflake handles token refresh automatically once the catalog integration is in place. |

---

## Checklist

- [ ] `SHOW INTEGRATIONS LIKE 'tableflow%'` returns one row
- [ ] `SHOW ICEBERG TABLES IN SCHEMA tableflow_db.confluent` returns `stock_trades`
- [ ] `SELECT * FROM tableflow_db.confluent.stock_trades LIMIT 10` returns rows
- [ ] `ALTER ICEBERG TABLE stock_trades REFRESH` runs without errors

For more information, see [Query Iceberg Tables with Snowflake and Tableflow](https://docs.confluent.io/cloud/current/topics/tableflow/how-to-guides/query-engines/query-with-snowflake.html).
