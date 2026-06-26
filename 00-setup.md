# Tableflow + Snowflake Horizon Catalog: Setup Guide

Confluent Tableflow materializes Apache Kafka® topics as Apache Iceberg™ tables in your own cloud storage bucket. This guide sets up the Confluent Cloud side and the Snowflake side so you are ready to run the integration in `README.md`.

Complete these steps:

1. Create an environment
2. Create a Kafka cluster
3. Enable Schema Registry
4. Create a topic
5. Produce sample data
6. Enable Tableflow with BYOS storage
7. Create a Snowflake database and schema
8. Create a Snowflake external volume
9. Grant privileges on the external volume

## Prerequisites

- Confluent Cloud account
- Cloud storage account on AWS, Azure, or GCP — whichever region matches your intended Kafka cluster
- Snowflake account (Enterprise edition or higher recommended for full Iceberg support)
- ACCOUNTADMIN role in Snowflake
- Permissions to create IAM roles and policies (AWS), service principals (Azure), or service accounts (GCP) in your cloud account

---

## Step 1: Create an environment

An environment groups your Confluent Cloud clusters and Schema Registry together. Use one per team or deployment stage.

1. Log in to [Confluent Cloud](https://confluent.io).
2. Click **Environments** in the left sidebar.
3. Click **+ Add environment**.
4. Name the environment (for example: `tableflow-snowflake-demo`).
5. Click **Create**.

---

## Step 2: Create a Kafka cluster

1. Inside the environment, click **Create cluster**.
2. Select **Basic**.
3. Select your cloud provider and region.

**Important**
Ensure the cluster region matches the region of the storage bucket you will create in Step 6. Tableflow requires the cluster and bucket to be in the same region.

4. Name the cluster and click **Launch cluster**.

---

## Step 3: Enable Schema Registry

Tableflow requires Schema Registry when using Avro format.

1. Click **Schema Registry** in the left sidebar.
2. Click **Enable Schema Registry**.
3. Select the same cloud provider and a region close to your cluster.
4. Click **Enable**.

---

## Step 4: Create a topic

1. Inside the cluster, click **Topics** in the left sidebar.
2. Click **+ Create topic**.

| Setting | Value |
|---|---|
| Topic name | `stock_trades` |
| Partitions | 6 |
| Retention | 7 days |

3. Click **Create with defaults**.

**Note**
The topic name becomes the Iceberg table name in Snowflake. Hyphens are allowed; spaces are not.

---

## Step 5: Produce sample data

Use the Datagen Source connector to generate sample stock trade records without needing a real data source.

1. Inside the cluster, click **Connectors** in the left sidebar.
2. Click **+ Add connector**.
3. Search for **Datagen Source** and select it.
4. Configure the connector:

| Setting | Value |
|---|---|
| Topic | `stock_trades` |
| Output record value format | Avro |
| Quickstart | STOCK_TRADES |
| Max interval (ms) | 1000 |
| Tasks | 1 |
| Schema context | Default |

5. Click **Continue**, then click **Launch**.

Navigate to **Topics → stock_trades → Messages** and confirm records are arriving. You should see fields including `side`, `quantity`, `symbol`, and `price`.

---

## Step 6: Enable Tableflow with BYOS storage

BYOS (Bring Your Own Storage) means Tableflow writes Iceberg files to a bucket in your own cloud account. Your data never lands in Confluent's storage.

Ensure the bucket or container is empty before enabling Tableflow. Existing objects may cause Tableflow to fail to start.

Follow Confluent's official BYOS setup guide for your cloud provider:

- **AWS S3:** [Configure Tableflow with Amazon S3](https://docs.confluent.io/cloud/current/topics/tableflow/how-to-guides/configure-storage.html)
- **Azure ADLS Gen2:** [Configure Tableflow with Azure Data Lake Storage Gen2](https://docs.confluent.io/cloud/current/topics/tableflow/how-to-guides/configure-storage.html)
- **GCP GCS:** [Configure Tableflow with Google Cloud Storage](https://docs.confluent.io/cloud/current/topics/tableflow/how-to-guides/configure-storage.html)

**Note**
Create the AWS IAM permission policy in **IAM → Policies → Create policy → JSON tab**.

Once Tableflow is enabled, verify that files appear in your bucket within 5–10 minutes and that the Tableflow status shows green under **Topics → stock_trades → Tableflow**.

---

## Step 7: Create a Snowflake database and schema

Run the following commands in a Snowflake SQL worksheet as `ACCOUNTADMIN`.

```sql
USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS tableflow_db;
CREATE SCHEMA IF NOT EXISTS tableflow_db.confluent;
```

---

## Step 8: Create a Snowflake external volume

A Snowflake external volume stores the IAM credentials Snowflake needs to read Parquet files from your storage bucket. This is separate from the catalog integration you will create in `README.md`: the external volume handles file access; the catalog integration handles table structure.

Creating an external volume takes two rounds with Snowflake and your cloud IAM:
1. Create the volume in Snowflake — Snowflake returns its IAM identity.
2. Grant that identity access to your bucket — Snowflake can now read your files.

Expand your cloud provider below.

---

<details>
<summary><strong>AWS — Amazon S3</strong></summary>

#### Create the external volume

Run the following statement as `ACCOUNTADMIN`. Replace `<bucket-name>` and `<aws-account-id>` with your values. You will update the role ARN after the next step.

```sql
CREATE OR REPLACE EXTERNAL VOLUME tableflow_external_volume
  STORAGE_LOCATIONS = ((
    NAME             = 'tableflow-s3-location'
    STORAGE_PROVIDER = 'S3'
    STORAGE_BASE_URL = 's3://<bucket-name>'
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<aws-account-id>:role/confluent-tableflow-role'
  ));
```

#### Retrieve Snowflake's IAM identity

```sql
DESC EXTERNAL VOLUME tableflow_external_volume;
```

Copy the `property_value` for both of the following:

- `STORAGE_AWS_IAM_USER_ARN` — looks like `arn:aws:iam::123456789012:user/abc123-s`
- `STORAGE_AWS_EXTERNAL_ID`

#### Add Snowflake to the Confluent Tableflow IAM role

Rather than creating a new role, add a second trust statement to the existing `confluent-tableflow-role` from Step 6. Snowflake inherits the S3 permissions the role already has.

1. In the AWS Console, navigate to **IAM → Roles** and open `confluent-tableflow-role`.
2. Click **Trust relationships → Edit trust policy**.
3. Add a second statement inside the existing `Statement` array. Replace the placeholder values with the values you copied above:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "...existing Confluent statement..."
    },
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "<storage-aws-iam-user-arn>"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "<storage-aws-external-id>"
        }
      }
    }
  ]
}
```

4. Click **Update policy**.
5. Copy the role's **ARN** from the role summary page.

#### Update the external volume with the role ARN

```sql
CREATE OR REPLACE EXTERNAL VOLUME tableflow_external_volume
  STORAGE_LOCATIONS = ((
    NAME             = 'tableflow-s3-location'
    STORAGE_PROVIDER = 'S3'
    STORAGE_BASE_URL = 's3://<bucket-name>'
    STORAGE_AWS_ROLE_ARN = '<confluent-tableflow-role-arn>'
  ));
```

Run `DESC EXTERNAL VOLUME tableflow_external_volume` and confirm the role ARN is present. End-to-end access is confirmed in `README.md` when `CREATE ICEBERG TABLE` completes without error.

</details>

---

<details>
<summary><strong>Azure — Azure Data Lake Storage Gen2</strong></summary>

#### Create the external volume

Replace `<storage-account>` and `<container>` with your values from Step 6.

```sql
CREATE OR REPLACE EXTERNAL VOLUME tableflow_external_volume
  STORAGE_LOCATIONS = ((
    NAME             = 'tableflow-azure-location'
    STORAGE_PROVIDER = 'AZURE'
    STORAGE_BASE_URL = 'azure://<storage-account>.blob.core.windows.net/<container>'
  ));
```

#### Retrieve Snowflake's Azure identity

```sql
DESC EXTERNAL VOLUME tableflow_external_volume;
```

Copy the `AZURE_CONSENT_URL` and open it in a browser. Grant Snowflake consent to access your Azure tenant. This is a one-time step.

Copy the `AZURE_MULTI_TENANT_APP_NAME` — this is the service principal Snowflake uses to access your storage.

#### Assign the storage role to Snowflake's service principal

1. In the Azure Portal, navigate to your storage account.
2. Click **Access Control (IAM) → + Add role assignment**.
3. Assign **Storage Blob Data Contributor** to the service principal from `AZURE_MULTI_TENANT_APP_NAME`.

Run `DESC EXTERNAL VOLUME tableflow_external_volume` and confirm the storage account URL is present. End-to-end access is confirmed in `README.md` when `CREATE ICEBERG TABLE` completes without error.

</details>

---

<details>
<summary><strong>GCP — Google Cloud Storage</strong></summary>

#### Create the external volume

```sql
CREATE OR REPLACE EXTERNAL VOLUME tableflow_external_volume
  STORAGE_LOCATIONS = ((
    NAME             = 'tableflow-gcs-location'
    STORAGE_PROVIDER = 'GCS'
    STORAGE_BASE_URL = 'gcs://<bucket-name>'
  ));
```

#### Retrieve Snowflake's GCP service account

```sql
DESC EXTERNAL VOLUME tableflow_external_volume;
```

Copy the `STORAGE_GCP_SERVICE_ACCOUNT` value — it looks like `abc123@<project-id>.iam.gserviceaccount.com`.

#### Grant bucket access to Snowflake's service account

1. In the GCP Console, navigate to your GCS bucket.
2. Click **Permissions → Grant access**.
3. Paste Snowflake's service account email.
4. Assign the role **Storage Object Viewer** (`roles/storage.objectViewer`).
5. Click **Save**.

Run `DESC EXTERNAL VOLUME tableflow_external_volume` and confirm the service account is present. End-to-end access is confirmed in `README.md` when `CREATE ICEBERG TABLE` completes without error.

</details>

---

## Step 9: Grant privileges on the external volume

Run the following as `ACCOUNTADMIN`. Replace `SYSADMIN` with whichever role you use for queries.

```sql
GRANT USAGE ON EXTERNAL VOLUME tableflow_external_volume
  TO ROLE SYSADMIN;
```

---

## Verify setup

Before opening `README.md`, confirm all of the following:

- [ ] Datagen connector is running — **Topics → stock_trades → Messages** shows records arriving
- [ ] Tableflow status is green — **Topics → stock_trades → Tableflow**
- [ ] Files appear in your cloud storage bucket (allow 5–10 minutes)
- [ ] `DESC EXTERNAL VOLUME tableflow_external_volume` returns your storage location without errors
- [ ] `tableflow_db.confluent` schema exists in Snowflake

For more information, see [Configure Tableflow Storage](https://docs.confluent.io/cloud/current/topics/tableflow/how-to-guides/configure-storage.html).
