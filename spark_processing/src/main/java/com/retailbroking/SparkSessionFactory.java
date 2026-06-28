package com.retailbroking;

import org.apache.spark.sql.SparkSession;

/**
 * Builds a SparkSession pre-configured for:
 *   - Iceberg catalog "local" backed by Hive Metastore
 *   - Iceberg S3FileIO pointing at the MinIO warehouse bucket (s3a://warehouse/)
 *
 * MinIO endpoint is read from the MINIO_ENDPOINT environment variable.
 * Default is http://localhost:9000 (local runs against the Docker Compose stack).
 * Set MINIO_ENDPOINT=http://minio:9000 when running inside Docker on the same network.
 *
 * Hive Metastore URI is read from HIVE_METASTORE_URI.
 * Default is thrift://localhost:9083 for local spark-submit runs.
 *
 * The spark.master property defaults to local[*] for IDE / direct-java runs.
 * When submitting via spark-submit, --master overrides this automatically.
 */
public final class SparkSessionFactory {

    private SparkSessionFactory() {}

    public static SparkSession create(String appName) {
        String minioEndpoint = System.getenv()
                .getOrDefault("MINIO_ENDPOINT", "http://localhost:9000");
        String hiveMetastoreUri = System.getenv()
                .getOrDefault("HIVE_METASTORE_URI", "thrift://localhost:9083");

        return SparkSession.builder()
                .appName(appName)
                .master(System.getProperty("spark.master", "local[*]"))

                // ── Iceberg extensions + Hive catalog "local" ─────────────────
                .config("spark.sql.extensions",
                        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.local",
                        "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.local.type", "hive")
                .config("spark.sql.catalog.local.uri", hiveMetastoreUri)
                .config("spark.sql.catalog.local.warehouse", "s3a://warehouse/")
                .config("spark.sql.catalog.local.io-impl",
                        "org.apache.iceberg.aws.s3.S3FileIO")
                .config("spark.sql.catalog.local.s3.endpoint", minioEndpoint)
                .config("spark.sql.catalog.local.s3.access-key-id", "minioadmin")
                .config("spark.sql.catalog.local.s3.secret-access-key", "minioadmin")
                .config("spark.sql.catalog.local.s3.path-style-access", "true")
                .config("spark.sql.catalog.local.client.region", "us-east-1")

                // ── S3A → MinIO ───────────────────────────────────────────────
                .config("spark.hadoop.fs.s3a.endpoint", minioEndpoint)
                .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
                .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.impl",
                        "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

                // Treat all datetimes as UTC — matches Flink's ZoneOffset.UTC parsing
                .config("spark.sql.session.timeZone", "UTC")

                .getOrCreate();
    }
}
