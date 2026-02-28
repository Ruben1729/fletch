use anyhow::{anyhow, Result};
use arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent}; // ADDED: CatalogBuilder
use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};
use iceberg::table::Table;
use iceberg::transaction::Transaction; // ADDED: Transaction
use iceberg_catalog_sql::{SqlCatalogBuilder, SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE};
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use std::collections::HashMap;
use std::sync::Arc;

pub struct FletchConfig {
    pub store: Arc<dyn ObjectStore>,
    pub catalog: Arc<dyn Catalog>,
    pub table: Table,
    pub store_path: String,
    pub file_uri: String,
}

impl FletchConfig {
    pub async fn init(uri: &str, table_name: &str, run_id: &str, arrow_schema: Arc<ArrowSchema>) -> Result<Self> {
        let (store, catalog) = Self::infer_environment(uri).await?;

        let namespace = NamespaceIdent::from_strs(vec!["hil_telemetry"]).map_err(|e| anyhow!("{}", e))?;
        let table_ident = TableIdent::new(namespace.clone(), table_name.to_string());
        let iceberg_schema = Self::arrow_to_iceberg(&arrow_schema)?;

        let table_dir_uri = format!("{}/{}", uri.trim_end_matches('/'), table_name);
        let _ = catalog.create_namespace(&namespace, HashMap::new()).await;
        let table = match catalog.load_table(&table_ident).await {
            Ok(existing_table) => {
                Self::evolve_schema_if_needed(existing_table, &iceberg_schema).await?
            }
            Err(_) => {
                let creation = TableCreation::builder()
                    .name(table_ident.name().to_string())
                    .schema(iceberg_schema)
                    .location(table_dir_uri.clone()) // FIXED: Pass the full URI
                    .build();

                catalog.create_table(&namespace, creation).await.map_err(|e| anyhow!("{}", e))?
            }
        };

        Ok(Self {
            store,
            catalog,
            table,
            store_path: format!("{}/data/{}.parquet", table_name, run_id),
            file_uri: format!("{}/data/{}.parquet", table_dir_uri, run_id),
        })
    }

    async fn infer_environment(uri: &str) -> Result<(Arc<dyn ObjectStore>, Arc<dyn Catalog>)> {
        if uri.starts_with("file://") {
            let path = uri.strip_prefix("file:///").or_else(|| uri.strip_prefix("file://")).unwrap();
            let os_path = if cfg!(windows) {
                path.to_string()
            } else {
                format!("/{}", path)
            };
            std::fs::create_dir_all(&os_path)?;
            let store = Arc::new(LocalFileSystem::new_with_prefix(&os_path)?);
            let db_path = std::path::Path::new(&os_path).join("iceberg_catalog.db");
            if !db_path.exists() {
                std::fs::File::create(&db_path)?;
            }
            let catalog_url = format!("sqlite:{}", db_path.to_string_lossy());
            let catalog = SqlCatalogBuilder::default()
                .load(
                    "local_hil",
                    HashMap::from([
                        (SQL_CATALOG_PROP_URI.to_string(), catalog_url),
                        (SQL_CATALOG_PROP_WAREHOUSE.to_string(), uri.to_string()),
                    ])
                )
                .await
                .map_err(|e| anyhow!("Failed to initialize SQLite catalog: {}", e))?;
            Ok((store, Arc::new(catalog)))
        } else if uri.starts_with("s3://") {
            let bucket = uri.strip_prefix("s3://").unwrap().split('/').next().unwrap();
            let _store = Arc::new(AmazonS3Builder::from_env().with_bucket_name(bucket).build()?);
            unimplemented!("S3 Catalog initialization goes here")
        } else {
            Err(anyhow!("Unsupported URI scheme: {}", uri))
        }
    }

    fn arrow_to_iceberg(arrow_schema: &ArrowSchema) -> Result<IcebergSchema> {
        let mut fields = Vec::new();
        let mut field_id = 1;
        for field in arrow_schema.fields() {
            let iceberg_type = match field.data_type() {
                ArrowDataType::Int64 => Type::Primitive(PrimitiveType::Long),
                ArrowDataType::Float64 => Type::Primitive(PrimitiveType::Double),
                ArrowDataType::Utf8 => Type::Primitive(PrimitiveType::String),
                _ => return Err(anyhow!("Unsupported Arrow type: {:?}", field.data_type())),
            };
            fields.push(NestedField::optional(
                field_id,
                field.name(),
                iceberg_type,
            ).into());
            field_id += 1;
        }
        IcebergSchema::builder().with_fields(fields).build().map_err(|e| anyhow!("{}", e))
    }

    async fn evolve_schema_if_needed(table: Table, desired_schema: &IcebergSchema) -> Result<Table> {
        let current_schema = table.metadata().current_schema();
        let mut missing_fields = Vec::new();
        for desired_field in desired_schema.as_struct().fields() {
            if current_schema.field_by_name(&desired_field.name).is_none() {
                missing_fields.push(desired_field.clone());
            }
        }
        if missing_fields.is_empty() {
            return Ok(table);
        }
        let _tx = Transaction::new(&table);

        // Note: Assuming `update_schema` is implemented as an action.
        // If not, you may just want to return Ok(table) for now.
        /* let mut schema_update = tx.update_schema();
        for field in missing_fields {
            schema_update = schema_update.add_column(field.name.clone(), field.field_type.clone());
        }
        schema_update.commit().map_err(|e| anyhow!("{}", e))?;
        let evolved_table = tx.commit().await.map_err(|e| anyhow!("{}", e))?;
        return Ok(evolved_table);
        */

        println!("Schema evolution is required but bypassed pending full v0.8.0 Action support.");
        Ok(table)
    }
}