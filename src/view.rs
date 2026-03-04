use anyhow::{anyhow, Result};
use futures::StreamExt;
use std::collections::HashMap;
use ::iceberg::{Catalog, CatalogBuilder, TableIdent};
use ::iceberg::table::Table;
use iceberg_catalog_sql::{SqlCatalogBuilder, SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE};
use polars::prelude::*;
use std::fs::File;
use std::path::Path;

use crate::workspace::FletchWorkspace;

struct ViewSource {
    table_name: String,
    columns: Vec<String>,
}

pub struct FletchViewBuilder<'a> {
    workspace: &'a FletchWorkspace,
    run_id: Option<String>,
    sources: Vec<ViewSource>,
    add_relative_timestamp: bool
}

impl<'a> FletchViewBuilder<'a> {
    pub fn new(workspace: &'a FletchWorkspace) -> Self {
        Self {
            workspace,
            run_id: None,
            sources: Vec::new(),
            add_relative_timestamp: false
        }
    }

    pub fn run_id(mut self, run_id: impl Into<String>) -> Self {
        self.run_id = Some(run_id.into());
        self
    }

    pub fn add_source(mut self, table_name: &str, columns: &[&str]) -> Self {
        self.sources.push(ViewSource {
            table_name: table_name.to_string(),
            columns: columns.iter().map(|s| s.to_string()).collect(),
        });
        self
    }

    pub fn with_relative_timestamp(mut self) -> Self {
        self.add_relative_timestamp = true;
        self
    }

    pub async fn build(self) -> Result<FletchView> {
        if self.sources.is_empty() {
            return Err(anyhow!("At least one source must be added to the view"));
        }

        let uri = self.workspace.uri();
        let os_path = uri.strip_prefix("file:///").unwrap_or(uri);
        let catalog = Self::load_catalog(uri, os_path).await?;

        let mut base_lf: Option<LazyFrame> = None;

        for source in self.sources {
            let table_ident = TableIdent::new(self.workspace.namespace().clone(), source.table_name.clone());
            let table = catalog.load_table(&table_ident).await
                .map_err(|_| anyhow!("Table {} not found", source.table_name))?;
            let file_paths = Self::resolve_iceberg_files(&table).await?;
            if file_paths.is_empty() {
                return Err(anyhow!("No data files found for table {}", source.table_name));
            }
            let mut lfs = Vec::new();
            for file_path in file_paths {
                let scan_args = ScanArgsParquet { n_rows: None, ..Default::default() };
                lfs.push(LazyFrame::scan_parquet(PlRefPath::new(&file_path), scan_args)?);
            }
            let mut lf = concat(lfs, Default::default())?;
            if let Some(ref r_id) = self.run_id {
                lf = lf.filter(col("run_id").eq(lit(r_id.clone())));
            }
            let mut selection = vec![col("timestamp_ns"), col("run_id")];
            for c in source.columns {
                selection.push(col(&c));
            }
            lf = lf.select(selection);
            lf = lf.sort(["timestamp_ns"], Default::default());
            if let Some(existing_lf) = base_lf {
                let asof_options = AsOfOptions {
                    strategy: AsofStrategy::Backward,
                    left_by: Some(vec!["run_id".into()]),
                    right_by: Some(vec!["run_id".into()]),
                    ..Default::default()
                };
                let join_args = JoinArgs::new(JoinType::AsOf(Box::new(asof_options)));
                base_lf = Some(existing_lf.join(
                    lf,
                    [col("timestamp_ns")],
                    [col("timestamp_ns")],
                    join_args,
                ));
            } else {
                base_lf = Some(lf);
            }
        }

        let mut final_lf = base_lf.unwrap();

        if self.add_relative_timestamp {
            final_lf = final_lf.with_columns([
                (col("timestamp_ns") - col("timestamp_ns").min().over(["run_id"]))
                    .alias("relative_time_ns")
            ]);
        }

        Ok(FletchView {
            lazy_frame: final_lf,
        })
    }

    async fn load_catalog(uri: &str, os_path: &str) -> Result<impl Catalog> {
        let db_path = std::path::Path::new(os_path).join("iceberg_catalog.db");
        let catalog_url = format!("sqlite:{}", db_path.to_string_lossy());
        SqlCatalogBuilder::default()
            .load(
                "local_hil",
                HashMap::from([
                    (SQL_CATALOG_PROP_URI.to_string(), catalog_url),
                    (SQL_CATALOG_PROP_WAREHOUSE.to_string(), uri.to_string()),
                ])
            )
            .await
            .map_err(|e| anyhow!("Failed to load catalog: {}", e))
    }

    async fn resolve_iceberg_files(table: &Table) -> Result<Vec<String>> {
        let scan = table.scan().build().map_err(|e| anyhow!("{}", e))?;
        let mut file_stream = scan.plan_files().await.map_err(|e| anyhow!("{}", e))?;

        let mut paths = Vec::new();
        while let Some(task_res) = file_stream.next().await {
            let task = task_res.map_err(|e| anyhow!("{}", e))?;

            let raw_path = task.data_file_path().to_string();
            let clean_path = raw_path.strip_prefix("file:///").unwrap_or(&raw_path);
            paths.push(clean_path.to_string());
        }
        Ok(paths)
    }
}

pub struct FletchView {
    lazy_frame: LazyFrame,
}

impl FletchView {
    pub fn collect(self) -> Result<DataFrame> {
        self.lazy_frame.collect().map_err(|e| anyhow!("Failed to compute view: {}", e))
    }

    pub fn into_lazy(self) -> LazyFrame {
        self.lazy_frame
    }
    pub fn to_csv<P: AsRef<Path>>(self, path: P) -> Result<()> {
        let mut df = self.collect()?;
        let mut file = File::create(path).map_err(|e| anyhow!("Failed to create CSV file: {}", e))?;
        CsvWriter::new(&mut file)
            .include_header(true)
            .finish(&mut df)
            .map_err(|e| anyhow!("Failed to write CSV: {}", e))?;
        Ok(())
    }
    pub fn to_parquet<P: AsRef<Path>>(self, path: P) -> Result<()> {
        let mut df = self.collect()?;
        let mut file = File::create(path).map_err(|e| anyhow!("Failed to create Parquet file: {}", e))?;
        ParquetWriter::new(&mut file)
            .finish(&mut df)
            .map_err(|e| anyhow!("Failed to write Parquet: {}", e))?;
        Ok(())
    }
}