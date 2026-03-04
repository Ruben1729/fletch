#[macro_export]
macro_rules! fletch_schema {
    (
        $struct_name:ident {
            $($field_name:ident : $rust_type:ty),* $(,)?
        }
    ) => {
        pub struct $struct_name {
            sink: $crate::BackgroundSink,
            schema: std::sync::Arc<arrow::datatypes::Schema>,
            timestamps: arrow::array::Int64Builder,
            run_id: String, // 1. Defined here
            current_ts: Option<i64>,
            $( $field_name: (<$rust_type as $crate::FletchType>::Builder, Option<$rust_type>), )*
        }

        impl $struct_name {
            pub async fn try_new(workspace: &$crate::FletchWorkspace, run_id: &str) -> anyhow::Result<Self> {
                let mut fields = vec![
                    arrow::datatypes::Field::new("timestamp_ns", arrow::datatypes::DataType::Int64, false),
                    arrow::datatypes::Field::new(
                        "run_id",
                        arrow::datatypes::DataType::Dictionary(
                            Box::new(arrow::datatypes::DataType::Int32),
                            Box::new(arrow::datatypes::DataType::Utf8),
                        ),
                        false,
                    ),
                ];
                $( fields.push(arrow::datatypes::Field::new(
                    stringify!($field_name),
                    <$rust_type as $crate::FletchType>::data_type(),
                    true
                )); )*
                let schema = std::sync::Arc::new(
                    arrow::datatypes::Schema::new(fields)
                );
                let table_name = stringify!($struct_name);
                let config = $crate::FletchConfig::init(workspace, table_name, schema.clone()).await?;
                let sink = $crate::BackgroundSink::spawn(config, schema.clone())?;
                Ok(Self {
                    sink,
                    schema,
                    timestamps: arrow::array::Int64Builder::with_capacity(10_000),
                    run_id: run_id.to_string(), 
                    current_ts: None,
                    $( $field_name: (<$rust_type as $crate::FletchType>::new_builder(10_000), None), )*
                })
            }

            fn commit_pending_row(&mut self) -> anyhow::Result<()> {
                if let Some(ts) = self.current_ts {
                    self.timestamps.append_value(ts);
                    $(
                        <$rust_type as $crate::FletchType>::append(&mut self.$field_name.0, self.$field_name.1.take());
                    )*
                }
                Ok(())
            }

            fn flush_batch(&mut self) -> anyhow::Result<()> {
                self.commit_pending_row()?;
                self.current_ts = None;
                if arrow::array::ArrayBuilder::is_empty(&self.timestamps) { return Ok(()); }
                let ts_array = std::sync::Arc::new(self.timestamps.finish()) as arrow::array::ArrayRef;
                let num_rows = ts_array.len();
                let mut run_id_builder = arrow::array::StringDictionaryBuilder::<arrow::datatypes::Int32Type>::new();
                for _ in 0..num_rows {
                    run_id_builder.append_value(&self.run_id);
                }
                let run_id_array = std::sync::Arc::new(run_id_builder.finish()) as arrow::array::ArrayRef;
                $(
                    let $field_name = <$rust_type as $crate::FletchType>::finish(&mut self.$field_name.0);
                )*
                let raw_batch = arrow::record_batch::RecordBatch::try_new(
                    self.schema.clone(),
                    vec![ts_array, run_id_array, $( $field_name, )*]
                )?;
                let sort_options = arrow::compute::SortOptions {
                    descending: false,
                    nulls_first: false,
                };
                let sort_column = arrow::compute::SortColumn {
                    values: raw_batch.column(0).clone(),
                    options: Some(sort_options),
                };
                let sorted_indices = arrow::compute::lexsort_to_indices(&[sort_column], None)?;
                let sorted_columns = raw_batch
                    .columns()
                    .iter()
                    .map(|c| arrow::compute::take(c.as_ref(), &sorted_indices, None))
                    .collect::<Result<Vec<_>, _>>()?;
                let sorted_batch = arrow::record_batch::RecordBatch::try_new(self.schema.clone(), sorted_columns)?;
                self.sink.write_batch(sorted_batch)?;
                Ok(())
            }

            $(
                pub fn $field_name(&mut self, ts: i64, value: $rust_type) -> anyhow::Result<()> {
                    if let Some(current) = self.current_ts {
                        if current != ts {
                            self.commit_pending_row()?;
                            self.current_ts = Some(ts);
                        }
                    } else {
                        self.current_ts = Some(ts);
                    }
                    self.$field_name.1 = Some(value);
                    if arrow::array::ArrayBuilder::len(&self.timestamps) >= 10_000 {
                        self.flush_batch()?;
                    }
                    Ok(())
                }
            )*

            pub fn close(mut self) -> anyhow::Result<()> {
                self.flush_batch()?;
                self.sink.close()?;
                Ok(())
            }
        }
    };
}