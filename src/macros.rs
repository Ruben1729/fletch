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
            current_ts: Option<i64>,
            $( $field_name: (<$rust_type as $crate::FletchType>::Builder, Option<$rust_type>), )*
        }

        impl $struct_name {
            pub async fn try_new(uri: &str, run_id: &str) -> anyhow::Result<Self> {
                let mut fields = vec![
                    arrow::datatypes::Field::new("timestamp_ns", arrow::datatypes::DataType::Int64, false),
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
                let config = $crate::FletchConfig::init(uri, table_name, run_id, schema.clone()).await?;
                let sink = $crate::BackgroundSink::spawn(config, schema.clone())?;
                Ok(Self {
                    sink,
                    schema,
                    timestamps: arrow::array::Int64Builder::with_capacity(10_000),
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
                $(
                    let $field_name = <$rust_type as $crate::FletchType>::finish(&mut self.$field_name.0);
                )*
                let batch = arrow::record_batch::RecordBatch::try_new(
                    self.schema.clone(),
                    vec![ts_array, $( $field_name, )*]
                )?;
                self.sink.write_batch(batch)?;
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