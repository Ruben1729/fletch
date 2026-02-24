use std::sync::Arc;
use arrow::array::{ArrayRef, Float64Builder, Int32Builder};
use arrow::datatypes::DataType;

pub trait FletchType: Sized {
    type Builder;
    fn data_type() -> DataType;
    fn new_builder(capacity: usize) -> Self::Builder;
    fn append(builder: &mut Self::Builder, value: Option<Self>);
    fn finish(builder: &mut Self::Builder) -> ArrayRef;
}

impl FletchType for f64 {
    type Builder = Float64Builder;
    fn data_type() -> DataType { DataType::Float64 }
    fn new_builder(capacity: usize) -> Self::Builder { Float64Builder::with_capacity(capacity) }
    fn append(builder: &mut Self::Builder, value: Option<Self>) { builder.append_option(value); }
    fn finish(builder: &mut Self::Builder) -> ArrayRef { Arc::new(builder.finish()) }
}

impl FletchType for i32 {
    type Builder = Int32Builder;
    fn data_type() -> DataType { DataType::Int32 }
    fn new_builder(capacity: usize) -> Self::Builder { Int32Builder::with_capacity(capacity) }
    fn append(builder: &mut Self::Builder, value: Option<Self>) { builder.append_option(value); }
    fn finish(builder: &mut Self::Builder) -> ArrayRef { Arc::new(builder.finish()) }
}