use std::collections::HashMap;
use tokio;
use std::sync::Arc;
use deltalake::protocol::SaveMode;
use deltalake::kernel::StructField;
use deltalake::arrow::datatypes::FieldRef;
use deltalake::{open_table_with_storage_options, DeltaOps, TableProperty};
use serde::{Serialize, Deserialize};
use serde_arrow::schema::{SchemaLike, TracingOptions};

#[derive(Serialize, Deserialize)]
struct R {
    num: i32,
    letter: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {

    // Specify any additional options as needed
    let mut options = HashMap::new();
    options.insert("aws_conditional_put".to_string(), "etag".to_string());

    // Specify the correct path to your Delta table
    let path = "s3://datafusion-test/test.dl";
    deltalake::aws::register_handlers(None);
    let arc_fields = Vec::<FieldRef>::from_type::<R>(TracingOptions::default())?;
    let fields: Vec<StructField> = arc_fields.clone()
        .into_iter()
        .map(|arc| (&*arc).try_into().expect("blah"))
        .collect();

    println!("Gen Fields {:?}!", fields);
    // Open the Delta table using the object store and path
    println!("Opening table!");
    let table_open = open_table_with_storage_options(path, options).await;
    let table = match table_open {
        Ok(t) => t,
        Err(e) => {
            println!("Error {e:?}");
            let delta_ops = DeltaOps::try_from_uri(path).await?;
            let table = delta_ops
                .create()
                .with_table_name("some-table")
                .with_save_mode(SaveMode::Overwrite)
                .with_configuration_property(TableProperty::MinReaderVersion, Some("3"))
                .with_configuration_property(TableProperty::MinWriterVersion, Some("7"))
                .with_columns(
                    fields,
                )
                .await?;

            table
        }

    };

    let delta_ops = DeltaOps::try_from_uri(path).await?;

    let records = vec![
        R { num: 4, letter: "d".to_string() },
        R { num: 5, letter: "e".to_string() },
        R { num: 6, letter: "f".to_string() },
    ];
    let batch = serde_arrow::to_record_batch(&arc_fields, &records)?;

    // Further processing on the Delta table...
    println!("Delta table opened successfully!");
    println!("Schema {:?}", table.metadata());
    use deltalake::datafusion::prelude::SessionContext;
    let ctx = SessionContext::new();
    ctx.register_table("some_table", Arc::new(table.clone()))?;
    ctx.sql("SELECT * FROM some_table;").await?.show().await?;

    use deltalake::datafusion::logical_expr as expr;
    let dataframe = ctx.table( "some_table").await?;
    let df = dataframe
        .filter(expr::col("num").eq(expr::lit(1)))?
        .select(vec![expr::col("num")])?;
    df.show().await?;

    delta_ops.write([batch]).await?;
    DeltaOps::try_from_uri(path).await?.optimize().with_type(deltalake::operations::optimize::OptimizeType::Compact).await?;
    DeltaOps::try_from_uri(path).await?.vacuum().await?;
    ctx.sql("SELECT count(*) FROM some_table;").await?.show().await?;

    Ok(())
}
