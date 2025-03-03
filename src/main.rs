use std::collections::HashMap;
use tokio;
use std::sync::Arc;
use deltalake::protocol::SaveMode;
use deltalake::kernel::StructField;
use deltalake::arrow::datatypes::FieldRef;
use deltalake::{open_table_with_storage_options, DeltaOps, TableProperty};
use serde::{Serialize, Deserialize};
use serde_arrow::schema::{SchemaLike, TracingOptions};

#[derive(Serialize, Deserialize, Debug)]
struct R {
    num: i32,
    letter: String,
    maybe: Option<i32>,
}

#[derive(Serialize, Deserialize, Debug)]
struct AddColumn {
    maybe: Option<i32>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {

    // Specify any additional options as needed
    let mut options = HashMap::new();
    options.insert("aws_conditional_put".to_string(), "etag".to_string());

    // Specify the correct path to your Delta table
    let path = "s3://datafusion-test/test.dl";
    //let path = "memory://test";
    //let path = "test/test.dl";
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

    if table.schema().expect("schema").fields.contains_key("maybe") {
        println!("Has `maybe` column");

    } else {
        println!("DOES NOT have `maybe` column");
        let delta_ops_migrate_column = DeltaOps::try_from_uri(path).await?;

        let arc_fields = Vec::<FieldRef>::from_type::<AddColumn>(TracingOptions::default())?;
        let fields: Vec<StructField> = arc_fields.clone()
        .into_iter()
        .map(|arc| (&*arc).try_into().expect("blah"))
        .collect();
        delta_ops_migrate_column.add_columns().with_fields(fields).await?;

    }

    let delta_ops = DeltaOps::try_from_uri(path).await?;

    let records = vec![
        R { num: 4, letter: "d".to_string(), maybe: Some(1) },
        R { num: 5, letter: "e".to_string(), maybe: None },
        R { num: 6, letter: "f".to_string(), maybe: Some(3) },
    ];
    let df_records = vec![
        R { num: 7, letter: "g".to_string(), maybe: Some(10) },
        R { num: 8, letter: "h".to_string(), maybe: None },
        R { num: 9, letter: "i".to_string(), maybe: Some(30) },
    ];
    let batch = serde_arrow::to_record_batch(&arc_fields, &records)?;
    let batch_to_merge = serde_arrow::to_record_batch(&arc_fields, &df_records)?;
    use deltalake::datafusion::prelude::SessionContext;
    let ctx = SessionContext::new();
    let df_to_merge = ctx.read_batch(batch_to_merge)?;

    // Further processing on the Delta table...
    println!("Delta table opened successfully!");
    println!("Schema {:?}", table.metadata());
    let ctx = SessionContext::new();
    ctx.register_table("some_table", Arc::new(table.clone()))?;
    ctx.sql("SELECT * FROM some_table;").await?.show().await?;

    use deltalake::datafusion::logical_expr as expr;
    use std::time::Instant;
    let dataframe = ctx.table("some_table").await?;
    let df = dataframe
        .filter(
            expr::or(
                expr::col("num").eq(expr::lit(1)),
                expr::col("num").eq(expr::lit(4)),
            )
        )?;
        //.select(vec![expr::col("num")])?;
    let now = Instant::now();
    let items: Result<Vec<Vec<R>>, serde_arrow::Error> = df.collect().await?.iter().map(|rb| serde_arrow::from_record_batch(&rb)).collect();
    for i in items?.iter().flatten() {
        println!("Record {i:?}")
    }
    let elapsed = now.elapsed();
    println!("Elapsed: {:.2?}", elapsed);
    //let count = table.state.expect("").files_count();
    //println!("Files count: {count}");

    let (table, metrics) = DeltaOps(table)
        .merge(df_to_merge,
               expr::col("target.num").eq(expr::col("source.num")))
        .with_source_alias("source")
        .with_target_alias("target")
        .when_not_matched_insert(|insert| {
        insert
            .set("num", expr::col("source.num"))
            .set("letter", expr::col("source.letter"))
            .set("maybe", expr::col("source.maybe"))
            })?
        .when_matched_update(|update| {
        update
            .update("letter", expr::lit("x"))
    })?

    .await?;
    println!("Metrics {metrics:?}");

    delta_ops.write([batch]).await?;
    DeltaOps::try_from_uri(path).await?.optimize().with_type(deltalake::operations::optimize::OptimizeType::Compact).await?;
    DeltaOps::try_from_uri(path).await?.vacuum().await?;
    ctx.sql("SELECT count(*) FROM some_table;").await?.show().await?;

    Ok(())
}
