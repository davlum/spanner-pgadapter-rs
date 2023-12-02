use tokio_postgres::{Error, NoTls};

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Connect to the database.
    let (client, connection) = tokio_postgres::Config::new()
        .dbname("test-database")
        .host("localhost")
        .connect(NoTls)
        .await
        .unwrap();

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    let task = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Now we can execute a simple statement that just returns its parameter.
    let rows = client.query("SELECT $1::TEXT", &[&"hello world"]).await?;

    // And then check that we got back the same string we sent over.
    let value: &str = rows[0].get(0);
    println!("VALUE IS {value}");
    assert_eq!(value, "hello world");

    client
        .batch_execute(
            "
    DROP TABLE IF EXISTS person
",
        )
        .await?;

    // Does not accept SERIAL type, which is auto incrementing.
    // Supported types:
    // https://cloud.google.com/spanner/docs/reference/postgresql/data-types
    //
    // Spanner does not support auto incrementing primary keys for the same reason
    // that object store partitions should start with non-incrementing keys, which is hotspots
    // on the partitions:
    // https://cloud.google.com/spanner/docs/schema-and-data-model#choosing_a_primary_key
    //
    // Spanner does not support DEFAULT values like Postgres. In the case of Spanner, Default
    // will be interpreted as null.
    //
    // INT is normally a 4 byte integer in Postgres, 8 byte Int here tho
    client
        .batch_execute(
            "
    CREATE TABLE person (
        name    TEXT PRIMARY KEY,
        data    BYTEA
    )
",
        )
        .await?;

    let name = "Ferris";
    client
        .execute(
            "INSERT INTO person (name) VALUES ($1)",
            &[&name],
        )
        .await?;

    for row in client
        .query("SELECT name, data FROM person", &[])
        .await?
    {
        let name: &str = row.get(0);
        let data: Option<&[u8]> = row.get(1);

        println!("found person: {} {:?}", name, data);
    }

    let data: Option<Vec<u8>> = Some(vec![1]);

    client
        .execute(
            "UPDATE person SET data = $1 WHERE name = $2",
            &[&data, &name],
        )
        .await?;

    for row in client
        .query("SELECT name, data FROM person", &[])
        .await?
    {
        let name: &str = row.get(0);
        let data: Option<&[u8]> = row.get(1);

        println!("found person: {} {:?}", name, data);
    }

    // I added this line. Less logs on close, but still a lot
    task.abort_handle().abort();

    Ok(())
}
