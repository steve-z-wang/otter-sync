pub mod common05;
use axton_client::*;
use axton_sqlite::SqliteStore;
use common05::*;
use serde_json::{Value, json};

#[test]
fn query_normalizes_filters_orders_nulls_and_resolves_relationships() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = Client::open05(
        SqliteStore::open(dir.path().join("db")).unwrap(),
        family_schema(),
        "User:u",
    )
    .unwrap();
    c.transaction(|tx| {
        tx.direct(create("Book", "b", json!({"title":"Book"})))?;
        for (id, text) in [("c1", "Z"), ("c2", "A"), ("c3", "A")] {
            tx.direct(create(
                "Comment",
                id,
                json!({"bookId":if id=="c3"{"other"}else{"b"},"text":text}),
            ))?;
        }
        Ok(())
    })
    .unwrap();
    let spec: QuerySpec = serde_json::from_value(
        json!({"orderBy":[{"field":"text","direction":"ascending"}],"limit":2}),
    )
    .unwrap();
    let rows = c.query_spec("Comment", &spec).unwrap();
    assert_eq!(
        rows.iter()
            .map(|r| r["id"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["c2", "c3"]
    );
    let key = family_schema()
        .record_key("Comment", &json!({"id":"c1"}))
        .unwrap();
    assert_eq!(c.related(&key, "book").unwrap().unwrap()["id"], "b");
    let book = family_schema()
        .record_key("Book", &json!({"id":"b"}))
        .unwrap();
    assert_eq!(c.referencing(&book, "Comment", "book").unwrap().len(), 2);
    assert!(c.query("Comment", &json!({"missing":1})).is_err());
    assert_eq!(c.query("Comment", &json!({"bookId":"b"})).unwrap().len(), 2);
    c.transaction(|tx| {
        assert_eq!(tx.query("Comment", &json!({}))?.len(), 3);
        tx.direct(create("Comment", "c4", json!({"bookId":"b","text":"Q"})))?;
        assert_eq!(
            tx.query("Comment", &json!({}))?.len(),
            4,
            "reads inside the transaction see its writes"
        );
        Ok(())
    })
    .unwrap();
}

#[test]
fn readonly_sql_sees_optimistic_rows_and_refuses_write_statements() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    seed(&mut c, "A");
    c.transaction(|tx| {
        tx.submit_mutation05("Edit", 1, json!({"entry":{"id":"e","text":"B"}}), vec![])?;
        Ok(())
    })
    .unwrap();
    assert_eq!(
        c.read_sql("SELECT id,text FROM Entry WHERE id=?", &[json!("e")])
            .unwrap(),
        vec![json!({"id":"e","text":"B"})]
    );
    assert!(c.read_sql("DELETE FROM Entry RETURNING id", &[]).is_err());
    assert!(c.read_sql("PRAGMA user_version=10", &[]).is_err());
    assert!(
        c.read_sql("SELECT id, id FROM Entry", &[]).is_err(),
        "duplicate column names need aliases"
    );
    c.begin_session().unwrap();
    c.session(|tx| tx.direct(update("C"))).unwrap();
    assert_eq!(
        c.session_sql("SELECT text FROM Entry", &[]).unwrap(),
        vec![json!({"text":"C"})]
    );
    assert_eq!(
        c.read_sql("SELECT text FROM Entry", &[]).unwrap(),
        vec![json!({"text":"B"})]
    );
    c.rollback_session().unwrap();
}

/// The tables a watched statement reads are SQLite's answer, never the
/// application's: a join names each Model table once, a `count(*)` or a
/// differently cased name resolves to the stored table, and a CTE is not a
/// table. Only one read-only `SELECT` (or `WITH … SELECT`) that reads no
/// engine table is accepted, and preparing it changes nothing about later
/// reads and writes ([#184](https://github.com/zanminwang/axton/issues/184)).
#[test]
fn sql_tables_are_the_model_tables_a_select_reads() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = Client::open05(
        SqliteStore::open(dir.path().join("db")).unwrap(),
        family_schema(),
        "User:u",
    )
    .unwrap();
    let tables = |c: &mut Client<SqliteStore>, sql: &str| {
        c.sql_tables(sql)
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
            .into_iter()
            .collect::<Vec<_>>()
    };
    assert_eq!(
        tables(
            &mut c,
            "SELECT b.title, c.text FROM Book b JOIN Comment c ON c.bookId = b.id"
        ),
        ["Book", "Comment"]
    );
    assert_eq!(tables(&mut c, "SELECT count(*) AS n FROM book"), ["Book"]);
    assert_eq!(
        tables(
            &mut c,
            "WITH titled AS (SELECT id FROM \"Book\") SELECT count(*) AS n FROM titled"
        ),
        ["Book"]
    );
    assert_eq!(
        tables(
            &mut c,
            "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 3) \
             SELECT i, (SELECT count(*) FROM Comment) AS c FROM n"
        ),
        ["Comment"]
    );
    assert_eq!(tables(&mut c, "SELECT 1 AS one"), Vec::<String>::new());
    for refused in [
        "INSERT INTO Book (id, title) VALUES ('b', 't')",
        "UPDATE Book SET title = 'x'",
        "DELETE FROM Book RETURNING id",
        "CREATE TABLE Other (id TEXT)",
        "PRAGMA table_info(Book)",
        "EXPLAIN SELECT id FROM Book",
        "EXPLAIN QUERY PLAN SELECT id FROM Book",
        "SELECT 1 AS one; SELECT 2 AS two",
        "SELECT model FROM axton_record",
        "SELECT count(*) AS n FROM AXTON_BEFORE_Book",
        "SELECT id FROM Book WHERE id IN (SELECT identity FROM axton_record)",
        "SELECT id FROM Missing",
    ] {
        assert!(c.sql_tables(refused).is_err(), "{refused} is refused");
    }
    // The authorizer lived for one prepare only.
    c.transaction(|tx| {
        tx.direct(create("Book", "b", json!({"title":"Title"})))?;
        Ok(())
    })
    .unwrap();
    assert_eq!(
        c.read_sql("SELECT count(*) AS n FROM axton_authority", &[])
            .unwrap(),
        vec![json!({"n":1})],
        "direct write evidence is exposed through the derived read view"
    );
    assert_eq!(
        c.read_sql("SELECT title FROM Book", &[]).unwrap(),
        vec![json!({"title":"Title"})]
    );
}

#[test]
fn unsupported_filter_and_order_shapes_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let schema = Schema::from_value(json!({"enums":[{"name":"Mood","values":["calm","busy"]}],"models":[{
        "name":"Note","identity":["id"],"fields":[
            {"name":"id","nullable":false,"type":{"kind":"scalar","name":"string"}},
            {"name":"mood","nullable":false,"type":{"kind":"enum","name":"Mood"}},
            {"name":"tags","nullable":false,"type":{"kind":"list","element":{"kind":"scalar","name":"string"}}}
        ]}]}))
    .unwrap();
    let mut c = Client::open05(
        SqliteStore::open(dir.path().join("db")).unwrap(),
        schema,
        "User:u",
    )
    .unwrap();
    c.transaction(|tx| tx.direct(create("Note", "n", json!({"mood":"calm","tags":["a"]}))))
        .unwrap();
    let mut query = |spec: Value| {
        let spec: QuerySpec = serde_json::from_value(spec).unwrap();
        c.query_spec("Note", &spec)
    };
    assert_eq!(query(json!({"filter":{"mood":"calm"}})).unwrap().len(), 1);
    assert_eq!(
        query(json!({"orderBy":[{"field":"id","direction":"ascending"}]}))
            .unwrap()
            .len(),
        1
    );
    for (spec, message) in [
        (
            json!({"filter":{"tags":["a"]}}),
            "list predicates unsupported",
        ),
        (json!({"filter":{"missing":1}}), "unknown query field"),
        (json!({"filter":{"mood":"angry"}}), "invalid enum value"),
        (
            json!({"orderBy":[{"field":"mood","direction":"ascending"}]}),
            "ordering requires scalar field",
        ),
        (
            json!({"orderBy":[{"field":"tags","direction":"ascending"}]}),
            "ordering requires scalar field",
        ),
        (
            json!({"orderBy":[{"field":"missing","direction":"ascending"}]}),
            "unknown query field",
        ),
    ] {
        let err = query(spec.clone()).unwrap_err();
        assert!(err.to_string().contains(message), "{spec}: {err}");
    }
}
