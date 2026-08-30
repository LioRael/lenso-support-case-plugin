use lenso_postgres_kit::OwnedPostgres;
use sqlx::{AssertSqlSafe, Executor as _};
use uuid::Uuid;

use crate::{SupportCaseOperator, schema, storage};

#[tokio::test]
async fn restart_replay_and_revision_conflicts_are_postgres_durable() {
    let Ok(database_url) = std::env::var("LENSO_SUPPORT_CASE_TEST_DATABASE_URL") else {
        return;
    };
    let schema_name = format!("support_case_test_{}", Uuid::new_v4().simple());
    SupportCaseOperator::setup(&database_url, &schema_name)
        .await
        .unwrap();
    let postgres = OwnedPostgres::prepare(
        &database_url,
        schema::schema_plan(schema_name.clone()).unwrap(),
    )
    .await
    .unwrap();

    let created = storage::create_case(
        &postgres,
        "support-api",
        "create-1",
        &[1, 2, 3],
        "org_1",
        "usr_requester",
        "Cannot sign in",
        "The login flow rejects my passkey.",
        "high",
    )
    .await
    .unwrap()
    .unwrap();
    postgres.pool().close().await;

    let restarted = OwnedPostgres::prepare(
        &database_url,
        schema::schema_plan(schema_name.clone()).unwrap(),
    )
    .await
    .unwrap();
    let replayed = storage::create_case(
        &restarted,
        "support-api",
        "create-1",
        &[1, 2, 3],
        "org_1",
        "usr_requester",
        "Cannot sign in",
        "The login flow rejects my passkey.",
        "high",
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(replayed, created);

    let first = storage::update_case(
        &restarted,
        "support-api",
        "update-1",
        &[4],
        "org_1",
        created.case_id,
        "usr_agent",
        1,
        Some("Cannot sign in with passkey"),
        None,
        None,
    );
    let second = storage::update_case(
        &restarted,
        "support-api",
        "update-2",
        &[5],
        "org_1",
        created.case_id,
        "usr_agent",
        1,
        None,
        None,
        Some("urgent"),
    );
    let (first, second) = tokio::join!(first, second);
    let outcomes = [first.unwrap(), second.unwrap()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, Ok(record) if record.revision == 2))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, Err(storage::DomainFailure::RevisionConflict)))
            .count(),
        1
    );

    restarted.pool().close().await;
    let cleanup = sqlx::PgPool::connect(&database_url).await.unwrap();
    cleanup
        .execute(AssertSqlSafe(format!(
            "DROP SCHEMA \"{schema_name}\" CASCADE"
        )))
        .await
        .unwrap();
    cleanup.close().await;
}
