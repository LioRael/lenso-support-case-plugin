use lenso_postgres_kit::OwnedPostgres;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use sqlx::{Postgres, Row, Transaction, types::Json};
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct CaseRecord {
    pub(crate) case_id: Uuid,
    pub(crate) identifier: String,
    pub(crate) organization_id: String,
    pub(crate) requester_subject: String,
    pub(crate) creator_subject: String,
    pub(crate) title: String,
    pub(crate) description: String,
    pub(crate) priority: String,
    pub(crate) state: String,
    pub(crate) assignee_subject: Option<String>,
    #[serde(with = "decimal_i64")]
    pub(crate) revision: i64,
    #[serde(with = "time::serde::rfc3339")]
    pub(crate) created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub(crate) updated_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339::option")]
    pub(crate) resolved_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub(crate) closed_at: Option<OffsetDateTime>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct MessageRecord {
    pub(crate) message_id: Uuid,
    pub(crate) case_id: Uuid,
    pub(crate) visibility: String,
    pub(crate) author_subject: String,
    pub(crate) body: String,
    #[serde(with = "time::serde::rfc3339")]
    pub(crate) created_at: OffsetDateTime,
    #[serde(with = "decimal_i64")]
    pub(crate) case_revision: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CaseCursor {
    pub(crate) updated_at: OffsetDateTime,
    pub(crate) case_id: Uuid,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MessageCursor {
    pub(crate) created_at: OffsetDateTime,
    pub(crate) message_id: Uuid,
}

#[derive(Clone, Debug)]
pub(crate) struct CaseFilters<'a> {
    pub(crate) organization_id: &'a str,
    pub(crate) state: Option<&'a str>,
    pub(crate) assignee_subject: Option<&'a str>,
    pub(crate) requester_subject: Option<&'a str>,
    pub(crate) cursor: Option<&'a CaseCursor>,
    pub(crate) actor: &'a str,
    pub(crate) can_read_all: bool,
    pub(crate) limit: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DomainFailure {
    Forbidden,
    CaseNotFound,
    RevisionConflict,
    IdempotencyConflict,
    InvalidTransition,
    RetentionConflict,
}

#[derive(Debug, Error)]
pub(crate) enum StorageError {
    #[error("PostgreSQL operation `{operation}` failed")]
    Database {
        operation: &'static str,
        #[source]
        source: sqlx::Error,
    },
    #[error("stored Support Case data is invalid: {detail}")]
    InvalidStoredData { detail: String },
    #[error("Support Case command response serialization failed")]
    Serialization(#[from] serde_json::Error),
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn create_case(
    postgres: &OwnedPostgres,
    caller: &str,
    idempotency_key: &str,
    request_hash: &[u8],
    organization_id: &str,
    actor: &str,
    title: &str,
    description: &str,
    priority: &str,
) -> Result<Result<CaseRecord, DomainFailure>, StorageError> {
    let mut transaction = begin(postgres, "begin case creation").await?;
    match command_replay::<CaseRecord>(
        &mut transaction,
        caller,
        idempotency_key,
        "create_case",
        request_hash,
    )
    .await?
    {
        Ok(Some(replay)) => {
            commit(transaction, "commit case creation replay").await?;
            return Ok(Ok(replay));
        }
        Ok(None) => {}
        Err(failure) => return Ok(Err(failure)),
    }

    let sequence = sqlx::query(
        "INSERT INTO support_case_sequences(organization_id,next_number) VALUES($1,2) ON CONFLICT(organization_id) DO UPDATE SET next_number=support_case_sequences.next_number+1 RETURNING next_number-1 AS allocated",
    )
    .bind(organization_id)
    .fetch_one(&mut *transaction)
    .await
    .map_err(|source| database("allocate case identifier", source))?;
    let number: i64 = sequence
        .try_get("allocated")
        .map_err(|source| database("decode case identifier", source))?;
    if number <= 0 {
        return Err(StorageError::InvalidStoredData {
            detail: "case sequence is not positive".to_owned(),
        });
    }
    let case_id = Uuid::new_v4();
    let identifier = format!("SUP-{number}");
    let row = sqlx::query(
        "INSERT INTO support_cases(case_id,organization_id,identifier,requester_subject,creator_subject,title,description,priority,state,revision) VALUES($1,$2,$3,$4,$4,$5,$6,$7,'open',1) RETURNING *",
    )
    .bind(case_id)
    .bind(organization_id)
    .bind(&identifier)
    .bind(actor)
    .bind(title)
    .bind(description)
    .bind(priority)
    .fetch_one(&mut *transaction)
    .await
    .map_err(|source| database("insert support case", source))?;
    let record = decode_case(&row)?;
    insert_activity(
        &mut transaction,
        organization_id,
        case_id,
        "case.created",
        actor,
        1,
        json!({"identifier": identifier}),
    )
    .await?;
    save_command(
        &mut transaction,
        caller,
        idempotency_key,
        organization_id,
        case_id,
        "create_case",
        request_hash,
        &record,
    )
    .await?;
    commit(transaction, "commit case creation").await?;
    Ok(Ok(record))
}

pub(crate) async fn get_case(
    postgres: &OwnedPostgres,
    organization_id: &str,
    case_ref: &str,
    actor: &str,
    can_read_all: bool,
) -> Result<Option<CaseRecord>, StorageError> {
    let parsed = Uuid::parse_str(case_ref).ok();
    let row = sqlx::query(
        "SELECT * FROM support_cases WHERE organization_id=$1 AND (($2::uuid IS NOT NULL AND case_id=$2) OR ($2::uuid IS NULL AND identifier=$3)) AND ($4 OR requester_subject=$5 OR creator_subject=$5 OR assignee_subject=$5)",
    )
    .bind(organization_id)
    .bind(parsed)
    .bind(case_ref)
    .bind(can_read_all)
    .bind(actor)
    .fetch_optional(postgres.pool())
    .await
    .map_err(|source| database("get support case", source))?;
    row.as_ref().map(decode_case).transpose()
}

pub(crate) async fn list_cases(
    postgres: &OwnedPostgres,
    filters: &CaseFilters<'_>,
) -> Result<Vec<CaseRecord>, StorageError> {
    let cursor_time = filters.cursor.map(|cursor| cursor.updated_at);
    let cursor_id = filters.cursor.map(|cursor| cursor.case_id);
    let rows = sqlx::query(
        "SELECT * FROM support_cases WHERE organization_id=$1 AND ($2::text IS NULL OR state=$2) AND ($3::text IS NULL OR assignee_subject=$3) AND ($4::text IS NULL OR requester_subject=$4) AND ($5::timestamptz IS NULL OR (updated_at,case_id)<($5,$6)) AND ($7 OR requester_subject=$8 OR creator_subject=$8 OR assignee_subject=$8) ORDER BY updated_at DESC,case_id DESC LIMIT $9",
    )
    .bind(filters.organization_id)
    .bind(filters.state)
    .bind(filters.assignee_subject)
    .bind(filters.requester_subject)
    .bind(cursor_time)
    .bind(cursor_id)
    .bind(filters.can_read_all)
    .bind(filters.actor)
    .bind(filters.limit)
    .fetch_all(postgres.pool())
    .await
    .map_err(|source| database("list support cases", source))?;
    rows.iter().map(decode_case).collect()
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn update_case(
    postgres: &OwnedPostgres,
    caller: &str,
    idempotency_key: &str,
    request_hash: &[u8],
    organization_id: &str,
    case_id: Uuid,
    actor: &str,
    expected_revision: i64,
    title: Option<&str>,
    description: Option<&str>,
    priority: Option<&str>,
) -> Result<Result<CaseRecord, DomainFailure>, StorageError> {
    let mut transaction = begin(postgres, "begin case update").await?;
    match command_replay::<CaseRecord>(
        &mut transaction,
        caller,
        idempotency_key,
        "update_case",
        request_hash,
    )
    .await?
    {
        Ok(Some(replay)) => {
            commit(transaction, "commit case update replay").await?;
            return Ok(Ok(replay));
        }
        Ok(None) => {}
        Err(failure) => return Ok(Err(failure)),
    }
    let Some(current) = locked_case(&mut transaction, organization_id, case_id).await? else {
        return Ok(Err(DomainFailure::CaseNotFound));
    };
    if current.revision != expected_revision {
        return Ok(Err(DomainFailure::RevisionConflict));
    }
    let row = sqlx::query(
        "UPDATE support_cases SET title=COALESCE($3,title),description=COALESCE($4,description),priority=COALESCE($5,priority),revision=revision+1,updated_at=CURRENT_TIMESTAMP WHERE organization_id=$1 AND case_id=$2 RETURNING *",
    )
    .bind(organization_id)
    .bind(case_id)
    .bind(title)
    .bind(description)
    .bind(priority)
    .fetch_one(&mut *transaction)
    .await
    .map_err(|source| database("update support case", source))?;
    let record = decode_case(&row)?;
    insert_activity(
        &mut transaction,
        organization_id,
        case_id,
        "case.updated",
        actor,
        record.revision,
        json!({}),
    )
    .await?;
    save_command(
        &mut transaction,
        caller,
        idempotency_key,
        organization_id,
        case_id,
        "update_case",
        request_hash,
        &record,
    )
    .await?;
    commit(transaction, "commit case update").await?;
    Ok(Ok(record))
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn assign_case(
    postgres: &OwnedPostgres,
    caller: &str,
    idempotency_key: &str,
    request_hash: &[u8],
    organization_id: &str,
    case_id: Uuid,
    actor: &str,
    expected_revision: i64,
    assignee_subject: Option<&str>,
) -> Result<Result<CaseRecord, DomainFailure>, StorageError> {
    let mut transaction = begin(postgres, "begin case assignment").await?;
    match command_replay::<CaseRecord>(
        &mut transaction,
        caller,
        idempotency_key,
        "assign_case",
        request_hash,
    )
    .await?
    {
        Ok(Some(replay)) => {
            commit(transaction, "commit case assignment replay").await?;
            return Ok(Ok(replay));
        }
        Ok(None) => {}
        Err(failure) => return Ok(Err(failure)),
    }
    let Some(current) = locked_case(&mut transaction, organization_id, case_id).await? else {
        return Ok(Err(DomainFailure::CaseNotFound));
    };
    if current.revision != expected_revision {
        return Ok(Err(DomainFailure::RevisionConflict));
    }
    let row = sqlx::query("UPDATE support_cases SET assignee_subject=$3,revision=revision+1,updated_at=CURRENT_TIMESTAMP WHERE organization_id=$1 AND case_id=$2 RETURNING *")
        .bind(organization_id).bind(case_id).bind(assignee_subject)
        .fetch_one(&mut *transaction).await.map_err(|source| database("assign support case", source))?;
    let record = decode_case(&row)?;
    insert_activity(
        &mut transaction,
        organization_id,
        case_id,
        "case.assigned",
        actor,
        record.revision,
        json!({"assignee_subject": assignee_subject}),
    )
    .await?;
    save_command(
        &mut transaction,
        caller,
        idempotency_key,
        organization_id,
        case_id,
        "assign_case",
        request_hash,
        &record,
    )
    .await?;
    commit(transaction, "commit case assignment").await?;
    Ok(Ok(record))
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn transition_case(
    postgres: &OwnedPostgres,
    caller: &str,
    idempotency_key: &str,
    request_hash: &[u8],
    organization_id: &str,
    case_id: Uuid,
    actor: &str,
    expected_revision: i64,
    target_state: &str,
    reason: Option<&str>,
) -> Result<Result<CaseRecord, DomainFailure>, StorageError> {
    let mut transaction = begin(postgres, "begin case transition").await?;
    match command_replay::<CaseRecord>(
        &mut transaction,
        caller,
        idempotency_key,
        "transition_case",
        request_hash,
    )
    .await?
    {
        Ok(Some(replay)) => {
            commit(transaction, "commit case transition replay").await?;
            return Ok(Ok(replay));
        }
        Ok(None) => {}
        Err(failure) => return Ok(Err(failure)),
    }
    let Some(current) = locked_case(&mut transaction, organization_id, case_id).await? else {
        return Ok(Err(DomainFailure::CaseNotFound));
    };
    if current.revision != expected_revision {
        return Ok(Err(DomainFailure::RevisionConflict));
    }
    if !valid_transition(&current.state, target_state) {
        return Ok(Err(DomainFailure::InvalidTransition));
    }
    let row = sqlx::query(
        "UPDATE support_cases SET state=$3,resolved_at=CASE WHEN $3='resolved' THEN CURRENT_TIMESTAMP WHEN $3='in_progress' THEN NULL ELSE resolved_at END,closed_at=CASE WHEN $3='closed' THEN CURRENT_TIMESTAMP WHEN $3='in_progress' THEN NULL ELSE closed_at END,revision=revision+1,updated_at=CURRENT_TIMESTAMP WHERE organization_id=$1 AND case_id=$2 RETURNING *",
    )
    .bind(organization_id).bind(case_id).bind(target_state)
    .fetch_one(&mut *transaction).await.map_err(|source| database("transition support case", source))?;
    let record = decode_case(&row)?;
    insert_activity(
        &mut transaction,
        organization_id,
        case_id,
        "case.transitioned",
        actor,
        record.revision,
        json!({"from": current.state, "to": target_state, "reason": reason}),
    )
    .await?;
    save_command(
        &mut transaction,
        caller,
        idempotency_key,
        organization_id,
        case_id,
        "transition_case",
        request_hash,
        &record,
    )
    .await?;
    commit(transaction, "commit case transition").await?;
    Ok(Ok(record))
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn add_message(
    postgres: &OwnedPostgres,
    caller: &str,
    idempotency_key: &str,
    request_hash: &[u8],
    organization_id: &str,
    case_id: Uuid,
    actor: &str,
    can_comment_all: bool,
    expected_revision: i64,
    visibility: &str,
    body: &str,
) -> Result<Result<MessageRecord, DomainFailure>, StorageError> {
    let mut transaction = begin(postgres, "begin message creation").await?;
    match command_replay::<MessageRecord>(
        &mut transaction,
        caller,
        idempotency_key,
        "add_message",
        request_hash,
    )
    .await?
    {
        Ok(Some(replay)) => {
            commit(transaction, "commit message replay").await?;
            return Ok(Ok(replay));
        }
        Ok(None) => {}
        Err(failure) => return Ok(Err(failure)),
    }
    let Some(current) = locked_case(&mut transaction, organization_id, case_id).await? else {
        return Ok(Err(DomainFailure::CaseNotFound));
    };
    if !can_comment_all
        && current.requester_subject != actor
        && current.creator_subject != actor
        && current.assignee_subject.as_deref() != Some(actor)
    {
        return Ok(Err(DomainFailure::Forbidden));
    }
    if current.revision != expected_revision {
        return Ok(Err(DomainFailure::RevisionConflict));
    }
    let revision_row = sqlx::query("UPDATE support_cases SET revision=revision+1,updated_at=CURRENT_TIMESTAMP WHERE case_id=$1 RETURNING revision")
        .bind(case_id).fetch_one(&mut *transaction).await.map_err(|source| database("bump case revision for message", source))?;
    let revision: i64 = revision_row
        .try_get("revision")
        .map_err(|source| database("decode message case revision", source))?;
    let message_id = Uuid::new_v4();
    let row = sqlx::query("INSERT INTO support_case_messages(message_id,case_id,visibility,author_subject,body) VALUES($1,$2,$3,$4,$5) RETURNING *")
        .bind(message_id).bind(case_id).bind(visibility).bind(actor).bind(body)
        .fetch_one(&mut *transaction).await.map_err(|source| database("insert support case message", source))?;
    let mut record = decode_message(&row)?;
    record.case_revision = revision;
    insert_activity(
        &mut transaction,
        organization_id,
        case_id,
        "message.added",
        actor,
        revision,
        json!({"message_id": message_id, "visibility": visibility}),
    )
    .await?;
    save_command(
        &mut transaction,
        caller,
        idempotency_key,
        organization_id,
        case_id,
        "add_message",
        request_hash,
        &record,
    )
    .await?;
    commit(transaction, "commit message creation").await?;
    Ok(Ok(record))
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn list_messages(
    postgres: &OwnedPostgres,
    organization_id: &str,
    case_id: Uuid,
    actor: &str,
    can_read_all: bool,
    can_read_internal: bool,
    cursor: Option<&MessageCursor>,
    limit: i64,
) -> Result<Option<Vec<MessageRecord>>, StorageError> {
    let visible = sqlx::query("SELECT 1 FROM support_cases WHERE organization_id=$1 AND case_id=$2 AND ($3 OR requester_subject=$4 OR creator_subject=$4 OR assignee_subject=$4)")
        .bind(organization_id).bind(case_id).bind(can_read_all).bind(actor)
        .fetch_optional(postgres.pool()).await.map_err(|source| database("authorize message list resource", source))?;
    if visible.is_none() {
        return Ok(None);
    }
    let cursor_time = cursor.map(|value| value.created_at);
    let cursor_id = cursor.map(|value| value.message_id);
    let rows = sqlx::query("SELECT *,0::bigint AS case_revision FROM support_case_messages WHERE case_id=$1 AND ($2 OR visibility='public') AND ($3::timestamptz IS NULL OR (created_at,message_id)>($3,$4)) ORDER BY created_at ASC,message_id ASC LIMIT $5")
        .bind(case_id).bind(can_read_internal).bind(cursor_time).bind(cursor_id).bind(limit)
        .fetch_all(postgres.pool()).await.map_err(|source| database("list support case messages", source))?;
    Ok(Some(
        rows.iter().map(decode_message).collect::<Result<_, _>>()?,
    ))
}

pub(crate) async fn export_subject(
    postgres: &OwnedPostgres,
    organization_id: &str,
    subject: &str,
) -> Result<Value, StorageError> {
    let cases = sqlx::query("SELECT case_id,identifier,organization_id,requester_subject,creator_subject,title,description,priority,state,assignee_subject,revision,created_at,updated_at,resolved_at,closed_at FROM support_cases c WHERE organization_id=$1 AND (requester_subject=$2 OR creator_subject=$2 OR assignee_subject=$2 OR EXISTS(SELECT 1 FROM support_case_messages m WHERE m.case_id=c.case_id AND m.author_subject=$2)) ORDER BY created_at,case_id")
        .bind(organization_id).bind(subject).fetch_all(postgres.pool()).await.map_err(|source| database("export support cases", source))?;
    let messages = sqlx::query("SELECT m.message_id,m.case_id,m.visibility,m.author_subject,m.body,m.created_at FROM support_case_messages m JOIN support_cases c ON c.case_id=m.case_id WHERE c.organization_id=$1 AND m.author_subject=$2 ORDER BY m.created_at,m.message_id")
        .bind(organization_id).bind(subject).fetch_all(postgres.pool()).await.map_err(|source| database("export support messages", source))?;
    let activities = sqlx::query("SELECT activity_id,case_id,kind,actor_subject,case_revision,payload,created_at FROM support_case_activity WHERE organization_id=$1 AND actor_subject=$2 ORDER BY created_at,activity_id")
        .bind(organization_id).bind(subject).fetch_all(postgres.pool()).await.map_err(|source| database("export support activity", source))?;

    let case_values = cases
        .iter()
        .map(|row| {
            let record = decode_case(row)?;
            serde_json::to_value(record).map_err(StorageError::from)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let message_values = messages
        .iter()
        .map(|row| {
            let mut record = decode_message(row)?;
            record.case_revision = 0;
            serde_json::to_value(record).map_err(StorageError::from)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let activity_values = activities.iter().map(|row| {
        let mut payload = row.try_get::<Json<Value>,_>("payload").map_err(|source| database("decode export activity payload", source))?.0;
        if payload.get("assignee_subject").and_then(Value::as_str) != Some(subject)
            && let Some(object) = payload.as_object_mut()
        {
            object.remove("assignee_subject");
        }
        Ok(json!({
            "activity_id": row.try_get::<Uuid,_>("activity_id").map_err(|source| database("decode export activity id", source))?,
            "case_id": row.try_get::<Uuid,_>("case_id").map_err(|source| database("decode export activity case", source))?,
            "kind": row.try_get::<String,_>("kind").map_err(|source| database("decode export activity kind", source))?,
            "actor_subject": row.try_get::<String,_>("actor_subject").map_err(|source| database("decode export activity actor", source))?,
            "case_revision": row.try_get::<i64,_>("case_revision").map_err(|source| database("decode export activity revision", source))?.to_string(),
            "payload": payload,
            "created_at": format_time(row.try_get::<OffsetDateTime,_>("created_at").map_err(|source| database("decode export activity time", source))?)?,
        }))
    }).collect::<Result<Vec<_>, StorageError>>()?;
    Ok(
        json!({"subject": subject, "organization_id": organization_id, "cases": case_values, "authored_messages": message_values, "actor_activity": activity_values}),
    )
}

pub(crate) async fn apply_retention(
    postgres: &OwnedPostgres,
    action_id: &str,
    request_hash: &[u8],
    organization_id: &str,
    subject: &str,
    delete_content: bool,
) -> Result<Result<String, DomainFailure>, StorageError> {
    let mut transaction = begin(postgres, "begin support retention").await?;
    advisory_lock(&mut transaction, action_id).await?;
    if let Some(row) = sqlx::query(
        "SELECT request_hash,receipt FROM support_case_retention_receipts WHERE action_id=$1",
    )
    .bind(action_id)
    .fetch_optional(&mut *transaction)
    .await
    .map_err(|source| database("read retention receipt", source))?
    {
        let existing: Vec<u8> = row
            .try_get("request_hash")
            .map_err(|source| database("decode retention request hash", source))?;
        if existing != request_hash {
            return Ok(Err(DomainFailure::RetentionConflict));
        }
        let receipt: String = row
            .try_get("receipt")
            .map_err(|source| database("decode retention receipt", source))?;
        commit(transaction, "commit retention replay").await?;
        return Ok(Ok(receipt));
    }
    let anonymized = format!("anon_{}", Uuid::new_v4().simple());
    let affected_rows = sqlx::query("SELECT case_id,revision FROM support_cases WHERE organization_id=$1 AND (requester_subject=$2 OR creator_subject=$2 OR assignee_subject=$2 OR EXISTS(SELECT 1 FROM support_case_messages m WHERE m.case_id=support_cases.case_id AND m.author_subject=$2)) FOR UPDATE")
        .bind(organization_id).bind(subject).fetch_all(&mut *transaction).await.map_err(|source| database("lock retention cases", source))?;
    sqlx::query("UPDATE support_case_commands SET response=response || CASE WHEN response->>'requester_subject'=$2 THEN jsonb_build_object('requester_subject',$3) ELSE '{}'::jsonb END || CASE WHEN response->>'creator_subject'=$2 THEN jsonb_build_object('creator_subject',$3) ELSE '{}'::jsonb END || CASE WHEN response->>'assignee_subject'=$2 THEN jsonb_build_object('assignee_subject',NULL) ELSE '{}'::jsonb END || CASE WHEN response->>'author_subject'=$2 THEN jsonb_build_object('author_subject',$3) ELSE '{}'::jsonb END || CASE WHEN $4 AND (response->>'requester_subject'=$2 OR response->>'creator_subject'=$2) THEN jsonb_build_object('title','[deleted support case]','description','[deleted by privacy request]') ELSE '{}'::jsonb END || CASE WHEN $4 AND response->>'author_subject'=$2 THEN jsonb_build_object('body','[deleted by privacy request]') ELSE '{}'::jsonb END WHERE organization_id=$1 AND (response->>'requester_subject'=$2 OR response->>'creator_subject'=$2 OR response->>'assignee_subject'=$2 OR response->>'author_subject'=$2)")
        .bind(organization_id).bind(subject).bind(&anonymized).bind(delete_content).execute(&mut *transaction).await.map_err(|source| database("sanitize personal command receipts", source))?;
    if delete_content {
        sqlx::query("UPDATE support_case_messages m SET body='[deleted by privacy request]' FROM support_cases c WHERE m.case_id=c.case_id AND c.organization_id=$1 AND m.author_subject=$2")
            .bind(organization_id).bind(subject).execute(&mut *transaction).await.map_err(|source| database("redact retained messages", source))?;
        sqlx::query("UPDATE support_cases SET title=CASE WHEN requester_subject=$2 OR creator_subject=$2 THEN '[deleted support case]' ELSE title END,description=CASE WHEN requester_subject=$2 OR creator_subject=$2 THEN '[deleted by privacy request]' ELSE description END WHERE organization_id=$1 AND (requester_subject=$2 OR creator_subject=$2)")
            .bind(organization_id).bind(subject).execute(&mut *transaction).await.map_err(|source| database("redact retained cases", source))?;
    }
    sqlx::query("UPDATE support_cases c SET requester_subject=CASE WHEN requester_subject=$2 THEN $3 ELSE requester_subject END,creator_subject=CASE WHEN creator_subject=$2 THEN $3 ELSE creator_subject END,assignee_subject=CASE WHEN assignee_subject=$2 THEN NULL ELSE assignee_subject END,revision=revision+1,updated_at=CURRENT_TIMESTAMP WHERE organization_id=$1 AND (requester_subject=$2 OR creator_subject=$2 OR assignee_subject=$2 OR EXISTS(SELECT 1 FROM support_case_messages m WHERE m.case_id=c.case_id AND m.author_subject=$2))")
        .bind(organization_id).bind(subject).bind(&anonymized).execute(&mut *transaction).await.map_err(|source| database("anonymize support cases", source))?;
    sqlx::query("UPDATE support_case_messages m SET author_subject=$3 FROM support_cases c WHERE m.case_id=c.case_id AND c.organization_id=$1 AND m.author_subject=$2")
        .bind(organization_id).bind(subject).bind(&anonymized).execute(&mut *transaction).await.map_err(|source| database("anonymize message authors", source))?;
    sqlx::query("UPDATE support_case_activity SET payload=payload || CASE WHEN payload->>'assignee_subject'=$2 THEN jsonb_build_object('assignee_subject',NULL) ELSE '{}'::jsonb END || CASE WHEN $3 AND actor_subject=$2 AND payload ? 'reason' THEN jsonb_build_object('reason','[deleted by privacy request]') ELSE '{}'::jsonb END WHERE organization_id=$1 AND (payload->>'assignee_subject'=$2 OR ($3 AND actor_subject=$2 AND payload ? 'reason'))")
        .bind(organization_id).bind(subject).bind(delete_content).execute(&mut *transaction).await.map_err(|source| database("sanitize support activity payload", source))?;
    sqlx::query("UPDATE support_case_activity SET actor_subject=$3 WHERE organization_id=$1 AND actor_subject=$2")
        .bind(organization_id).bind(subject).bind(&anonymized).execute(&mut *transaction).await.map_err(|source| database("anonymize support activity", source))?;
    for row in &affected_rows {
        let case_id: Uuid = row
            .try_get("case_id")
            .map_err(|source| database("decode retained case id", source))?;
        let revision: i64 = row
            .try_get::<i64, _>("revision")
            .map_err(|source| database("decode retained case revision", source))?
            + 1;
        insert_activity(
            &mut transaction,
            organization_id,
            case_id,
            "privacy.retention_applied",
            &format!("retention:{action_id}"),
            revision,
            json!({"mode": if delete_content {"delete"} else {"anonymize"}}),
        )
        .await?;
    }
    let receipt = format!(
        "support-case:{action_id}:{}",
        if delete_content {
            "deleted"
        } else {
            "anonymized"
        }
    );
    sqlx::query("INSERT INTO support_case_retention_receipts(action_id,request_hash,receipt,anonymized_subject) VALUES($1,$2,$3,$4)")
        .bind(action_id).bind(request_hash).bind(&receipt).bind(&anonymized)
        .execute(&mut *transaction).await.map_err(|source| database("store retention receipt", source))?;
    commit(transaction, "commit support retention").await?;
    Ok(Ok(receipt))
}

async fn begin<'a>(
    postgres: &'a OwnedPostgres,
    operation: &'static str,
) -> Result<Transaction<'a, Postgres>, StorageError> {
    postgres
        .pool()
        .begin()
        .await
        .map_err(|source| database(operation, source))
}

async fn commit(
    transaction: Transaction<'_, Postgres>,
    operation: &'static str,
) -> Result<(), StorageError> {
    transaction
        .commit()
        .await
        .map_err(|source| database(operation, source))
}

async fn command_replay<T: DeserializeOwned>(
    transaction: &mut Transaction<'_, Postgres>,
    caller: &str,
    key: &str,
    operation: &str,
    request_hash: &[u8],
) -> Result<Result<Option<T>, DomainFailure>, StorageError> {
    advisory_lock(transaction, &format!("{caller}\u{0}{key}")).await?;
    let row = sqlx::query("SELECT operation,request_hash,response FROM support_case_commands WHERE caller_instance=$1 AND idempotency_key=$2")
        .bind(caller).bind(key).fetch_optional(&mut **transaction).await.map_err(|source| database("read command replay", source))?;
    let Some(row) = row else {
        return Ok(Ok(None));
    };
    let stored_operation: String = row
        .try_get("operation")
        .map_err(|source| database("decode command operation", source))?;
    let stored_hash: Vec<u8> = row
        .try_get("request_hash")
        .map_err(|source| database("decode command hash", source))?;
    if stored_operation != operation || stored_hash != request_hash {
        return Ok(Err(DomainFailure::IdempotencyConflict));
    }
    let Json(response): Json<Value> = row
        .try_get("response")
        .map_err(|source| database("decode command response", source))?;
    Ok(Ok(Some(serde_json::from_value(response)?)))
}

#[allow(clippy::too_many_arguments)]
async fn save_command<T: Serialize>(
    transaction: &mut Transaction<'_, Postgres>,
    caller: &str,
    key: &str,
    organization_id: &str,
    case_id: Uuid,
    operation: &str,
    request_hash: &[u8],
    response: &T,
) -> Result<(), StorageError> {
    sqlx::query("INSERT INTO support_case_commands(caller_instance,idempotency_key,organization_id,case_id,operation,request_hash,response) VALUES($1,$2,$3,$4,$5,$6,$7)")
        .bind(caller).bind(key).bind(organization_id).bind(case_id).bind(operation).bind(request_hash).bind(Json(serde_json::to_value(response)?))
        .execute(&mut **transaction).await.map_err(|source| database("store command response", source))?;
    Ok(())
}

async fn advisory_lock(
    transaction: &mut Transaction<'_, Postgres>,
    value: &str,
) -> Result<(), StorageError> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
        .bind(value)
        .execute(&mut **transaction)
        .await
        .map_err(|source| database("acquire idempotency lock", source))?;
    Ok(())
}

async fn locked_case(
    transaction: &mut Transaction<'_, Postgres>,
    organization_id: &str,
    case_id: Uuid,
) -> Result<Option<CaseRecord>, StorageError> {
    let row = sqlx::query(
        "SELECT * FROM support_cases WHERE organization_id=$1 AND case_id=$2 FOR UPDATE",
    )
    .bind(organization_id)
    .bind(case_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|source| database("lock support case", source))?;
    row.as_ref().map(decode_case).transpose()
}

#[allow(clippy::too_many_arguments)]
async fn insert_activity(
    transaction: &mut Transaction<'_, Postgres>,
    organization_id: &str,
    case_id: Uuid,
    kind: &str,
    actor: &str,
    revision: i64,
    payload: Value,
) -> Result<(), StorageError> {
    sqlx::query("INSERT INTO support_case_activity(activity_id,organization_id,case_id,kind,actor_subject,case_revision,payload) VALUES($1,$2,$3,$4,$5,$6,$7)")
        .bind(Uuid::new_v4()).bind(organization_id).bind(case_id).bind(kind).bind(actor).bind(revision).bind(Json(payload))
        .execute(&mut **transaction).await.map_err(|source| database("insert support case activity", source))?;
    Ok(())
}

fn decode_case(row: &sqlx::postgres::PgRow) -> Result<CaseRecord, StorageError> {
    let revision: i64 = row
        .try_get("revision")
        .map_err(|source| database("decode case revision", source))?;
    if revision <= 0 {
        return Err(StorageError::InvalidStoredData {
            detail: "case revision is not positive".to_owned(),
        });
    }
    Ok(CaseRecord {
        case_id: row
            .try_get("case_id")
            .map_err(|source| database("decode case id", source))?,
        identifier: row
            .try_get("identifier")
            .map_err(|source| database("decode case identifier", source))?,
        organization_id: row
            .try_get("organization_id")
            .map_err(|source| database("decode case organization", source))?,
        requester_subject: row
            .try_get("requester_subject")
            .map_err(|source| database("decode case requester", source))?,
        creator_subject: row
            .try_get("creator_subject")
            .map_err(|source| database("decode case creator", source))?,
        title: row
            .try_get("title")
            .map_err(|source| database("decode case title", source))?,
        description: row
            .try_get("description")
            .map_err(|source| database("decode case description", source))?,
        priority: row
            .try_get("priority")
            .map_err(|source| database("decode case priority", source))?,
        state: row
            .try_get("state")
            .map_err(|source| database("decode case state", source))?,
        assignee_subject: row
            .try_get("assignee_subject")
            .map_err(|source| database("decode case assignee", source))?,
        revision,
        created_at: row
            .try_get("created_at")
            .map_err(|source| database("decode case created time", source))?,
        updated_at: row
            .try_get("updated_at")
            .map_err(|source| database("decode case updated time", source))?,
        resolved_at: row
            .try_get("resolved_at")
            .map_err(|source| database("decode case resolved time", source))?,
        closed_at: row
            .try_get("closed_at")
            .map_err(|source| database("decode case closed time", source))?,
    })
}

fn decode_message(row: &sqlx::postgres::PgRow) -> Result<MessageRecord, StorageError> {
    Ok(MessageRecord {
        message_id: row
            .try_get("message_id")
            .map_err(|source| database("decode message id", source))?,
        case_id: row
            .try_get("case_id")
            .map_err(|source| database("decode message case", source))?,
        visibility: row
            .try_get("visibility")
            .map_err(|source| database("decode message visibility", source))?,
        author_subject: row
            .try_get("author_subject")
            .map_err(|source| database("decode message author", source))?,
        body: row
            .try_get("body")
            .map_err(|source| database("decode message body", source))?,
        created_at: row
            .try_get("created_at")
            .map_err(|source| database("decode message created time", source))?,
        case_revision: row.try_get("case_revision").unwrap_or(0),
    })
}

pub(crate) fn encode_case_cursor(record: &CaseRecord) -> Result<String, StorageError> {
    Ok(format!(
        "{}|{}",
        format_time(record.updated_at)?,
        record.case_id
    ))
}

pub(crate) fn decode_case_cursor(value: &str) -> Option<CaseCursor> {
    let (time, id) = value.split_once('|')?;
    Some(CaseCursor {
        updated_at: OffsetDateTime::parse(time, &Rfc3339).ok()?,
        case_id: Uuid::parse_str(id).ok()?,
    })
}

pub(crate) fn encode_message_cursor(record: &MessageRecord) -> Result<String, StorageError> {
    Ok(format!(
        "{}|{}",
        format_time(record.created_at)?,
        record.message_id
    ))
}

pub(crate) fn decode_message_cursor(value: &str) -> Option<MessageCursor> {
    let (time, id) = value.split_once('|')?;
    Some(MessageCursor {
        created_at: OffsetDateTime::parse(time, &Rfc3339).ok()?,
        message_id: Uuid::parse_str(id).ok()?,
    })
}

fn format_time(value: OffsetDateTime) -> Result<String, StorageError> {
    value
        .format(&Rfc3339)
        .map_err(|error| StorageError::InvalidStoredData {
            detail: format!("timestamp cannot be formatted: {error}"),
        })
}

pub(crate) fn valid_transition(current: &str, target: &str) -> bool {
    matches!(
        (current, target),
        ("open" | "closed", "in_progress")
            | ("in_progress", "waiting_customer" | "resolved")
            | ("waiting_customer", "in_progress" | "resolved")
            | ("resolved", "in_progress" | "closed")
    )
}

fn database(operation: &'static str, source: sqlx::Error) -> StorageError {
    StorageError::Database { operation, source }
}

mod decimal_i64 {
    use serde::{Deserialize as _, Deserializer, Serializer};

    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub(super) fn serialize<S>(value: &i64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&value.to_string())
    }

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<i64, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}
