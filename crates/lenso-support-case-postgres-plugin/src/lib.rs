//! PostgreSQL-backed Support Case Plugin with target-owned authorization.

mod operator;
#[cfg(all(test, feature = "postgres-acceptance"))]
mod postgres_tests;
mod schema;
mod storage;

use std::{cell::RefCell, collections::BTreeSet, fmt, rc::Rc, time::Duration};

use lenso::prelude::*;
use lenso_auth_sdk::{
    ActorAssertion, ActorAssertionVerifier, ActorProjectionError, AssertionClock, TypedActor,
};
use lenso_capability_access_control as access;
use lenso_capability_access_control::{
    AccessControlInvocationError, CheckPermissionRequest, CheckPermissionRequestScope,
};
use lenso_capability_data_export_source as export_source;
use lenso_capability_data_export_source::{
    CollectExportError, CollectExportRequest, CollectExportResponse, CollectExportResponseItemsItem,
};
use lenso_capability_organization_membership as membership;
use lenso_capability_organization_membership::{
    CheckMembershipRequest, OrganizationMembershipInvocationError,
};
use lenso_capability_retention_participant as retention;
use lenso_capability_retention_participant::{
    ApplyRetentionError, ApplyRetentionRequest, ApplyRetentionRequestMode, ApplyRetentionResponse,
};
use lenso_capability_secrets as secrets;
use lenso_capability_secrets::{ResolveRequest, SecretsClient, SecretsInvocationError};
use lenso_capability_support_case as support;
use lenso_capability_support_case::{
    AddMessageError, AddMessageRequest, AddMessageRequestVisibility, AddMessageResponse,
    AssignCaseError, AssignCaseRequest, AssignCaseResponse, CreateCaseError, CreateCaseRequest,
    CreateCaseRequestPriority, CreateCaseResponse, GetCaseError, GetCaseRequest, GetCaseResponse,
    ListCasesError, ListCasesRequest, ListCasesRequestState, ListCasesResponse,
    ListCasesResponseCasesItem, ListMessagesError, ListMessagesRequest, ListMessagesResponse,
    ListMessagesResponseMessagesItem, TransitionCaseError, TransitionCaseRequest,
    TransitionCaseRequestState, TransitionCaseResponse, UpdateCaseError, UpdateCaseRequest,
    UpdateCaseRequestPriority, UpdateCaseResponse,
};
use lenso_capability_support_case_authorization as case_authorization;
use lenso_capability_support_case_authorization::{
    AuthorizeCaseAccessError, AuthorizeCaseAccessRequest, AuthorizeCaseAccessRequestAction,
    AuthorizeCaseAccessResponse,
};
use lenso_capability_support_intake as intake;
use lenso_capability_support_intake::{
    AppendChannelMessageError, AppendChannelMessageRequest, AppendChannelMessageResponse,
    GetRequesterCaseError, GetRequesterCaseRequest, GetRequesterCaseResponse,
    ListRequesterMessagesError, ListRequesterMessagesRequest, ListRequesterMessagesResponse,
    ListRequesterMessagesResponseMessagesItem, OpenCaseFromChannelError,
    OpenCaseFromChannelRequest, OpenCaseFromChannelRequestPriority, OpenCaseFromChannelResponse,
};
use lenso_kernel::{PluginDependencies, RuntimeFailure};
use lenso_postgres_kit::OwnedPostgres;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;
use zeroize::Zeroizing;

use crate::storage::{CaseFilters, DomainFailure};

pub use operator::{SupportCaseOperator, SupportCaseOperatorError};

const DEPENDENCY_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_CALLERS: usize = 64;
const MAX_ID_BYTES: usize = 512;
const MAX_TITLE_BYTES: usize = 300;
const MAX_DESCRIPTION_BYTES: usize = 50_000;
const MAX_MESSAGE_BYTES: usize = 100_000;
const MAX_REASON_BYTES: usize = 2_000;
const MAX_IDEMPOTENCY_BYTES: usize = 200;
const DEFAULT_MAX_EXPORT_BYTES: usize = 8 * 1024 * 1024;

const CASES_CREATE: &str = "support.cases.create";
const CASES_READ: &str = "support.cases.read";
const CASES_WRITE: &str = "support.cases.write";
const CASES_ASSIGN: &str = "support.cases.assign";
const CASES_TRANSITION: &str = "support.cases.transition";
const CASES_COMMENT: &str = "support.cases.comment";
const CASES_INTERNAL_NOTE: &str = "support.cases.internal-note";

#[derive(Serialize)]
struct ChannelMessageFingerprint<'a> {
    body: &'a str,
    case_ref: &'a str,
    organization_id: &'a str,
    requester_subject: &'a str,
}

/// Immutable configuration for one Support Case Plugin Instance.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SupportCaseConfig {
    schema: String,
    database_url_secret: String,
    auth_issuer: String,
    auth_assertion_public_key: String,
    business_callers: Vec<String>,
    intake_callers: Vec<String>,
    resource_callers: Vec<String>,
    export_callers: Vec<String>,
    retention_callers: Vec<String>,
    #[serde(default = "default_max_export_bytes")]
    max_export_bytes: usize,
}

impl SupportCaseConfig {
    /// Creates and validates immutable Support Case configuration.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        schema: impl Into<String>,
        database_url_secret: impl Into<String>,
        auth_issuer: impl Into<String>,
        auth_assertion_public_key: impl Into<String>,
        business_callers: Vec<String>,
        intake_callers: Vec<String>,
        resource_callers: Vec<String>,
        export_callers: Vec<String>,
        retention_callers: Vec<String>,
        max_export_bytes: usize,
    ) -> Result<Self, SupportCaseConfigError> {
        let config = Self {
            schema: schema.into(),
            database_url_secret: database_url_secret.into(),
            auth_issuer: auth_issuer.into(),
            auth_assertion_public_key: auth_assertion_public_key.into(),
            business_callers,
            intake_callers,
            resource_callers,
            export_callers,
            retention_callers,
            max_export_bytes,
        };
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), SupportCaseConfigError> {
        schema::schema_plan(self.schema.clone())
            .map_err(|_| SupportCaseConfigError::InvalidSchema)?;
        if !valid_secret_reference(&self.database_url_secret) {
            return Err(SupportCaseConfigError::InvalidSecretReference);
        }
        if !valid_identifier(&self.auth_issuer, 256) {
            return Err(SupportCaseConfigError::InvalidAuthIssuer);
        }
        ActorAssertionVerifier::from_public_key_base64(
            self.auth_issuer.clone(),
            &self.auth_assertion_public_key,
        )
        .map_err(|_| SupportCaseConfigError::InvalidAuthPublicKey)?;
        validate_callers(&self.business_callers)
            .map_err(|()| SupportCaseConfigError::InvalidBusinessCallers)?;
        validate_callers(&self.intake_callers)
            .map_err(|()| SupportCaseConfigError::InvalidIntakeCallers)?;
        validate_callers(&self.resource_callers)
            .map_err(|()| SupportCaseConfigError::InvalidResourceCallers)?;
        validate_callers(&self.export_callers)
            .map_err(|()| SupportCaseConfigError::InvalidExportCallers)?;
        validate_callers(&self.retention_callers)
            .map_err(|()| SupportCaseConfigError::InvalidRetentionCallers)?;
        if !(1..=DEFAULT_MAX_EXPORT_BYTES).contains(&self.max_export_bytes) {
            return Err(SupportCaseConfigError::InvalidExportLimit);
        }
        Ok(())
    }

    fn verifier(&self) -> Result<ActorAssertionVerifier, RuntimeFailure> {
        ActorAssertionVerifier::from_public_key_base64(
            self.auth_issuer.clone(),
            &self.auth_assertion_public_key,
        )
        .map_err(|_| RuntimeFailure::InvalidResolvedPlan {
            detail: "Support Case Auth verification key is invalid".to_owned(),
        })
    }
}

/// Invalid immutable Support Case configuration.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum SupportCaseConfigError {
    #[error("invalid owned PostgreSQL schema")]
    InvalidSchema,
    #[error("invalid database URL secret reference")]
    InvalidSecretReference,
    #[error("invalid Auth issuer")]
    InvalidAuthIssuer,
    #[error("invalid Auth assertion public key")]
    InvalidAuthPublicKey,
    #[error("business_callers must contain unique exact Instance keys")]
    InvalidBusinessCallers,
    #[error("intake_callers must contain unique exact Instance keys")]
    InvalidIntakeCallers,
    #[error("resource_callers must contain unique exact Instance keys")]
    InvalidResourceCallers,
    #[error("export_callers must contain unique exact Instance keys")]
    InvalidExportCallers,
    #[error("retention_callers must contain unique exact Instance keys")]
    InvalidRetentionCallers,
    #[error("max_export_bytes must be between 1 and 8388608")]
    InvalidExportLimit,
}

fn validate_config(config: &SupportCaseConfig) -> Result<(), RuntimeFailure> {
    config
        .validate()
        .map_err(|error| RuntimeFailure::InvalidResolvedPlan {
            detail: format!("Support Case configuration is invalid: {error}"),
        })
}

#[derive(Clone, Debug)]
struct PreparedSupportCase {
    postgres: OwnedPostgres,
}

#[lenso::plugin(
    lifecycle,
    configuration_schema = "configuration.schema.json",
    validate = validate_config
)]
#[derive(Clone)]
struct PostgresSupportCasePlugin {
    #[config]
    config: SupportCaseConfig,
    secrets: Port<secrets::SecretsClient>,
    membership: Port<membership::OrganizationMembershipClient>,
    access: Port<access::AccessControlClient>,
    prepared: Rc<RefCell<Option<PreparedSupportCase>>>,
}

impl fmt::Debug for PostgresSupportCasePlugin {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresSupportCasePlugin")
            .field("schema", &self.config.schema)
            .field("prepared", &self.prepared.borrow().is_some())
            .field("business_caller_count", &self.config.business_callers.len())
            .field("intake_caller_count", &self.config.intake_callers.len())
            .field("resource_caller_count", &self.config.resource_callers.len())
            .finish_non_exhaustive()
    }
}

#[lenso::provides(
    support::SupportCase,
    intake::SupportIntake,
    case_authorization::SupportCaseAuthorization,
    export_source::DataExportSource,
    retention::RetentionParticipant
)]
impl PostgresSupportCasePlugin {}

impl PostgresSupportCasePlugin {
    async fn open_case_from_channel(
        &self,
        context: Ctx,
        request: OpenCaseFromChannelRequest,
    ) -> PluginResult<OpenCaseFromChannelResponse, OpenCaseFromChannelError> {
        let caller = Self::allowed_caller(&context, &self.config.intake_callers)
            .ok_or_else(|| PluginError::domain(OpenCaseFromChannelError::Forbidden))?;
        if !valid_opaque_id(&request.organization_id, MAX_ID_BYTES)
            || !valid_opaque_id(&request.requester_subject, MAX_ID_BYTES)
            || !valid_text(&request.title, MAX_TITLE_BYTES, false)
            || !valid_text(&request.description, MAX_DESCRIPTION_BYTES, true)
            || !valid_idempotency_key(&request.idempotency_key)
        {
            return Err(PluginError::domain(
                OpenCaseFromChannelError::InvalidRequest,
            ));
        }
        let request_hash = request_hash(&request)?;
        let record = storage::create_case(
            &self.prepared().map_err(PluginError::runtime)?.postgres,
            &caller,
            &request.idempotency_key,
            &request_hash,
            &request.organization_id,
            &request.requester_subject,
            &request.title,
            &request.description,
            intake_priority(&request.priority),
        )
        .await
        .map_err(storage_runtime)?
        .map_err(|failure| PluginError::domain(map_intake_open_failure(failure)))?;
        wire_cast(&requester_case_value(&record).map_err(serialization_runtime)?)
    }

    async fn append_channel_message(
        &self,
        context: Ctx,
        request: AppendChannelMessageRequest,
    ) -> PluginResult<AppendChannelMessageResponse, AppendChannelMessageError> {
        let caller = Self::allowed_caller(&context, &self.config.intake_callers)
            .ok_or_else(|| PluginError::domain(AppendChannelMessageError::Forbidden))?;
        let expected_revision = parse_mutation_request(
            &request.organization_id,
            &request.case_ref,
            &request.expected_revision,
            &request.idempotency_key,
        )
        .ok_or_else(|| PluginError::domain(AppendChannelMessageError::InvalidRequest))?;
        if !valid_opaque_id(&request.requester_subject, MAX_ID_BYTES)
            || !valid_text(&request.body, MAX_MESSAGE_BYTES, false)
        {
            return Err(PluginError::domain(
                AppendChannelMessageError::InvalidRequest,
            ));
        }
        let case_id = Uuid::parse_str(&request.case_ref)
            .map_err(|_| PluginError::domain(AppendChannelMessageError::InvalidRequest))?;
        // The optimistic revision is a first-attempt concurrency guard, not
        // part of the channel message's semantic identity. A provider retry
        // re-reads the current revision; keeping it out of the receipt hash
        // lets the stable idempotency key replay the original result.
        let request_hash = request_hash(&ChannelMessageFingerprint {
            body: &request.body,
            case_ref: &request.case_ref,
            organization_id: &request.organization_id,
            requester_subject: &request.requester_subject,
        })?;
        let record = storage::add_message(
            &self.prepared().map_err(PluginError::runtime)?.postgres,
            &caller,
            &request.idempotency_key,
            &request_hash,
            &request.organization_id,
            case_id,
            &request.requester_subject,
            false,
            expected_revision,
            "public",
            &request.body,
        )
        .await
        .map_err(storage_runtime)?
        .map_err(|failure| PluginError::domain(map_intake_message_failure(failure)))?;
        wire_cast(&requester_message_value(&record).map_err(serialization_runtime)?)
    }

    async fn get_requester_case(
        &self,
        context: Ctx,
        request: GetRequesterCaseRequest,
    ) -> PluginResult<GetRequesterCaseResponse, GetRequesterCaseError> {
        Self::allowed_caller(&context, &self.config.intake_callers)
            .ok_or_else(|| PluginError::domain(GetRequesterCaseError::Forbidden))?;
        if !valid_opaque_id(&request.organization_id, MAX_ID_BYTES)
            || !valid_opaque_id(&request.requester_subject, MAX_ID_BYTES)
            || !valid_case_ref(&request.case_ref)
        {
            return Err(PluginError::domain(GetRequesterCaseError::InvalidRequest));
        }
        let record = storage::get_case(
            &self.prepared().map_err(PluginError::runtime)?.postgres,
            &request.organization_id,
            &request.case_ref,
            &request.requester_subject,
            false,
        )
        .await
        .map_err(storage_runtime)?
        .ok_or_else(|| PluginError::domain(GetRequesterCaseError::CaseNotFound))?;
        wire_cast(&requester_case_value(&record).map_err(serialization_runtime)?)
    }

    async fn list_requester_messages(
        &self,
        context: Ctx,
        request: ListRequesterMessagesRequest,
    ) -> PluginResult<ListRequesterMessagesResponse, ListRequesterMessagesError> {
        Self::allowed_caller(&context, &self.config.intake_callers)
            .ok_or_else(|| PluginError::domain(ListRequesterMessagesError::Forbidden))?;
        let cursor =
            parse_optional_cursor(request.cursor.as_deref(), storage::decode_message_cursor)
                .map_err(|()| PluginError::domain(ListRequesterMessagesError::InvalidRequest))?;
        if !valid_opaque_id(&request.organization_id, MAX_ID_BYTES)
            || !valid_opaque_id(&request.requester_subject, MAX_ID_BYTES)
            || !(1..=200).contains(&request.limit)
        {
            return Err(PluginError::domain(
                ListRequesterMessagesError::InvalidRequest,
            ));
        }
        let case_id = Uuid::parse_str(&request.case_ref)
            .map_err(|_| PluginError::domain(ListRequesterMessagesError::InvalidRequest))?;
        let mut records = storage::list_messages(
            &self.prepared().map_err(PluginError::runtime)?.postgres,
            &request.organization_id,
            case_id,
            &request.requester_subject,
            false,
            false,
            cursor.as_ref(),
            request.limit + 1,
        )
        .await
        .map_err(storage_runtime)?
        .ok_or_else(|| PluginError::domain(ListRequesterMessagesError::CaseNotFound))?;
        let page_size = usize::try_from(request.limit)
            .map_err(|_| PluginError::domain(ListRequesterMessagesError::InvalidRequest))?;
        let has_more = records.len() > page_size;
        if has_more {
            records.pop();
        }
        let next_cursor = if has_more {
            records
                .last()
                .map(storage::encode_message_cursor)
                .transpose()
                .map_err(storage_runtime)?
        } else {
            None
        };
        let messages = records
            .iter()
            .map(|record| {
                wire_cast(&requester_list_message_value(record).map_err(serialization_runtime)?)
            })
            .collect::<PluginResult<
                Vec<ListRequesterMessagesResponseMessagesItem>,
                ListRequesterMessagesError,
            >>()?;
        Ok(ListRequesterMessagesResponse {
            messages,
            next_cursor,
        })
    }

    async fn authorize_case_access(
        &self,
        context: Ctx,
        request: AuthorizeCaseAccessRequest,
    ) -> PluginResult<AuthorizeCaseAccessResponse, AuthorizeCaseAccessError> {
        Self::allowed_caller(&context, &self.config.resource_callers)
            .ok_or_else(|| PluginError::domain(AuthorizeCaseAccessError::Forbidden))?;
        if !valid_opaque_id(&request.organization_id, MAX_ID_BYTES)
            || !valid_opaque_id(&request.subject, MAX_ID_BYTES)
            || !valid_case_ref(&request.case_ref)
            || request
                .message_id
                .as_deref()
                .is_some_and(|value| Uuid::parse_str(value).is_err())
        {
            return Err(PluginError::domain(
                AuthorizeCaseAccessError::InvalidRequest,
            ));
        }
        let postgres = self.prepared().map_err(PluginError::runtime)?.postgres;
        let public_action = matches!(
            request.action,
            AuthorizeCaseAccessRequestAction::ReadPublic
                | AuthorizeCaseAccessRequestAction::AttachPublic
        );
        if public_action {
            let related = storage::get_case(
                &postgres,
                &request.organization_id,
                &request.case_ref,
                &request.subject,
                false,
            )
            .await
            .map_err(storage_runtime)?;
            if let Some(record) = related {
                return Self::case_resource_response(
                    &postgres,
                    &request.organization_id,
                    record.case_id,
                    request.message_id.as_deref(),
                )
                .await;
            }
        }
        let permission = match request.action {
            AuthorizeCaseAccessRequestAction::ReadPublic => CASES_READ,
            AuthorizeCaseAccessRequestAction::AttachPublic => CASES_COMMENT,
            AuthorizeCaseAccessRequestAction::ReadInternal
            | AuthorizeCaseAccessRequestAction::AttachInternal => CASES_INTERNAL_NOTE,
        };
        let privileged = self
            .require_membership(&context, &request.organization_id, &request.subject)
            .await
            .map_err(PluginError::runtime)?
            && self
                .permission(
                    &context,
                    &request.organization_id,
                    &request.subject,
                    permission,
                )
                .await
                .map_err(PluginError::runtime)?;
        if !privileged {
            return Ok(AuthorizeCaseAccessResponse {
                allowed: false,
                case_id: None,
            });
        }
        let record = storage::get_case(
            &postgres,
            &request.organization_id,
            &request.case_ref,
            &request.subject,
            true,
        )
        .await
        .map_err(storage_runtime)?;
        let Some(record) = record else {
            return Ok(AuthorizeCaseAccessResponse {
                allowed: false,
                case_id: None,
            });
        };
        Self::case_resource_response(
            &postgres,
            &request.organization_id,
            record.case_id,
            request.message_id.as_deref(),
        )
        .await
    }

    async fn case_resource_response(
        postgres: &OwnedPostgres,
        organization_id: &str,
        case_id: Uuid,
        message_id: Option<&str>,
    ) -> PluginResult<AuthorizeCaseAccessResponse, AuthorizeCaseAccessError> {
        let message_matches = match message_id {
            Some(message_id) => storage::message_belongs_to_case(
                postgres,
                organization_id,
                case_id,
                Uuid::parse_str(message_id)
                    .map_err(|_| PluginError::domain(AuthorizeCaseAccessError::InvalidRequest))?,
            )
            .await
            .map_err(storage_runtime)?,
            None => true,
        };
        Ok(AuthorizeCaseAccessResponse {
            allowed: message_matches,
            case_id: message_matches.then(|| case_id.to_string()),
        })
    }

    async fn create_case(
        &self,
        context: Ctx,
        request: CreateCaseRequest,
    ) -> PluginResult<CreateCaseResponse, CreateCaseError> {
        let caller = self
            .business_caller(&context)
            .ok_or_else(|| PluginError::domain(CreateCaseError::Forbidden))?;
        let actor = self
            .authenticated_subject(&context, support::CREATE_CASE_OPERATION)
            .map_err(|()| PluginError::domain(CreateCaseError::Unauthenticated))?;
        if !valid_opaque_id(&request.organization_id, MAX_ID_BYTES)
            || !valid_text(&request.title, MAX_TITLE_BYTES, false)
            || !valid_text(&request.description, MAX_DESCRIPTION_BYTES, true)
            || !valid_idempotency_key(&request.idempotency_key)
        {
            return Err(PluginError::domain(CreateCaseError::InvalidRequest));
        }
        self.require_membership(&context, &request.organization_id, &actor)
            .await
            .map_err(PluginError::runtime)?
            .then_some(())
            .ok_or_else(|| PluginError::domain(CreateCaseError::Forbidden))?;
        self.permission(&context, &request.organization_id, &actor, CASES_CREATE)
            .await
            .map_err(PluginError::runtime)?
            .then_some(())
            .ok_or_else(|| PluginError::domain(CreateCaseError::Forbidden))?;
        let priority = create_priority(&request.priority);
        let request_hash = request_hash(&request)?;
        let record = storage::create_case(
            &self.prepared().map_err(PluginError::runtime)?.postgres,
            &caller,
            &request.idempotency_key,
            &request_hash,
            &request.organization_id,
            &actor,
            &request.title,
            &request.description,
            priority,
        )
        .await
        .map_err(storage_runtime)?
        .map_err(|failure| PluginError::domain(map_create_failure(failure)))?;
        wire_cast(&record)
    }

    async fn get_case(
        &self,
        context: Ctx,
        request: GetCaseRequest,
    ) -> PluginResult<GetCaseResponse, GetCaseError> {
        self.business_caller(&context)
            .ok_or_else(|| PluginError::domain(GetCaseError::Forbidden))?;
        let actor = self
            .authenticated_subject(&context, support::GET_CASE_OPERATION)
            .map_err(|()| PluginError::domain(GetCaseError::Unauthenticated))?;
        if !valid_opaque_id(&request.organization_id, MAX_ID_BYTES)
            || !valid_case_ref(&request.case_ref)
        {
            return Err(PluginError::domain(GetCaseError::InvalidRequest));
        }
        self.require_membership(&context, &request.organization_id, &actor)
            .await
            .map_err(PluginError::runtime)?
            .then_some(())
            .ok_or_else(|| PluginError::domain(GetCaseError::Forbidden))?;
        let can_read_all = self
            .permission(&context, &request.organization_id, &actor, CASES_READ)
            .await
            .map_err(PluginError::runtime)?;
        let record = storage::get_case(
            &self.prepared().map_err(PluginError::runtime)?.postgres,
            &request.organization_id,
            &request.case_ref,
            &actor,
            can_read_all,
        )
        .await
        .map_err(storage_runtime)?
        .ok_or_else(|| PluginError::domain(GetCaseError::CaseNotFound))?;
        wire_cast(&record)
    }

    async fn list_cases(
        &self,
        context: Ctx,
        request: ListCasesRequest,
    ) -> PluginResult<ListCasesResponse, ListCasesError> {
        self.business_caller(&context)
            .ok_or_else(|| PluginError::domain(ListCasesError::Forbidden))?;
        let actor = self
            .authenticated_subject(&context, support::LIST_CASES_OPERATION)
            .map_err(|()| PluginError::domain(ListCasesError::Unauthenticated))?;
        let cursor = parse_optional_cursor(request.cursor.as_deref(), storage::decode_case_cursor)
            .map_err(|()| PluginError::domain(ListCasesError::InvalidRequest))?;
        if !valid_opaque_id(&request.organization_id, MAX_ID_BYTES)
            || !(1..=200).contains(&request.limit)
            || request
                .assignee_subject
                .as_deref()
                .is_some_and(|value| !valid_opaque_id(value, MAX_ID_BYTES))
            || request
                .requester_subject
                .as_deref()
                .is_some_and(|value| !valid_opaque_id(value, MAX_ID_BYTES))
        {
            return Err(PluginError::domain(ListCasesError::InvalidRequest));
        }
        self.require_membership(&context, &request.organization_id, &actor)
            .await
            .map_err(PluginError::runtime)?
            .then_some(())
            .ok_or_else(|| PluginError::domain(ListCasesError::Forbidden))?;
        let can_read_all = self
            .permission(&context, &request.organization_id, &actor, CASES_READ)
            .await
            .map_err(PluginError::runtime)?;
        let state = request.state.as_ref().map(list_state);
        let mut records = storage::list_cases(
            &self.prepared().map_err(PluginError::runtime)?.postgres,
            &CaseFilters {
                organization_id: &request.organization_id,
                state,
                assignee_subject: request.assignee_subject.as_deref(),
                requester_subject: request.requester_subject.as_deref(),
                cursor: cursor.as_ref(),
                actor: &actor,
                can_read_all,
                limit: request.limit + 1,
            },
        )
        .await
        .map_err(storage_runtime)?;
        let page_size = usize::try_from(request.limit)
            .map_err(|_| PluginError::domain(ListCasesError::InvalidRequest))?;
        let has_more = records.len() > page_size;
        if has_more {
            records.pop();
        }
        let next_cursor = if has_more {
            records
                .last()
                .map(storage::encode_case_cursor)
                .transpose()
                .map_err(storage_runtime)?
        } else {
            None
        };
        let cases = records
            .iter()
            .map(wire_cast)
            .collect::<PluginResult<Vec<ListCasesResponseCasesItem>, ListCasesError>>()?;
        Ok(ListCasesResponse { cases, next_cursor })
    }

    async fn update_case(
        &self,
        context: Ctx,
        request: UpdateCaseRequest,
    ) -> PluginResult<UpdateCaseResponse, UpdateCaseError> {
        let caller = self
            .business_caller(&context)
            .ok_or_else(|| PluginError::domain(UpdateCaseError::Forbidden))?;
        let actor = self
            .authenticated_subject(&context, support::UPDATE_CASE_OPERATION)
            .map_err(|()| PluginError::domain(UpdateCaseError::Unauthenticated))?;
        let expected_revision = parse_mutation_request(
            &request.organization_id,
            &request.case_id,
            &request.expected_revision,
            &request.idempotency_key,
        )
        .ok_or_else(|| PluginError::domain(UpdateCaseError::InvalidRequest))?;
        if request.title.is_none() && request.description.is_none() && request.priority.is_none()
            || request
                .title
                .as_deref()
                .is_some_and(|value| !valid_text(value, MAX_TITLE_BYTES, false))
            || request
                .description
                .as_deref()
                .is_some_and(|value| !valid_text(value, MAX_DESCRIPTION_BYTES, true))
        {
            return Err(PluginError::domain(UpdateCaseError::InvalidRequest));
        }
        self.require_strict_permission(&context, &request.organization_id, &actor, CASES_WRITE)
            .await
            .map_err(map_update_authorization)?;
        let case_id = Uuid::parse_str(&request.case_id)
            .map_err(|_| PluginError::domain(UpdateCaseError::InvalidRequest))?;
        let priority = request.priority.as_ref().map(update_priority);
        let request_hash = request_hash(&request)?;
        let record = storage::update_case(
            &self.prepared().map_err(PluginError::runtime)?.postgres,
            &caller,
            &request.idempotency_key,
            &request_hash,
            &request.organization_id,
            case_id,
            &actor,
            expected_revision,
            request.title.as_deref(),
            request.description.as_deref(),
            priority,
        )
        .await
        .map_err(storage_runtime)?
        .map_err(|failure| PluginError::domain(map_update_failure(failure)))?;
        wire_cast(&record)
    }

    async fn assign_case(
        &self,
        context: Ctx,
        request: AssignCaseRequest,
    ) -> PluginResult<AssignCaseResponse, AssignCaseError> {
        let caller = self
            .business_caller(&context)
            .ok_or_else(|| PluginError::domain(AssignCaseError::Forbidden))?;
        let actor = self
            .authenticated_subject(&context, support::ASSIGN_CASE_OPERATION)
            .map_err(|()| PluginError::domain(AssignCaseError::Unauthenticated))?;
        let expected_revision = parse_mutation_request(
            &request.organization_id,
            &request.case_id,
            &request.expected_revision,
            &request.idempotency_key,
        )
        .ok_or_else(|| PluginError::domain(AssignCaseError::InvalidRequest))?;
        if request
            .assignee_subject
            .as_deref()
            .is_some_and(|value| !valid_opaque_id(value, MAX_ID_BYTES))
        {
            return Err(PluginError::domain(AssignCaseError::InvalidRequest));
        }
        self.require_strict_permission(&context, &request.organization_id, &actor, CASES_ASSIGN)
            .await
            .map_err(map_assign_authorization)?;
        if let Some(assignee) = &request.assignee_subject {
            self.require_membership(&context, &request.organization_id, assignee)
                .await
                .map_err(PluginError::runtime)?
                .then_some(())
                .ok_or_else(|| PluginError::domain(AssignCaseError::InvalidRequest))?;
        }
        let case_id = Uuid::parse_str(&request.case_id)
            .map_err(|_| PluginError::domain(AssignCaseError::InvalidRequest))?;
        let request_hash = request_hash(&request)?;
        let record = storage::assign_case(
            &self.prepared().map_err(PluginError::runtime)?.postgres,
            &caller,
            &request.idempotency_key,
            &request_hash,
            &request.organization_id,
            case_id,
            &actor,
            expected_revision,
            request.assignee_subject.as_deref(),
        )
        .await
        .map_err(storage_runtime)?
        .map_err(|failure| PluginError::domain(map_assign_failure(failure)))?;
        wire_cast(&record)
    }

    async fn transition_case(
        &self,
        context: Ctx,
        request: TransitionCaseRequest,
    ) -> PluginResult<TransitionCaseResponse, TransitionCaseError> {
        let caller = self
            .business_caller(&context)
            .ok_or_else(|| PluginError::domain(TransitionCaseError::Forbidden))?;
        let actor = self
            .authenticated_subject(&context, support::TRANSITION_CASE_OPERATION)
            .map_err(|()| PluginError::domain(TransitionCaseError::Unauthenticated))?;
        let expected_revision = parse_mutation_request(
            &request.organization_id,
            &request.case_id,
            &request.expected_revision,
            &request.idempotency_key,
        )
        .ok_or_else(|| PluginError::domain(TransitionCaseError::InvalidRequest))?;
        if request
            .reason
            .as_deref()
            .is_some_and(|value| !valid_text(value, MAX_REASON_BYTES, true))
        {
            return Err(PluginError::domain(TransitionCaseError::InvalidRequest));
        }
        self.require_strict_permission(
            &context,
            &request.organization_id,
            &actor,
            CASES_TRANSITION,
        )
        .await
        .map_err(map_transition_authorization)?;
        let case_id = Uuid::parse_str(&request.case_id)
            .map_err(|_| PluginError::domain(TransitionCaseError::InvalidRequest))?;
        let target_state = transition_state(&request.state);
        let request_hash = request_hash(&request)?;
        let record = storage::transition_case(
            &self.prepared().map_err(PluginError::runtime)?.postgres,
            &caller,
            &request.idempotency_key,
            &request_hash,
            &request.organization_id,
            case_id,
            &actor,
            expected_revision,
            target_state,
            request.reason.as_deref(),
        )
        .await
        .map_err(storage_runtime)?
        .map_err(|failure| PluginError::domain(map_transition_failure(failure)))?;
        wire_cast(&record)
    }

    async fn add_message(
        &self,
        context: Ctx,
        request: AddMessageRequest,
    ) -> PluginResult<AddMessageResponse, AddMessageError> {
        let caller = self
            .business_caller(&context)
            .ok_or_else(|| PluginError::domain(AddMessageError::Forbidden))?;
        let actor = self
            .authenticated_subject(&context, support::ADD_MESSAGE_OPERATION)
            .map_err(|()| PluginError::domain(AddMessageError::Unauthenticated))?;
        let expected_revision = parse_mutation_request(
            &request.organization_id,
            &request.case_id,
            &request.expected_revision,
            &request.idempotency_key,
        )
        .ok_or_else(|| PluginError::domain(AddMessageError::InvalidRequest))?;
        if !valid_text(&request.body, MAX_MESSAGE_BYTES, false) {
            return Err(PluginError::domain(AddMessageError::InvalidRequest));
        }
        self.require_membership(&context, &request.organization_id, &actor)
            .await
            .map_err(PluginError::runtime)?
            .then_some(())
            .ok_or_else(|| PluginError::domain(AddMessageError::Forbidden))?;
        let (visibility, can_comment_all) = match request.visibility {
            AddMessageRequestVisibility::Public => (
                "public",
                self.permission(&context, &request.organization_id, &actor, CASES_COMMENT)
                    .await
                    .map_err(PluginError::runtime)?,
            ),
            AddMessageRequestVisibility::Internal => {
                let allowed = self
                    .permission(
                        &context,
                        &request.organization_id,
                        &actor,
                        CASES_INTERNAL_NOTE,
                    )
                    .await
                    .map_err(PluginError::runtime)?;
                if !allowed {
                    return Err(PluginError::domain(AddMessageError::Forbidden));
                }
                ("internal", true)
            }
        };
        let case_id = Uuid::parse_str(&request.case_id)
            .map_err(|_| PluginError::domain(AddMessageError::InvalidRequest))?;
        let request_hash = request_hash(&request)?;
        let record = storage::add_message(
            &self.prepared().map_err(PluginError::runtime)?.postgres,
            &caller,
            &request.idempotency_key,
            &request_hash,
            &request.organization_id,
            case_id,
            &actor,
            can_comment_all,
            expected_revision,
            visibility,
            &request.body,
        )
        .await
        .map_err(storage_runtime)?
        .map_err(|failure| PluginError::domain(map_message_failure(failure)))?;
        wire_cast(&record)
    }

    async fn list_messages(
        &self,
        context: Ctx,
        request: ListMessagesRequest,
    ) -> PluginResult<ListMessagesResponse, ListMessagesError> {
        self.business_caller(&context)
            .ok_or_else(|| PluginError::domain(ListMessagesError::Forbidden))?;
        let actor = self
            .authenticated_subject(&context, support::LIST_MESSAGES_OPERATION)
            .map_err(|()| PluginError::domain(ListMessagesError::Unauthenticated))?;
        let cursor =
            parse_optional_cursor(request.cursor.as_deref(), storage::decode_message_cursor)
                .map_err(|()| PluginError::domain(ListMessagesError::InvalidRequest))?;
        if !valid_opaque_id(&request.organization_id, MAX_ID_BYTES)
            || !(1..=200).contains(&request.limit)
        {
            return Err(PluginError::domain(ListMessagesError::InvalidRequest));
        }
        let case_id = Uuid::parse_str(&request.case_id)
            .map_err(|_| PluginError::domain(ListMessagesError::InvalidRequest))?;
        self.require_membership(&context, &request.organization_id, &actor)
            .await
            .map_err(PluginError::runtime)?
            .then_some(())
            .ok_or_else(|| PluginError::domain(ListMessagesError::Forbidden))?;
        let can_read_all = self
            .permission(&context, &request.organization_id, &actor, CASES_READ)
            .await
            .map_err(PluginError::runtime)?;
        let can_read_internal = self
            .permission(
                &context,
                &request.organization_id,
                &actor,
                CASES_INTERNAL_NOTE,
            )
            .await
            .map_err(PluginError::runtime)?;
        let mut records = storage::list_messages(
            &self.prepared().map_err(PluginError::runtime)?.postgres,
            &request.organization_id,
            case_id,
            &actor,
            can_read_all,
            can_read_internal,
            cursor.as_ref(),
            request.limit + 1,
        )
        .await
        .map_err(storage_runtime)?
        .ok_or_else(|| PluginError::domain(ListMessagesError::CaseNotFound))?;
        let page_size = usize::try_from(request.limit)
            .map_err(|_| PluginError::domain(ListMessagesError::InvalidRequest))?;
        let has_more = records.len() > page_size;
        if has_more {
            records.pop();
        }
        let next_cursor = if has_more {
            records
                .last()
                .map(storage::encode_message_cursor)
                .transpose()
                .map_err(storage_runtime)?
        } else {
            None
        };
        let messages = records
            .iter()
            .map(wire_cast)
            .collect::<PluginResult<Vec<ListMessagesResponseMessagesItem>, ListMessagesError>>()?;
        Ok(ListMessagesResponse {
            messages,
            next_cursor,
        })
    }

    async fn collect_export(
        &self,
        context: Ctx,
        request: CollectExportRequest,
    ) -> PluginResult<CollectExportResponse, CollectExportError> {
        if Self::allowed_caller(&context, &self.config.export_callers).is_none() {
            return Err(PluginError::domain(CollectExportError::Forbidden));
        }
        if request.scope_kind != "organization"
            || !valid_opaque_id(&request.export_id, MAX_ID_BYTES)
            || !valid_opaque_id(&request.scope_id, MAX_ID_BYTES)
            || !valid_opaque_id(&request.subject, MAX_ID_BYTES)
        {
            return Err(PluginError::domain(CollectExportError::InvalidRequest));
        }
        let value = storage::export_subject(
            &self.prepared().map_err(PluginError::runtime)?.postgres,
            &request.scope_id,
            &request.subject,
        )
        .await
        .map_err(storage_runtime)?;
        let payload = serde_json::to_string(&value).map_err(serialization_runtime)?;
        if payload.len() > self.config.max_export_bytes {
            return Err(PluginError::runtime(RuntimeFailure::ResourceExhausted {
                capability: export_source::CAPABILITY_ID,
                operation: export_source::COLLECT_EXPORT_OPERATION.to_owned(),
            }));
        }
        Ok(CollectExportResponse {
            items: vec![CollectExportResponseItemsItem {
                item_name: "support-cases.json".to_owned(),
                media_type: "application/json".to_owned(),
                payload,
            }],
        })
    }

    async fn apply_retention(
        &self,
        context: Ctx,
        request: ApplyRetentionRequest,
    ) -> PluginResult<ApplyRetentionResponse, ApplyRetentionError> {
        if Self::allowed_caller(&context, &self.config.retention_callers).is_none() {
            return Err(PluginError::domain(ApplyRetentionError::Forbidden));
        }
        if request.scope_kind != "organization"
            || !valid_opaque_id(&request.action_id, MAX_ID_BYTES)
            || !valid_opaque_id(&request.scope_id, MAX_ID_BYTES)
            || !valid_opaque_id(&request.subject, MAX_ID_BYTES)
            || !valid_text(&request.reason, MAX_REASON_BYTES, true)
        {
            return Err(PluginError::domain(ApplyRetentionError::InvalidRequest));
        }
        let delete_content = matches!(request.mode, ApplyRetentionRequestMode::Delete);
        let hash = request_hash(&request)?;
        let receipt = storage::apply_retention(
            &self.prepared().map_err(PluginError::runtime)?.postgres,
            &request.action_id,
            &hash,
            &request.scope_id,
            &request.subject,
            delete_content,
        )
        .await
        .map_err(storage_runtime)?
        .map_err(|_| PluginError::domain(ApplyRetentionError::InvalidRequest))?;
        Ok(ApplyRetentionResponse { receipt })
    }

    fn prepared(&self) -> Result<PreparedSupportCase, RuntimeFailure> {
        self.prepared
            .borrow()
            .clone()
            .ok_or_else(|| RuntimeFailure::PluginFailure {
                detail: "Support Case Plugin is not prepared".to_owned(),
            })
    }

    fn business_caller(&self, context: &Ctx) -> Option<String> {
        Self::allowed_caller(context, &self.config.business_callers)
    }

    fn allowed_caller(context: &Ctx, allowed: &[String]) -> Option<String> {
        context.caller_instance().and_then(|caller| {
            allowed
                .iter()
                .any(|entry| entry == caller)
                .then(|| caller.to_owned())
        })
    }

    fn authenticated_subject(&self, context: &Ctx, operation: &str) -> Result<String, ()> {
        let actor = self
            .config
            .verifier()
            .map_err(|_| ())?
            .project_context::<SupportActor>(context, support::CAPABILITY_ID, operation, &UtcClock)
            .map_err(|_| ())?;
        valid_opaque_id(&actor.subject, MAX_ID_BYTES)
            .then_some(actor.subject)
            .ok_or(())
    }

    async fn require_membership(
        &self,
        context: &Ctx,
        organization_id: &str,
        subject: &str,
    ) -> Result<bool, RuntimeFailure> {
        self.membership
            .check_membership_with_context(
                context.clone(),
                CheckMembershipRequest {
                    organization_id: organization_id.to_owned(),
                    subject: subject.to_owned(),
                },
            )
            .await
            .map(|response| response.active)
            .map_err(|error| match error {
                OrganizationMembershipInvocationError::Domain(_) => RuntimeFailure::PluginFailure {
                    detail: "Organization Membership rejected a Support Case authorization query"
                        .to_owned(),
                },
                OrganizationMembershipInvocationError::Runtime(error) => error,
            })
    }

    async fn permission(
        &self,
        context: &Ctx,
        organization_id: &str,
        subject: &str,
        permission: &str,
    ) -> Result<bool, RuntimeFailure> {
        self.access
            .check_permission_with_context(
                context.clone(),
                CheckPermissionRequest {
                    subject: subject.to_owned(),
                    scope: CheckPermissionRequestScope {
                        kind: "organization".to_owned(),
                        id: organization_id.to_owned(),
                    },
                    permission: permission.to_owned(),
                },
            )
            .await
            .map(|response| response.allowed)
            .map_err(|error| match error {
                AccessControlInvocationError::Domain(_) => RuntimeFailure::PluginFailure {
                    detail: "Access Control rejected a Support Case authorization query".to_owned(),
                },
                AccessControlInvocationError::Runtime(error) => error,
            })
    }

    async fn require_strict_permission(
        &self,
        context: &Ctx,
        organization_id: &str,
        actor: &str,
        permission: &str,
    ) -> Result<(), AuthorizationFailure> {
        if !self
            .require_membership(context, organization_id, actor)
            .await
            .map_err(AuthorizationFailure::Runtime)?
        {
            return Err(AuthorizationFailure::Forbidden);
        }
        if !self
            .permission(context, organization_id, actor, permission)
            .await
            .map_err(AuthorizationFailure::Runtime)?
        {
            return Err(AuthorizationFailure::Forbidden);
        }
        Ok(())
    }
}

impl Lifecycle for PostgresSupportCasePlugin {
    async fn activate(&self, context: ActivateContext) -> Result<(), RuntimeFailure> {
        let database_url = resolve_secret(
            &self.secrets,
            context.dependencies(),
            context.cancellation(),
            &self.config.database_url_secret,
        )
        .await?;
        let postgres = OwnedPostgres::prepare(
            &database_url,
            schema::schema_plan(self.config.schema.clone()).map_err(|error| {
                RuntimeFailure::InvalidResolvedPlan {
                    detail: error.to_string(),
                }
            })?,
        )
        .await
        .map_err(|error| RuntimeFailure::PluginFailure {
            detail: error.to_string(),
        })?;
        self.prepared
            .borrow_mut()
            .replace(PreparedSupportCase { postgres });
        Ok(())
    }

    async fn deactivate(&self, _context: DeactivateContext) -> Result<(), RuntimeFailure> {
        let prepared = self.prepared.borrow_mut().take();
        if let Some(prepared) = prepared {
            prepared.postgres.pool().close().await;
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct SupportActor {
    subject: String,
}

impl TypedActor for SupportActor {
    fn from_assertion(assertion: &ActorAssertion) -> Result<Self, ActorProjectionError> {
        Ok(Self {
            subject: assertion.subject().to_owned(),
        })
    }
}

#[derive(Clone, Copy, Debug)]
struct UtcClock;

impl AssertionClock for UtcClock {
    fn now(&self) -> OffsetDateTime {
        OffsetDateTime::now_utc()
    }
}

#[derive(Debug)]
enum AuthorizationFailure {
    Forbidden,
    Runtime(RuntimeFailure),
}

async fn resolve_secret(
    secrets: &SecretsClient,
    dependencies: &PluginDependencies,
    cancellation: lenso_kernel::CancellationToken,
    reference: &str,
) -> Result<Zeroizing<String>, RuntimeFailure> {
    let context = dependencies.invocation_context_after(DEPENDENCY_TIMEOUT, cancellation)?;
    secrets
        .resolve_with_context(
            context,
            ResolveRequest {
                reference: reference.to_owned(),
            },
        )
        .await
        .map(|response| Zeroizing::new(response.value))
        .map_err(|error| match error {
            SecretsInvocationError::Domain(_) => RuntimeFailure::PluginFailure {
                detail: format!("database URL secret `{reference}` was rejected"),
            },
            SecretsInvocationError::Runtime(error) => error,
        })
}

fn map_update_authorization(failure: AuthorizationFailure) -> PluginError<UpdateCaseError> {
    match failure {
        AuthorizationFailure::Forbidden => PluginError::domain(UpdateCaseError::Forbidden),
        AuthorizationFailure::Runtime(error) => PluginError::runtime(error),
    }
}
fn map_assign_authorization(failure: AuthorizationFailure) -> PluginError<AssignCaseError> {
    match failure {
        AuthorizationFailure::Forbidden => PluginError::domain(AssignCaseError::Forbidden),
        AuthorizationFailure::Runtime(error) => PluginError::runtime(error),
    }
}
fn map_transition_authorization(failure: AuthorizationFailure) -> PluginError<TransitionCaseError> {
    match failure {
        AuthorizationFailure::Forbidden => PluginError::domain(TransitionCaseError::Forbidden),
        AuthorizationFailure::Runtime(error) => PluginError::runtime(error),
    }
}

fn map_create_failure(failure: DomainFailure) -> CreateCaseError {
    match failure {
        DomainFailure::IdempotencyConflict => CreateCaseError::IdempotencyConflict,
        _ => CreateCaseError::InvalidRequest,
    }
}
fn map_update_failure(failure: DomainFailure) -> UpdateCaseError {
    match failure {
        DomainFailure::Forbidden => UpdateCaseError::Forbidden,
        DomainFailure::CaseNotFound => UpdateCaseError::CaseNotFound,
        DomainFailure::RevisionConflict => UpdateCaseError::RevisionConflict,
        DomainFailure::IdempotencyConflict => UpdateCaseError::IdempotencyConflict,
        DomainFailure::InvalidTransition | DomainFailure::RetentionConflict => {
            UpdateCaseError::InvalidRequest
        }
    }
}
fn map_assign_failure(failure: DomainFailure) -> AssignCaseError {
    match failure {
        DomainFailure::Forbidden => AssignCaseError::Forbidden,
        DomainFailure::CaseNotFound => AssignCaseError::CaseNotFound,
        DomainFailure::RevisionConflict => AssignCaseError::RevisionConflict,
        DomainFailure::IdempotencyConflict => AssignCaseError::IdempotencyConflict,
        DomainFailure::InvalidTransition | DomainFailure::RetentionConflict => {
            AssignCaseError::InvalidRequest
        }
    }
}
fn map_transition_failure(failure: DomainFailure) -> TransitionCaseError {
    match failure {
        DomainFailure::Forbidden => TransitionCaseError::Forbidden,
        DomainFailure::CaseNotFound => TransitionCaseError::CaseNotFound,
        DomainFailure::RevisionConflict => TransitionCaseError::RevisionConflict,
        DomainFailure::IdempotencyConflict => TransitionCaseError::IdempotencyConflict,
        DomainFailure::InvalidTransition => TransitionCaseError::InvalidTransition,
        DomainFailure::RetentionConflict => TransitionCaseError::InvalidRequest,
    }
}
fn map_message_failure(failure: DomainFailure) -> AddMessageError {
    match failure {
        DomainFailure::Forbidden => AddMessageError::Forbidden,
        DomainFailure::CaseNotFound => AddMessageError::CaseNotFound,
        DomainFailure::RevisionConflict => AddMessageError::RevisionConflict,
        DomainFailure::IdempotencyConflict => AddMessageError::IdempotencyConflict,
        DomainFailure::InvalidTransition | DomainFailure::RetentionConflict => {
            AddMessageError::InvalidRequest
        }
    }
}

fn map_intake_open_failure(failure: DomainFailure) -> OpenCaseFromChannelError {
    match failure {
        DomainFailure::Forbidden => OpenCaseFromChannelError::Forbidden,
        DomainFailure::IdempotencyConflict => OpenCaseFromChannelError::IdempotencyConflict,
        DomainFailure::CaseNotFound
        | DomainFailure::RevisionConflict
        | DomainFailure::InvalidTransition
        | DomainFailure::RetentionConflict => OpenCaseFromChannelError::InvalidRequest,
    }
}

fn map_intake_message_failure(failure: DomainFailure) -> AppendChannelMessageError {
    match failure {
        DomainFailure::Forbidden => AppendChannelMessageError::Forbidden,
        DomainFailure::CaseNotFound => AppendChannelMessageError::CaseNotFound,
        DomainFailure::RevisionConflict => AppendChannelMessageError::RevisionConflict,
        DomainFailure::IdempotencyConflict => AppendChannelMessageError::IdempotencyConflict,
        DomainFailure::InvalidTransition | DomainFailure::RetentionConflict => {
            AppendChannelMessageError::InvalidRequest
        }
    }
}

fn requester_case_value(record: &storage::CaseRecord) -> Result<Value, serde_json::Error> {
    let mut value = serde_json::to_value(record)?;
    let Some(object) = value.as_object_mut() else {
        return Err(serde_json::Error::io(std::io::Error::other(
            "Support Case record did not serialize as an object",
        )));
    };
    object.retain(|key, _| {
        matches!(
            key.as_str(),
            "case_id"
                | "identifier"
                | "organization_id"
                | "requester_subject"
                | "title"
                | "description"
                | "priority"
                | "state"
                | "revision"
                | "created_at"
                | "updated_at"
        )
    });
    Ok(value)
}

fn requester_message_value(record: &storage::MessageRecord) -> Result<Value, serde_json::Error> {
    let mut value = serde_json::to_value(record)?;
    let Some(object) = value.as_object_mut() else {
        return Err(serde_json::Error::io(std::io::Error::other(
            "Support Case message did not serialize as an object",
        )));
    };
    object.remove("visibility");
    Ok(value)
}

fn requester_list_message_value(
    record: &storage::MessageRecord,
) -> Result<Value, serde_json::Error> {
    let mut value = requester_message_value(record)?;
    if let Some(object) = value.as_object_mut() {
        object.remove("case_revision");
    }
    Ok(value)
}

fn request_hash<T: Serialize, E>(request: &T) -> Result<Vec<u8>, PluginError<E>> {
    serde_json::to_vec(request)
        .map(|wire| Sha256::digest(wire).to_vec())
        .map_err(serialization_runtime)
}

fn wire_cast<T: DeserializeOwned, E>(value: &impl Serialize) -> Result<T, PluginError<E>> {
    serde_json::to_value(value)
        .and_then(serde_json::from_value)
        .map_err(serialization_runtime)
}

#[allow(clippy::needless_pass_by_value)]
fn serialization_runtime<E>(error: serde_json::Error) -> PluginError<E> {
    PluginError::runtime(RuntimeFailure::Internal {
        detail: format!("Support Case wire serialization failed: {error}"),
    })
}

#[allow(clippy::needless_pass_by_value)]
fn storage_runtime<E>(error: storage::StorageError) -> PluginError<E> {
    PluginError::runtime(RuntimeFailure::PluginFailure {
        detail: error.to_string(),
    })
}

fn create_priority(value: &CreateCaseRequestPriority) -> &'static str {
    match value {
        CreateCaseRequestPriority::Low => "low",
        CreateCaseRequestPriority::Normal => "normal",
        CreateCaseRequestPriority::High => "high",
        CreateCaseRequestPriority::Urgent => "urgent",
    }
}
fn intake_priority(value: &OpenCaseFromChannelRequestPriority) -> &'static str {
    match value {
        OpenCaseFromChannelRequestPriority::Low => "low",
        OpenCaseFromChannelRequestPriority::Normal => "normal",
        OpenCaseFromChannelRequestPriority::High => "high",
        OpenCaseFromChannelRequestPriority::Urgent => "urgent",
    }
}
fn update_priority(value: &UpdateCaseRequestPriority) -> &'static str {
    match value {
        UpdateCaseRequestPriority::Low => "low",
        UpdateCaseRequestPriority::Normal => "normal",
        UpdateCaseRequestPriority::High => "high",
        UpdateCaseRequestPriority::Urgent => "urgent",
    }
}
fn list_state(value: &ListCasesRequestState) -> &'static str {
    match value {
        ListCasesRequestState::Open => "open",
        ListCasesRequestState::InProgress => "in_progress",
        ListCasesRequestState::WaitingCustomer => "waiting_customer",
        ListCasesRequestState::Resolved => "resolved",
        ListCasesRequestState::Closed => "closed",
    }
}
fn transition_state(value: &TransitionCaseRequestState) -> &'static str {
    match value {
        TransitionCaseRequestState::Open => "open",
        TransitionCaseRequestState::InProgress => "in_progress",
        TransitionCaseRequestState::WaitingCustomer => "waiting_customer",
        TransitionCaseRequestState::Resolved => "resolved",
        TransitionCaseRequestState::Closed => "closed",
    }
}

fn parse_mutation_request(
    organization_id: &str,
    case_id: &str,
    revision: &str,
    key: &str,
) -> Option<i64> {
    if !valid_opaque_id(organization_id, MAX_ID_BYTES)
        || Uuid::parse_str(case_id).is_err()
        || !valid_idempotency_key(key)
    {
        return None;
    }
    revision.parse::<i64>().ok().filter(|value| *value > 0)
}

fn valid_case_ref(value: &str) -> bool {
    Uuid::parse_str(value).is_ok()
        || value.strip_prefix("SUP-").is_some_and(|number| {
            !number.is_empty()
                && number.bytes().all(|byte| byte.is_ascii_digit())
                && !number.starts_with('0')
        })
}

fn valid_text(value: &str, maximum: usize, allow_empty: bool) -> bool {
    let trimmed = value.trim();
    (allow_empty || !trimmed.is_empty())
        && value.len() <= maximum
        && !value
            .chars()
            .any(|character| character.is_control() && !matches!(character, '\n' | '\r' | '\t'))
}

fn valid_idempotency_key(value: &str) -> bool {
    valid_opaque_id(value, MAX_IDEMPOTENCY_BYTES)
}

fn valid_opaque_id(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':' | b'/')
        })
}

fn valid_identifier(value: &str, maximum: usize) -> bool {
    valid_opaque_id(value, maximum) && !value.contains('/')
}

fn valid_secret_reference(reference: &str) -> bool {
    !reference.is_empty()
        && reference.len() <= 256
        && !reference.starts_with('/')
        && !reference.ends_with('/')
        && !reference.contains("//")
        && reference
            .split('/')
            .all(|segment| segment != "." && segment != "..")
        && valid_opaque_id(reference, 256)
}

fn validate_callers(callers: &[String]) -> Result<(), ()> {
    if callers.is_empty()
        || callers.len() > MAX_CALLERS
        || callers.iter().any(|caller| !valid_identifier(caller, 256))
        || callers.iter().collect::<BTreeSet<_>>().len() != callers.len()
    {
        Err(())
    } else {
        Ok(())
    }
}

const fn default_max_export_bytes() -> usize {
    DEFAULT_MAX_EXPORT_BYTES
}

fn parse_optional_cursor<T>(
    value: Option<&str>,
    parser: impl FnOnce(&str) -> Option<T>,
) -> Result<Option<T>, ()> {
    match value {
        Some(value) => parser(value).map(Some).ok_or(()),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lenso_auth_sdk::{ActorAssertionIssuer, Validity, audience};
    use lenso_kernel::{CancellationToken, InvocationContext};
    use lenso_native_adapter::NativePluginRegistry;
    use time::Duration as TimeDuration;

    fn config() -> SupportCaseConfig {
        let issuer = ActorAssertionIssuer::new("auth.users", b"support-case-test-key");
        SupportCaseConfig::new(
            "support_case",
            "support-case/database-url",
            "auth.users",
            issuer.public_key_base64(),
            vec!["support-api".to_owned()],
            vec!["support-email".to_owned(), "help-center".to_owned()],
            vec!["support-attachment".to_owned()],
            vec!["privacy-export".to_owned()],
            vec!["privacy-retention".to_owned()],
            DEFAULT_MAX_EXPORT_BYTES,
        )
        .unwrap()
    }

    fn plugin() -> PostgresSupportCasePlugin {
        PostgresSupportCasePlugin {
            config: config(),
            secrets: Port::default(),
            membership: Port::default(),
            access: Port::default(),
            prepared: Rc::new(RefCell::new(None)),
        }
    }

    fn context(caller: &str) -> InvocationContext {
        InvocationContext::new(1, None, CancellationToken::new()).with_caller_instance(caller)
    }

    #[test]
    fn descriptor_declares_only_real_capabilities_and_dependencies() {
        let descriptor: serde_json::Value = serde_json::from_str(PLUGIN_DESCRIPTOR_JSON).unwrap();
        let provided = descriptor["provided_capabilities"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value["capability_id"].as_str().unwrap())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            provided,
            BTreeSet::from([
                support::CAPABILITY_ID,
                intake::CAPABILITY_ID,
                case_authorization::CAPABILITY_ID,
                export_source::CAPABILITY_ID,
                retention::CAPABILITY_ID
            ])
        );
        let required = descriptor["required_capabilities"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value["capability_id"].as_str().unwrap())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            required,
            BTreeSet::from([
                secrets::CAPABILITY_ID,
                membership::CAPABILITY_ID,
                access::CAPABILITY_ID
            ])
        );
        assert_eq!(
            NativePluginRegistry::new()
                .with_linked_factories()
                .factories()
                .filter(|factory| factory.package_id() == PACKAGE_ID)
                .count(),
            1
        );
    }

    #[test]
    fn state_machine_is_explicit_and_reopen_is_supported() {
        assert!(storage::valid_transition("open", "in_progress"));
        assert!(storage::valid_transition("resolved", "closed"));
        assert!(storage::valid_transition("closed", "in_progress"));
        assert!(!storage::valid_transition("open", "closed"));
        assert!(!storage::valid_transition("resolved", "waiting_customer"));
    }

    #[test]
    fn config_rejects_ambient_and_duplicate_callers() {
        let mut invalid = config();
        invalid.business_callers.clear();
        assert_eq!(
            invalid.validate(),
            Err(SupportCaseConfigError::InvalidBusinessCallers)
        );
        let mut invalid = config();
        invalid.intake_callers.clear();
        assert_eq!(
            invalid.validate(),
            Err(SupportCaseConfigError::InvalidIntakeCallers)
        );
        let mut invalid = config();
        invalid.export_callers.push("privacy-export".to_owned());
        assert_eq!(
            invalid.validate(),
            Err(SupportCaseConfigError::InvalidExportCallers)
        );
    }

    #[test]
    fn actor_assertions_are_bound_to_the_exact_operation() {
        let issuer = ActorAssertionIssuer::new("auth.users", b"support-case-test-key");
        let now = OffsetDateTime::now_utc();
        let assertion = issuer.issue(
            "usr_1",
            "user",
            "strong",
            [audience(
                support::CAPABILITY_ID,
                support::CREATE_CASE_OPERATION,
            )],
            Validity::new(
                now - TimeDuration::seconds(1),
                now + TimeDuration::minutes(1),
            )
            .unwrap(),
            std::collections::BTreeMap::default(),
        );
        let context = assertion.attach(context("support-api")).unwrap();
        assert_eq!(
            plugin().authenticated_subject(&context, support::CREATE_CASE_OPERATION),
            Ok("usr_1".to_owned())
        );
        assert_eq!(
            plugin().authenticated_subject(&context, support::UPDATE_CASE_OPERATION),
            Err(())
        );
    }

    #[test]
    fn exact_caller_is_required_before_storage_or_ports() {
        let request = CreateCaseRequest {
            organization_id: "org_1".to_owned(),
            title: "Need help".to_owned(),
            description: String::new(),
            priority: CreateCaseRequestPriority::Normal,
            idempotency_key: "create_1".to_owned(),
        };
        let result =
            futures::executor::block_on(plugin().create_case(context("other-api"), request));
        assert_eq!(result, Err(PluginError::Domain(CreateCaseError::Forbidden)));
    }

    #[test]
    fn cursors_are_round_trip_and_malformed_values_fail_closed() {
        let record = storage::CaseRecord {
            case_id: Uuid::new_v4(),
            identifier: "SUP-1".to_owned(),
            organization_id: "org_1".to_owned(),
            requester_subject: "usr_1".to_owned(),
            creator_subject: "usr_1".to_owned(),
            title: "x".to_owned(),
            description: String::new(),
            priority: "normal".to_owned(),
            state: "open".to_owned(),
            assignee_subject: None,
            revision: 1,
            created_at: OffsetDateTime::now_utc(),
            updated_at: OffsetDateTime::now_utc(),
            resolved_at: None,
            closed_at: None,
        };
        let encoded = storage::encode_case_cursor(&record).unwrap();
        let decoded = storage::decode_case_cursor(&encoded).unwrap();
        assert_eq!(decoded.case_id, record.case_id);
        assert!(storage::decode_case_cursor("not-a-cursor").is_none());
    }
}
