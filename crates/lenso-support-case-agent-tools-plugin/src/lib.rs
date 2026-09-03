//! Agent-facing Tools over an explicitly bound Support Case capability.

use lenso::prelude::*;
use lenso_capability_agent_tool_provider::{
    self as tool_contract, CatalogRequest, CatalogResponse, ContentType, ExecuteError,
    ExecuteRequest, ExecuteResponse, ExecutionFailedPayload, ToolDefinition, ToolExecutionClass,
};
use lenso_capability_support_case::{
    self as support_case, AddMessageRequest, AssignCaseRequest, CreateCaseRequest, GetCaseRequest,
    ListCasesRequest, ListMessagesRequest, TransitionCaseRequest, UpdateCaseRequest,
};
use lenso_kernel::RuntimeFailure;
use serde::{Serialize, de::DeserializeOwned};

pub const CREATE_CASE_TOOL: &str = "support_case_create_case";
pub const GET_CASE_TOOL: &str = "support_case_get_case";
pub const LIST_CASES_TOOL: &str = "support_case_list_cases";
pub const UPDATE_CASE_TOOL: &str = "support_case_update_case";
pub const ASSIGN_CASE_TOOL: &str = "support_case_assign_case";
pub const TRANSITION_CASE_TOOL: &str = "support_case_transition_case";
pub const ADD_MESSAGE_TOOL: &str = "support_case_add_message";
pub const LIST_MESSAGES_TOOL: &str = "support_case_list_messages";

#[lenso::plugin]
#[derive(Clone, Debug)]
struct SupportCaseAgentToolsPlugin {
    support_case: Port<support_case::SupportCaseClient>,
}

#[lenso::provides(tool_contract::ToolProvider)]
impl SupportCaseAgentToolsPlugin {
    fn catalog(
        &self,
        _context: Ctx,
        _request: CatalogRequest,
    ) -> impl std::future::Future<Output = PluginResult<CatalogResponse, tool_contract::CatalogError>>
    {
        let _ = self;
        futures::future::ready(Ok(CatalogResponse {
            tools: tool_definitions(),
        }))
    }

    async fn execute(
        &self,
        context: Ctx,
        request: ExecuteRequest,
    ) -> PluginResult<ExecuteResponse, ExecuteError> {
        macro_rules! invoke {
            ($future:expr, $tool:expr, $domain:path, $runtime:path) => {
                match $future.await {
                    Ok(response) => success($tool, &response),
                    Err($domain(error)) => Err(PluginError::domain(map_domain_error(&error))),
                    Err($runtime(error)) => Err(PluginError::runtime(error)),
                }
            };
        }

        match request.name.as_str() {
            CREATE_CASE_TOOL => {
                let arguments = decode::<CreateCaseRequest>(&request)?;
                invoke!(
                    self.support_case
                        .create_case_with_context(context, arguments),
                    CREATE_CASE_TOOL,
                    support_case::SupportCaseCreateCaseInvocationError::Domain,
                    support_case::SupportCaseCreateCaseInvocationError::Runtime
                )
            }
            GET_CASE_TOOL => {
                let arguments = decode::<GetCaseRequest>(&request)?;
                invoke!(
                    self.support_case.get_case_with_context(context, arguments),
                    GET_CASE_TOOL,
                    support_case::SupportCaseGetCaseInvocationError::Domain,
                    support_case::SupportCaseGetCaseInvocationError::Runtime
                )
            }
            LIST_CASES_TOOL => {
                let arguments = decode::<ListCasesRequest>(&request)?;
                invoke!(
                    self.support_case
                        .list_cases_with_context(context, arguments),
                    LIST_CASES_TOOL,
                    support_case::SupportCaseListCasesInvocationError::Domain,
                    support_case::SupportCaseListCasesInvocationError::Runtime
                )
            }
            UPDATE_CASE_TOOL => {
                let arguments = decode::<UpdateCaseRequest>(&request)?;
                invoke!(
                    self.support_case
                        .update_case_with_context(context, arguments),
                    UPDATE_CASE_TOOL,
                    support_case::SupportCaseUpdateCaseInvocationError::Domain,
                    support_case::SupportCaseUpdateCaseInvocationError::Runtime
                )
            }
            ASSIGN_CASE_TOOL => {
                let arguments = decode::<AssignCaseRequest>(&request)?;
                invoke!(
                    self.support_case
                        .assign_case_with_context(context, arguments),
                    ASSIGN_CASE_TOOL,
                    support_case::SupportCaseAssignCaseInvocationError::Domain,
                    support_case::SupportCaseAssignCaseInvocationError::Runtime
                )
            }
            TRANSITION_CASE_TOOL => {
                let arguments = decode::<TransitionCaseRequest>(&request)?;
                invoke!(
                    self.support_case
                        .transition_case_with_context(context, arguments),
                    TRANSITION_CASE_TOOL,
                    support_case::SupportCaseTransitionCaseInvocationError::Domain,
                    support_case::SupportCaseTransitionCaseInvocationError::Runtime
                )
            }
            ADD_MESSAGE_TOOL => {
                let arguments = decode::<AddMessageRequest>(&request)?;
                invoke!(
                    self.support_case
                        .add_message_with_context(context, arguments),
                    ADD_MESSAGE_TOOL,
                    support_case::SupportCaseAddMessageInvocationError::Domain,
                    support_case::SupportCaseAddMessageInvocationError::Runtime
                )
            }
            LIST_MESSAGES_TOOL => {
                let arguments = decode::<ListMessagesRequest>(&request)?;
                invoke!(
                    self.support_case
                        .list_messages_with_context(context, arguments),
                    LIST_MESSAGES_TOOL,
                    support_case::SupportCaseListMessagesInvocationError::Domain,
                    support_case::SupportCaseListMessagesInvocationError::Runtime
                )
            }
            _ => Err(PluginError::domain(ExecuteError::NotFound)),
        }
    }
}

fn tool_definitions() -> Vec<ToolDefinition> {
    vec![
        tool(
            GET_CASE_TOOL,
            "Get one visible Support Case by stable UUID or organization-local identifier, including its current revision.",
            include_str!(
                "../../lenso-capability-support-case/schemas/get-case-request.schema.json"
            ),
            ToolExecutionClass::ParallelSafe,
        ),
        tool(
            LIST_CASES_TOOL,
            "List visible Support Cases with bounded cursor pagination and optional state, requester, or assignee filters.",
            include_str!(
                "../../lenso-capability-support-case/schemas/list-cases-request.schema.json"
            ),
            ToolExecutionClass::ParallelSafe,
        ),
        tool(
            LIST_MESSAGES_TOOL,
            "List messages visible to the current actor for one Support Case with bounded cursor pagination.",
            include_str!(
                "../../lenso-capability-support-case/schemas/list-messages-request.schema.json"
            ),
            ToolExecutionClass::ParallelSafe,
        ),
        tool(
            CREATE_CASE_TOOL,
            "Create one Support Case. Reuse the same idempotency_key when retrying the same intent.",
            include_str!(
                "../../lenso-capability-support-case/schemas/create-case-request.schema.json"
            ),
            ToolExecutionClass::Exclusive,
        ),
        tool(
            UPDATE_CASE_TOOL,
            "Update editable Support Case fields using the revision returned by get_case. Reuse the same idempotency_key for retries.",
            include_str!(
                "../../lenso-capability-support-case/schemas/update-case-request.schema.json"
            ),
            ToolExecutionClass::Exclusive,
        ),
        tool(
            ASSIGN_CASE_TOOL,
            "Assign or unassign one Support Case using its current revision. Reuse the same idempotency_key for retries.",
            include_str!(
                "../../lenso-capability-support-case/schemas/assign-case-request.schema.json"
            ),
            ToolExecutionClass::Exclusive,
        ),
        tool(
            TRANSITION_CASE_TOOL,
            "Transition one Support Case through its allowed lifecycle using its current revision. Reuse the same idempotency_key for retries.",
            include_str!(
                "../../lenso-capability-support-case/schemas/transition-case-request.schema.json"
            ),
            ToolExecutionClass::Exclusive,
        ),
        tool(
            ADD_MESSAGE_TOOL,
            "Add one public reply or internal note to a Support Case using its current revision. Internal notes require separate authority.",
            include_str!(
                "../../lenso-capability-support-case/schemas/add-message-request.schema.json"
            ),
            ToolExecutionClass::Exclusive,
        ),
    ]
}

fn tool(
    name: &str,
    description: &str,
    schema: &str,
    execution: ToolExecutionClass,
) -> ToolDefinition {
    let schema: serde_json::Value =
        serde_json::from_str(schema).expect("Support Case Tool schema must be valid JSON");
    ToolDefinition {
        name: name.to_owned(),
        description: description.to_owned(),
        input_schema_json: schema
            .to_string()
            .try_into()
            .expect("Support Case Tool schema must remain valid JSON"),
        execution,
    }
}

fn decode<T: DeserializeOwned>(request: &ExecuteRequest) -> PluginResult<T, ExecuteError> {
    serde_json::from_str(request.arguments_json.as_str())
        .map_err(|_| PluginError::domain(ExecuteError::InvalidArguments))
}

fn success<T: Serialize>(
    tool_name: &str,
    response: &T,
) -> PluginResult<ExecuteResponse, ExecuteError> {
    let content = serde_json::to_string_pretty(response).map_err(|error| {
        PluginError::runtime(RuntimeFailure::PluginFailure {
            detail: format!("Support Case Tool could not serialize its typed response: {error}"),
        })
    })?;
    Ok(ExecuteResponse {
        content_blocks: None,
        content,
        content_type: ContentType::Text,
        metadata_json: serde_json::json!({ "tool": tool_name })
            .to_string()
            .try_into()
            .expect("Support Case Tool metadata must be valid JSON"),
    })
}

trait DomainToolError {
    fn to_tool_error(&self) -> ExecuteError;
}

fn map_domain_error(error: &impl DomainToolError) -> ExecuteError {
    error.to_tool_error()
}

fn rejected(reason_code: &str) -> ExecuteError {
    ExecuteError::ExecutionFailed {
        payload: ExecutionFailedPayload {
            reason_code: reason_code.to_owned(),
            message: "Support Case rejected the requested operation.".to_owned(),
            details_json: serde_json::json!({ "domain_error": reason_code })
                .to_string()
                .try_into()
                .expect("Support Case Tool error metadata must be valid JSON"),
        },
    }
}

macro_rules! impl_case_read_error {
    ($($error:ty),+ $(,)?) => {
        $(
            impl DomainToolError for $error {
                fn to_tool_error(&self) -> ExecuteError {
                    match self {
                        Self::InvalidRequest => ExecuteError::InvalidArguments,
                        Self::CaseNotFound => ExecuteError::NotFound,
                        Self::Forbidden | Self::Unauthenticated => ExecuteError::PermissionDenied,
                        Self::Unknown(_) => rejected("unknown_domain_error"),
                    }
                }
            }
        )+
    };
}

impl_case_read_error!(support_case::GetCaseError, support_case::ListMessagesError);

impl DomainToolError for support_case::ListCasesError {
    fn to_tool_error(&self) -> ExecuteError {
        match self {
            Self::InvalidRequest => ExecuteError::InvalidArguments,
            Self::Forbidden | Self::Unauthenticated => ExecuteError::PermissionDenied,
            Self::Unknown(_) => rejected("unknown_domain_error"),
        }
    }
}

macro_rules! impl_case_mutation_error {
    ($($error:ty),+ $(,)?) => {
        $(
            impl DomainToolError for $error {
                fn to_tool_error(&self) -> ExecuteError {
                    match self {
                        Self::InvalidRequest => ExecuteError::InvalidArguments,
                        Self::CaseNotFound => ExecuteError::NotFound,
                        Self::Forbidden | Self::Unauthenticated => ExecuteError::PermissionDenied,
                        Self::IdempotencyConflict => rejected("idempotency_conflict"),
                        Self::RevisionConflict => rejected("revision_conflict"),
                        Self::Unknown(_) => rejected("unknown_domain_error"),
                    }
                }
            }
        )+
    };
}

impl_case_mutation_error!(
    support_case::AddMessageError,
    support_case::AssignCaseError,
    support_case::UpdateCaseError,
);

impl DomainToolError for support_case::CreateCaseError {
    fn to_tool_error(&self) -> ExecuteError {
        match self {
            Self::InvalidRequest => ExecuteError::InvalidArguments,
            Self::Forbidden | Self::Unauthenticated => ExecuteError::PermissionDenied,
            Self::IdempotencyConflict => rejected("idempotency_conflict"),
            Self::Unknown(_) => rejected("unknown_domain_error"),
        }
    }
}

impl DomainToolError for support_case::TransitionCaseError {
    fn to_tool_error(&self) -> ExecuteError {
        match self {
            Self::InvalidRequest => ExecuteError::InvalidArguments,
            Self::CaseNotFound => ExecuteError::NotFound,
            Self::Forbidden | Self::Unauthenticated => ExecuteError::PermissionDenied,
            Self::IdempotencyConflict => rejected("idempotency_conflict"),
            Self::InvalidTransition => rejected("invalid_transition"),
            Self::RevisionConflict => rejected("revision_conflict"),
            Self::Unknown(_) => rejected("unknown_domain_error"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(name: &str, arguments: &str) -> ExecuteRequest {
        ExecuteRequest {
            name: name.to_owned(),
            arguments_json: arguments.try_into().unwrap(),
        }
    }

    #[test]
    fn descriptor_is_a_removable_adapter_with_one_business_requirement() {
        let descriptor: serde_json::Value = serde_json::from_str(PLUGIN_DESCRIPTOR_JSON).unwrap();
        assert_eq!(descriptor["plugin_id"], "lenso.support-case.agent-tools");
        let provided = descriptor["provided_capabilities"].as_array().unwrap();
        assert_eq!(provided.len(), 1);
        assert_eq!(provided[0]["capability_id"], "lenso.agent.tool-provider@2");
        let required = descriptor["required_capabilities"].as_array().unwrap();
        assert_eq!(required.len(), 1);
        assert_eq!(required[0]["capability_id"], "lenso.support-case@1");
    }

    #[test]
    fn catalog_has_three_parallel_reads_and_five_exclusive_mutations() {
        let tools = tool_definitions();
        assert_eq!(tools.len(), 8);
        assert_eq!(
            tools
                .iter()
                .filter(|tool| tool.execution == ToolExecutionClass::ParallelSafe)
                .count(),
            3
        );
        assert_eq!(
            tools
                .iter()
                .filter(|tool| tool.execution == ToolExecutionClass::Exclusive)
                .count(),
            5
        );
        assert!(tools.iter().all(|tool| {
            let schema: serde_json::Value =
                serde_json::from_str(tool.input_schema_json.as_str()).unwrap();
            schema["additionalProperties"] == false
        }));
    }

    #[test]
    fn exact_capability_requests_decode_without_adapter_owned_business_fields() {
        let get = decode::<GetCaseRequest>(&request(
            GET_CASE_TOOL,
            r#"{"organization_id":"org-1","case_ref":"SUP-42"}"#,
        ))
        .unwrap();
        assert_eq!(get.case_ref, "SUP-42");

        assert!(decode::<GetCaseRequest>(&request(GET_CASE_TOOL, r#"{"case_ref":42}"#)).is_err());
    }

    #[test]
    fn authorization_not_found_and_lifecycle_failures_remain_distinct() {
        assert_eq!(
            map_domain_error(&support_case::GetCaseError::Forbidden),
            ExecuteError::PermissionDenied
        );
        assert_eq!(
            map_domain_error(&support_case::GetCaseError::CaseNotFound),
            ExecuteError::NotFound
        );
        let ExecuteError::ExecutionFailed { payload } =
            map_domain_error(&support_case::TransitionCaseError::InvalidTransition)
        else {
            panic!("invalid transition must remain an execution failure");
        };
        assert_eq!(payload.reason_code, "invalid_transition");
    }
}
