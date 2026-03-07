use std::sync::Arc;

use crate::codex::Session;
use crate::codex::TurnContext;
use crate::function_tool::FunctionCallError;
use crate::scheduler::ScheduleCreateArgs;
use crate::scheduler::ScheduleIdArgs;
use crate::scheduler::ScheduleReportArgs;
use crate::scheduler::serialize_output;
use crate::tools::context::ToolInvocation;
use crate::tools::context::ToolOutput;
use crate::tools::context::ToolPayload;
use crate::tools::handlers::parse_arguments;
use crate::tools::registry::ToolHandler;
use crate::tools::registry::ToolKind;
use async_trait::async_trait;
use codex_protocol::ThreadId;
use codex_protocol::models::FunctionCallOutputBody;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;

pub struct ScheduleHandler;

#[async_trait]
impl ToolHandler for ScheduleHandler {
    fn kind(&self) -> ToolKind {
        ToolKind::Function
    }

    fn matches_kind(&self, payload: &ToolPayload) -> bool {
        matches!(payload, ToolPayload::Function { .. })
    }

    async fn handle(&self, invocation: ToolInvocation) -> Result<ToolOutput, FunctionCallError> {
        let ToolInvocation {
            session,
            turn,
            tool_name,
            payload,
            ..
        } = invocation;

        let arguments = match payload {
            ToolPayload::Function { arguments } => arguments,
            _ => {
                return Err(FunctionCallError::RespondToModel(
                    "schedule handler received unsupported payload".to_string(),
                ));
            }
        };

        let body = match tool_name.as_str() {
            "schedule_create" => create(session, turn, arguments).await?,
            "schedule_list" => list(session).await?,
            "schedule_delete" => delete(session, arguments).await?,
            "schedule_pause" => pause(session, arguments).await?,
            "schedule_resume" => resume(session, arguments).await?,
            "schedule_report" => report(session, turn, arguments).await?,
            other => {
                return Err(FunctionCallError::RespondToModel(format!(
                    "unsupported schedule tool {other}"
                )));
            }
        };

        Ok(ToolOutput::Function {
            body: FunctionCallOutputBody::Text(body),
            success: Some(true),
        })
    }
}

async fn create(
    session: Arc<Session>,
    turn: Arc<TurnContext>,
    arguments: String,
) -> Result<String, FunctionCallError> {
    let args: ScheduleCreateArgs = parse_arguments(&arguments)?;
    let schedule = session
        .services
        .scheduler
        .create_job(turn.as_ref(), args)
        .await
        .map_err(FunctionCallError::RespondToModel)?;
    serialize_output(&schedule)
}

async fn list(session: Arc<Session>) -> Result<String, FunctionCallError> {
    let schedules = session
        .services
        .scheduler
        .list_jobs(session.conversation_id)
        .await
        .map_err(FunctionCallError::RespondToModel)?;
    serialize_output(&schedules)
}

async fn delete(session: Arc<Session>, arguments: String) -> Result<String, FunctionCallError> {
    let args: ScheduleIdArgs = parse_arguments(&arguments)?;
    let schedule = session
        .services
        .scheduler
        .delete_job(session.conversation_id, args.id.as_str())
        .await
        .map_err(FunctionCallError::RespondToModel)?;
    serialize_output(&schedule)
}

async fn pause(session: Arc<Session>, arguments: String) -> Result<String, FunctionCallError> {
    let args: ScheduleIdArgs = parse_arguments(&arguments)?;
    let schedule = session
        .services
        .scheduler
        .pause_job(session.conversation_id, args.id.as_str())
        .await
        .map_err(FunctionCallError::RespondToModel)?;
    serialize_output(&schedule)
}

async fn resume(session: Arc<Session>, arguments: String) -> Result<String, FunctionCallError> {
    let args: ScheduleIdArgs = parse_arguments(&arguments)?;
    let schedule = session
        .services
        .scheduler
        .resume_job(session.conversation_id, args.id.as_str())
        .await
        .map_err(FunctionCallError::RespondToModel)?;
    serialize_output(&schedule)
}

async fn report(
    session: Arc<Session>,
    turn: Arc<TurnContext>,
    arguments: String,
) -> Result<String, FunctionCallError> {
    let args: ScheduleReportArgs = parse_arguments(&arguments)?;
    validate_report_target(session.as_ref(), turn.as_ref(), args.thread_id.as_str())?;
    let ack = session
        .services
        .scheduler
        .record_report(args)
        .await
        .map_err(FunctionCallError::RespondToModel)?;
    serialize_output(&ack)
}

fn validate_report_target(
    session: &Session,
    turn: &TurnContext,
    thread_id: &str,
) -> Result<(), FunctionCallError> {
    let expected_thread_id = scheduled_report_thread_id(session, turn)?;
    if thread_id == expected_thread_id.to_string() {
        Ok(())
    } else {
        Err(FunctionCallError::RespondToModel(format!(
            "schedule_report must target thread_id `{expected_thread_id}` for this scheduled execution"
        )))
    }
}

fn scheduled_report_thread_id(
    session: &Session,
    turn: &TurnContext,
) -> Result<ThreadId, FunctionCallError> {
    if turn.sub_id.starts_with("schedule-") {
        return Ok(session.conversation_id);
    }

    match &turn.session_source {
        SessionSource::SubAgent(SubAgentSource::Other(label)) => {
            parse_schedule_owner_thread_id(label).ok_or_else(|| {
                FunctionCallError::RespondToModel(
                    "schedule_report can only be called from a scheduled execution".to_string(),
                )
            })
        }
        SessionSource::Cli
        | SessionSource::VSCode
        | SessionSource::Exec
        | SessionSource::Mcp
        | SessionSource::SubAgent(_)
        | SessionSource::Unknown => Err(FunctionCallError::RespondToModel(
            "schedule_report can only be called from a scheduled execution".to_string(),
        )),
    }
}

fn parse_schedule_owner_thread_id(label: &str) -> Option<ThreadId> {
    let mut parts = label.splitn(3, ':');
    if parts.next()? != "schedule" {
        return None;
    }
    let thread_id = parts.next()?;
    let _job_id = parts.next()?;
    ThreadId::from_string(thread_id).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codex::make_session_and_context;
    use pretty_assertions::assert_eq;
    use uuid::Uuid;

    #[test]
    fn parse_schedule_owner_thread_id_requires_schedule_label() {
        let thread_id = ThreadId::new();
        let label = format!("schedule:{thread_id}:{}", Uuid::new_v4());
        assert_eq!(parse_schedule_owner_thread_id(&label), Some(thread_id));
        assert_eq!(parse_schedule_owner_thread_id("agent_job:123"), None);
        assert_eq!(
            parse_schedule_owner_thread_id("schedule:not-a-thread-id:job"),
            None
        );
    }

    #[tokio::test]
    async fn schedule_report_validation_rejects_non_scheduled_turns() {
        let (session, turn) = make_session_and_context().await;
        let session = Arc::new(session);
        let error = validate_report_target(
            session.as_ref(),
            &turn,
            session.conversation_id.to_string().as_str(),
        )
        .expect_err("non-scheduled turn should be rejected");
        assert_eq!(
            error.to_string(),
            "schedule_report can only be called from a scheduled execution"
        );
    }

    #[tokio::test]
    async fn schedule_report_validation_uses_owner_thread_id_for_scheduled_turns() {
        let (session, _turn) = make_session_and_context().await;
        let session = Arc::new(session);
        let scheduled_turn = session
            .new_default_turn_with_sub_id(format!("schedule-{}", Uuid::new_v4()))
            .await;

        validate_report_target(
            session.as_ref(),
            scheduled_turn.as_ref(),
            session.conversation_id.to_string().as_str(),
        )
        .expect("scheduled turn should accept owner thread id");

        let wrong_thread_id = ThreadId::new();
        let error = validate_report_target(
            session.as_ref(),
            scheduled_turn.as_ref(),
            wrong_thread_id.to_string().as_str(),
        )
        .expect_err("wrong owner thread id should be rejected");
        assert_eq!(
            error.to_string(),
            format!(
                "schedule_report must target thread_id `{}` for this scheduled execution",
                session.conversation_id
            )
        );
    }
}
