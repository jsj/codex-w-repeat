use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use serde::Deserialize;
use serde::Serialize;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::fs;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::warn;
use uuid::Uuid;

use crate::agent::AgentControl;
use crate::agent::AgentStatus;
use crate::agent::status::is_final;
use crate::codex::Session;
use crate::codex::TurnContext;
use crate::function_tool::FunctionCallError;
use crate::protocol::AskForApproval;
use crate::protocol::SandboxPolicy;
use crate::protocol::SessionSource;
use crate::protocol::SubAgentSource;
use crate::tasks::RegularTask;
use codex_protocol::ThreadId;
use codex_protocol::config_types::CollaborationMode;
use codex_protocol::config_types::ModeKind;
use codex_protocol::config_types::Personality;
use codex_protocol::config_types::ReasoningSummary as ReasoningSummaryConfig;
use codex_protocol::config_types::ServiceTier;
use codex_protocol::user_input::UserInput;

const SCHEDULES_DIR: &str = "schedules";
const STORE_VERSION: u32 = 1;
const MAX_HISTORY_RECORDS: usize = 20;
const DEFAULT_IDLE_WAIT_SECONDS: u64 = 30;
const LOOP_TICK_SECONDS: u64 = 1;
const MIN_DELAY_SECONDS: u64 = 1;
const MIN_INTERVAL_SECONDS: u64 = 10;
const MISFIRE_GRACE_SECONDS: i64 = 5;

static STORE_LOCKS: LazyLock<std::sync::Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>> =
    LazyLock::new(|| std::sync::Mutex::new(HashMap::new()));

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ScheduleTriggerType {
    Time,
    Poll,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub(crate) enum ScheduleConcurrency {
    #[default]
    Queue,
    Skip,
    Parallel,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub(crate) enum ScheduleMisfirePolicy {
    #[default]
    RunOnce,
    Skip,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ScheduleReportStatus {
    Continue,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum StoredScheduleStatus {
    Active,
    Paused,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ScheduleExecutionMode {
    Queue,
    Parallel,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ScheduleExecutionDisposition {
    Executed,
    Continued,
    Completed,
    Failed,
    Skipped,
    Aborted,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ScheduleCreateArgs {
    pub(crate) trigger: ScheduleTriggerType,
    pub(crate) prompt: String,
    pub(crate) schedule: Option<ScheduleTimeArgs>,
    pub(crate) poll: Option<SchedulePollArgs>,
    pub(crate) concurrency: Option<ScheduleConcurrency>,
    pub(crate) misfire_policy: Option<ScheduleMisfirePolicy>,
    pub(crate) durable: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ScheduleTimeArgs {
    pub(crate) at: Option<String>,
    #[serde(rename = "in")]
    pub(crate) after: Option<String>,
    pub(crate) every: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct SchedulePollArgs {
    pub(crate) every: String,
    pub(crate) until: String,
    pub(crate) timeout: Option<String>,
    pub(crate) max_runs: Option<u32>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ScheduleIdArgs {
    pub(crate) id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ScheduleReportArgs {
    pub(crate) thread_id: String,
    pub(crate) id: String,
    pub(crate) run_id: String,
    pub(crate) status: ScheduleReportStatus,
    pub(crate) summary: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct ScheduleReportAck {
    pub(crate) thread_id: ThreadId,
    pub(crate) id: String,
    pub(crate) run_id: String,
    pub(crate) status: ScheduleReportStatus,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct ScheduleJobSummary {
    id: String,
    status: StoredScheduleStatus,
    prompt: String,
    trigger: ScheduleTriggerSummary,
    concurrency: ScheduleConcurrency,
    misfire_policy: ScheduleMisfirePolicy,
    durable: bool,
    created_at: Option<String>,
    updated_at: Option<String>,
    next_run_at: Option<String>,
    run_count: u32,
    active_run: Option<ScheduleActiveRunSummary>,
    history: Vec<ScheduleExecutionRecordSummary>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum ScheduleTriggerSummary {
    Time {
        schedule: TimeScheduleSummary,
    },
    Poll {
        every: String,
        until: String,
        timeout_at: Option<String>,
        max_runs: Option<u32>,
    },
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum TimeScheduleSummary {
    At { at: String },
    In { after: String },
    Every { every: String, anchor_at: String },
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct ScheduleActiveRunSummary {
    run_id: String,
    started_at: Option<String>,
    mode: ScheduleExecutionMode,
    thread_id: Option<ThreadId>,
    report: Option<SchedulePendingReportSummary>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct SchedulePendingReportSummary {
    status: ScheduleReportStatus,
    summary: Option<String>,
    reported_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct ScheduleExecutionRecordSummary {
    run_id: String,
    started_at: Option<String>,
    finished_at: Option<String>,
    disposition: ScheduleExecutionDisposition,
    summary: Option<String>,
    thread_id: Option<ThreadId>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub(crate) struct ScheduledTurnConfig {
    pub(crate) cwd: PathBuf,
    pub(crate) approval_policy: AskForApproval,
    pub(crate) sandbox_policy: SandboxPolicy,
    pub(crate) collaboration_mode: CollaborationMode,
    pub(crate) reasoning_summary: ReasoningSummaryConfig,
    pub(crate) service_tier: Option<ServiceTier>,
    pub(crate) personality: Option<Personality>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
struct ScheduleStore {
    version: u32,
    jobs: Vec<StoredScheduleJob>,
}

impl Default for ScheduleStore {
    fn default() -> Self {
        Self {
            version: STORE_VERSION,
            jobs: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
struct StoredScheduleJob {
    id: String,
    prompt: String,
    created_at: i64,
    updated_at: i64,
    status: StoredScheduleStatus,
    trigger: StoredScheduleTrigger,
    concurrency: ScheduleConcurrency,
    misfire_policy: ScheduleMisfirePolicy,
    durable: bool,
    turn: ScheduledTurnConfig,
    next_run_at: Option<i64>,
    run_count: u32,
    active_run: Option<ActiveScheduleRun>,
    history: Vec<ScheduleExecutionRecord>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
enum StoredScheduleTrigger {
    Time { schedule: TimeScheduleTrigger },
    Poll { poll: PollScheduleTrigger },
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum TimeScheduleTrigger {
    At { at: i64 },
    In { delay_seconds: u64 },
    Every { every_seconds: u64, anchor_at: i64 },
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
struct PollScheduleTrigger {
    every_seconds: u64,
    until: String,
    timeout_at: Option<i64>,
    max_runs: Option<u32>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
struct ActiveScheduleRun {
    id: String,
    started_at: i64,
    mode: ScheduleExecutionMode,
    thread_id: Option<ThreadId>,
    sub_id: Option<String>,
    report: Option<PendingScheduleReport>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
struct PendingScheduleReport {
    status: ScheduleReportStatus,
    summary: Option<String>,
    reported_at: i64,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
struct ScheduleExecutionRecord {
    run_id: String,
    started_at: Option<i64>,
    finished_at: Option<i64>,
    disposition: ScheduleExecutionDisposition,
    summary: Option<String>,
    thread_id: Option<ThreadId>,
}

#[derive(Debug, Clone)]
struct ClaimedScheduleRun {
    job_id: String,
    run_id: String,
    prompt: String,
    trigger: StoredScheduleTrigger,
    turn: ScheduledTurnConfig,
}

#[derive(Debug, Clone)]
enum FinalizeReason {
    Completed { summary: Option<String> },
    Aborted { summary: Option<String> },
    LaunchFailed { summary: String },
}

pub(crate) struct SchedulerService {
    enabled: bool,
    codex_home: PathBuf,
    owner_thread_id: ThreadId,
    wake: Notify,
    started: AtomicBool,
    shutdown: CancellationToken,
}

impl SchedulerService {
    pub(crate) fn new(enabled: bool, codex_home: PathBuf, owner_thread_id: ThreadId) -> Self {
        Self {
            enabled,
            codex_home,
            owner_thread_id,
            wake: Notify::new(),
            started: AtomicBool::new(false),
            shutdown: CancellationToken::new(),
        }
    }

    pub(crate) fn start(self: &Arc<Self>, session: Arc<Session>) {
        if !self.enabled || self.started.swap(true, Ordering::SeqCst) {
            return;
        }

        let weak_session = Arc::downgrade(&session);
        let service = Arc::clone(self);
        tokio::spawn(async move {
            service.run_loop(weak_session).await;
        });
    }

    pub(crate) async fn shutdown_owned_thread(
        &self,
        agent_control: &AgentControl,
    ) -> Result<(), String> {
        self.shutdown.cancel();
        self.wake.notify_waiters();
        if !self.enabled {
            return Ok(());
        }

        let jobs = self.read_jobs(self.owner_thread_id).await?;
        let active_runs = jobs
            .into_iter()
            .filter_map(|job| {
                job.active_run
                    .as_ref()
                    .map(|run| (job.id.clone(), run.id.clone(), run.thread_id))
            })
            .collect::<Vec<_>>();

        for (job_id, run_id, child_thread_id) in active_runs {
            self.finalize_run(
                self.owner_thread_id,
                job_id.as_str(),
                run_id.as_str(),
                FinalizeReason::Aborted {
                    summary: Some(
                        "Owner thread shut down before the scheduled run finished.".to_string(),
                    ),
                },
            )
            .await?;
            if let Some(child_thread_id) = child_thread_id {
                let _ = agent_control.shutdown_agent(child_thread_id).await;
            }
        }

        Ok(())
    }

    pub(crate) async fn create_job(
        &self,
        turn: &TurnContext,
        args: ScheduleCreateArgs,
    ) -> Result<ScheduleJobSummary, String> {
        self.ensure_enabled()?;

        let now = OffsetDateTime::now_utc().unix_timestamp();
        let prompt = args.prompt.trim().to_string();
        if prompt.is_empty() {
            return Err("prompt must not be empty".to_string());
        }

        if args.durable == Some(false) {
            return Err("durable must be true for persistent schedules".to_string());
        }

        if matches!(turn.collaboration_mode.mode, ModeKind::Plan) {
            return Err("scheduled jobs cannot be created from Plan mode turns".to_string());
        }

        let (trigger, next_run_at) = build_trigger(&args, now)?;
        let job = StoredScheduleJob {
            id: Uuid::new_v4().to_string(),
            prompt,
            created_at: now,
            updated_at: now,
            status: StoredScheduleStatus::Active,
            trigger,
            concurrency: args.concurrency.unwrap_or_default(),
            misfire_policy: args.misfire_policy.unwrap_or_default(),
            durable: true,
            turn: capture_turn_config(turn),
            next_run_at,
            run_count: 0,
            active_run: None,
            history: Vec::new(),
        };

        let summary = self
            .with_store_mut(self.owner_thread_id, |store| {
                store.jobs.push(job.clone());
                Ok(schedule_job_summary(&job))
            })
            .await?;
        self.wake.notify_waiters();
        Ok(summary)
    }

    pub(crate) async fn list_jobs(
        &self,
        thread_id: ThreadId,
    ) -> Result<Vec<ScheduleJobSummary>, String> {
        self.ensure_enabled()?;
        let mut jobs = self
            .read_jobs(thread_id)
            .await?
            .into_iter()
            .map(|job| schedule_job_summary(&job))
            .collect::<Vec<_>>();
        jobs.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(jobs)
    }

    pub(crate) async fn delete_job(
        &self,
        thread_id: ThreadId,
        id: &str,
    ) -> Result<ScheduleJobSummary, String> {
        self.ensure_enabled()?;
        let deleted = self
            .with_store_mut(thread_id, |store| {
                let index = find_job_index(store, id)?;
                Ok(schedule_job_summary(&store.jobs.remove(index)))
            })
            .await?;
        self.wake.notify_waiters();
        Ok(deleted)
    }

    pub(crate) async fn pause_job(
        &self,
        thread_id: ThreadId,
        id: &str,
    ) -> Result<ScheduleJobSummary, String> {
        self.ensure_enabled()?;
        let summary = self
            .with_store_mut(thread_id, |store| {
                let job = find_job_mut(store, id)?;
                if matches!(
                    job.status,
                    StoredScheduleStatus::Completed | StoredScheduleStatus::Failed
                ) {
                    return Err("only active schedules can be paused".to_string());
                }
                job.status = StoredScheduleStatus::Paused;
                job.updated_at = OffsetDateTime::now_utc().unix_timestamp();
                Ok(schedule_job_summary(job))
            })
            .await?;
        self.wake.notify_waiters();
        Ok(summary)
    }

    pub(crate) async fn resume_job(
        &self,
        thread_id: ThreadId,
        id: &str,
    ) -> Result<ScheduleJobSummary, String> {
        self.ensure_enabled()?;
        let now = OffsetDateTime::now_utc().unix_timestamp();
        let summary = self
            .with_store_mut(thread_id, |store| {
                let job = find_job_mut(store, id)?;
                if !matches!(job.status, StoredScheduleStatus::Paused) {
                    return Err("only paused schedules can be resumed".to_string());
                }
                job.status = StoredScheduleStatus::Active;
                if job.next_run_at.is_none() {
                    job.next_run_at = next_run_after_resume(job, now);
                }
                job.updated_at = now;
                Ok(schedule_job_summary(job))
            })
            .await?;
        self.wake.notify_waiters();
        Ok(summary)
    }

    pub(crate) async fn record_report(
        &self,
        args: ScheduleReportArgs,
    ) -> Result<ScheduleReportAck, String> {
        self.ensure_enabled()?;
        let thread_id = parse_thread_id(args.thread_id.as_str())?;
        let status = self
            .with_store_mut(thread_id, |store| {
                let job = find_job_mut(store, args.id.as_str())?;
                let Some(active_run) = job.active_run.as_mut() else {
                    return Err("schedule has no active run to report".to_string());
                };
                if active_run.id != args.run_id {
                    return Err("run_id does not match the active scheduled run".to_string());
                }
                let StoredScheduleTrigger::Poll { .. } = job.trigger else {
                    return Err("schedule_report is only valid for poll schedules".to_string());
                };
                active_run.report = Some(PendingScheduleReport {
                    status: args.status,
                    summary: args.summary.clone(),
                    reported_at: OffsetDateTime::now_utc().unix_timestamp(),
                });
                job.updated_at = OffsetDateTime::now_utc().unix_timestamp();
                Ok(args.status)
            })
            .await?;

        Ok(ScheduleReportAck {
            thread_id,
            id: args.id,
            run_id: args.run_id,
            status,
        })
    }

    async fn run_loop(self: Arc<Self>, weak_session: std::sync::Weak<Session>) {
        loop {
            if self.shutdown.is_cancelled() {
                break;
            }

            let Some(session) = weak_session.upgrade() else {
                break;
            };

            if let Err(err) = self.process_due_jobs(&session).await {
                warn!(
                    thread_id = %self.owner_thread_id,
                    "scheduler failed processing due jobs: {err}"
                );
            }

            let wait = self
                .next_wait_duration(self.owner_thread_id)
                .await
                .unwrap_or_else(|| Duration::from_secs(DEFAULT_IDLE_WAIT_SECONDS));
            tokio::select! {
                _ = self.shutdown.cancelled() => break,
                _ = self.wake.notified() => {}
                _ = tokio::time::sleep(wait) => {}
            }
        }
    }

    async fn process_due_jobs(&self, session: &Arc<Session>) -> Result<(), String> {
        let now = OffsetDateTime::now_utc().unix_timestamp();
        let jobs = self.read_jobs(self.owner_thread_id).await?;

        let active_turn = session.active_turn.lock().await.is_some();
        let mut launched_queue = false;

        for job in jobs {
            if !matches!(job.status, StoredScheduleStatus::Active) || job.active_run.is_some() {
                continue;
            }

            let Some(next_run_at) = job.next_run_at else {
                continue;
            };
            if next_run_at > now {
                continue;
            }

            if should_skip_misfire(&job, now) {
                self.skip_occurrence(
                    self.owner_thread_id,
                    job.id.as_str(),
                    "Skipped a missed scheduled occurrence.".to_string(),
                    now,
                )
                .await?;
                continue;
            }

            if matches!(job.trigger, StoredScheduleTrigger::Poll { ref poll } if poll_timeout_expired(poll, now))
            {
                self.fail_inactive_poll_job(
                    self.owner_thread_id,
                    job.id.as_str(),
                    "Poll schedule timed out before the next run started.".to_string(),
                    now,
                )
                .await?;
                continue;
            }

            match job.concurrency {
                ScheduleConcurrency::Queue => {
                    if active_turn || launched_queue {
                        continue;
                    }
                    if self.launch_queue_run(session, job.id.clone(), now).await? {
                        launched_queue = true;
                    }
                }
                ScheduleConcurrency::Skip => {
                    if active_turn || launched_queue {
                        self.skip_occurrence(
                            self.owner_thread_id,
                            job.id.as_str(),
                            "Skipped the scheduled occurrence because the thread was busy."
                                .to_string(),
                            now,
                        )
                        .await?;
                        continue;
                    }
                    if self.launch_queue_run(session, job.id.clone(), now).await? {
                        launched_queue = true;
                    }
                }
                ScheduleConcurrency::Parallel => {
                    let _ = self
                        .launch_parallel_run(session, job.id.clone(), now)
                        .await?;
                }
            }
        }

        Ok(())
    }

    async fn launch_queue_run(
        &self,
        session: &Arc<Session>,
        job_id: String,
        now: i64,
    ) -> Result<bool, String> {
        let sub_id = format!("schedule-{job_id}-{}", Uuid::new_v4());
        let claimed = match self
            .claim_run(
                self.owner_thread_id,
                job_id.as_str(),
                now,
                ScheduleExecutionMode::Queue,
                Some(sub_id.clone()),
            )
            .await?
        {
            Some(claimed) => claimed,
            None => return Ok(false),
        };

        let turn_context = match session
            .new_scheduled_turn_with_sub_id(sub_id.clone(), &claimed.turn)
            .await
        {
            Ok(turn_context) => turn_context,
            Err(err) => {
                self.finalize_run(
                    self.owner_thread_id,
                    claimed.job_id.as_str(),
                    claimed.run_id.as_str(),
                    FinalizeReason::LaunchFailed { summary: err },
                )
                .await?;
                return Ok(false);
            }
        };

        let input = scheduled_input(
            self.owner_thread_id,
            claimed.job_id.as_str(),
            claimed.run_id.as_str(),
            claimed.prompt.as_str(),
            &claimed.trigger,
        );
        session
            .spawn_task(Arc::clone(&turn_context), input, RegularTask::default())
            .await;

        let service = Arc::new(Self {
            enabled: self.enabled,
            codex_home: self.codex_home.clone(),
            owner_thread_id: self.owner_thread_id,
            wake: Notify::new(),
            started: AtomicBool::new(self.started.load(Ordering::SeqCst)),
            shutdown: self.shutdown.child_token(),
        });
        let weak_session = Arc::downgrade(session);
        tokio::spawn(async move {
            loop {
                if service.shutdown.is_cancelled() {
                    break;
                }

                let Some(session) = weak_session.upgrade() else {
                    let _ = service
                        .finalize_run(
                            service.owner_thread_id,
                            claimed.job_id.as_str(),
                            claimed.run_id.as_str(),
                            FinalizeReason::Aborted {
                                summary: Some(
                                    "The owner thread disappeared before the scheduled run finished."
                                        .to_string(),
                                ),
                            },
                        )
                        .await;
                    break;
                };

                let still_running = session
                    .active_turn
                    .lock()
                    .await
                    .as_ref()
                    .is_some_and(|turn| turn.tasks.contains_key(sub_id.as_str()));
                if !still_running {
                    let _ = service
                        .finalize_run(
                            service.owner_thread_id,
                            claimed.job_id.as_str(),
                            claimed.run_id.as_str(),
                            FinalizeReason::Completed { summary: None },
                        )
                        .await;
                    break;
                }

                tokio::time::sleep(Duration::from_secs(LOOP_TICK_SECONDS)).await;
            }
        });

        Ok(true)
    }

    async fn launch_parallel_run(
        &self,
        session: &Arc<Session>,
        job_id: String,
        now: i64,
    ) -> Result<bool, String> {
        let claimed = match self
            .claim_run(
                self.owner_thread_id,
                job_id.as_str(),
                now,
                ScheduleExecutionMode::Parallel,
                None,
            )
            .await?
        {
            Some(claimed) => claimed,
            None => return Ok(false),
        };

        let child_config = match session.build_scheduled_spawn_config(&claimed.turn).await {
            Ok(child_config) => child_config,
            Err(err) => {
                self.finalize_run(
                    self.owner_thread_id,
                    claimed.job_id.as_str(),
                    claimed.run_id.as_str(),
                    FinalizeReason::LaunchFailed { summary: err },
                )
                .await?;
                return Ok(false);
            }
        };
        let input = scheduled_input(
            self.owner_thread_id,
            claimed.job_id.as_str(),
            claimed.run_id.as_str(),
            claimed.prompt.as_str(),
            &claimed.trigger,
        );
        let session_source = SessionSource::SubAgent(SubAgentSource::Other(format!(
            "schedule:{}:{}",
            self.owner_thread_id, claimed.job_id
        )));
        let child_thread_id = match session
            .services
            .agent_control
            .spawn_agent(child_config, input, Some(session_source))
            .await
        {
            Ok(child_thread_id) => child_thread_id,
            Err(err) => {
                self.finalize_run(
                    self.owner_thread_id,
                    claimed.job_id.as_str(),
                    claimed.run_id.as_str(),
                    FinalizeReason::LaunchFailed {
                        summary: err.to_string(),
                    },
                )
                .await?;
                return Ok(false);
            }
        };
        self.attach_parallel_thread(
            self.owner_thread_id,
            claimed.job_id.as_str(),
            claimed.run_id.as_str(),
            child_thread_id,
        )
        .await?;

        let codex_home = self.codex_home.clone();
        let owner_thread_id = self.owner_thread_id;
        let shutdown = self.shutdown.child_token();
        let agent_control = session.services.agent_control.clone();
        tokio::spawn(async move {
            let wait_status = async {
                let mut rx = agent_control.subscribe_status(child_thread_id).await.ok();
                loop {
                    let status = match rx.as_mut() {
                        Some(status_rx) => status_rx.borrow().clone(),
                        None => agent_control.get_status(child_thread_id).await,
                    };
                    if is_final(&status) {
                        return status;
                    }
                    match rx.as_mut() {
                        Some(status_rx) => {
                            if status_rx.changed().await.is_err() {
                                rx = None;
                            }
                        }
                        None => tokio::time::sleep(Duration::from_secs(LOOP_TICK_SECONDS)).await,
                    }
                }
            };

            let status = tokio::select! {
                _ = shutdown.cancelled() => AgentStatus::Shutdown,
                status = wait_status => status,
            };
            let service = SchedulerService::new(true, codex_home, owner_thread_id);
            let reason = match status {
                AgentStatus::Completed(last_agent_message) => FinalizeReason::Completed {
                    summary: last_agent_message,
                },
                AgentStatus::Errored(message) => FinalizeReason::Aborted {
                    summary: Some(message),
                },
                AgentStatus::Shutdown | AgentStatus::NotFound => FinalizeReason::Aborted {
                    summary: Some(
                        "The parallel scheduled agent shut down before reporting completion."
                            .to_string(),
                    ),
                },
                AgentStatus::PendingInit | AgentStatus::Running => FinalizeReason::Aborted {
                    summary: Some("The parallel scheduled agent stopped unexpectedly.".to_string()),
                },
            };
            let _ = service
                .finalize_run(
                    owner_thread_id,
                    claimed.job_id.as_str(),
                    claimed.run_id.as_str(),
                    reason,
                )
                .await;
            let _ = agent_control.shutdown_agent(child_thread_id).await;
        });

        Ok(true)
    }

    async fn claim_run(
        &self,
        thread_id: ThreadId,
        job_id: &str,
        now: i64,
        mode: ScheduleExecutionMode,
        sub_id: Option<String>,
    ) -> Result<Option<ClaimedScheduleRun>, String> {
        self.with_store_mut(thread_id, |store| {
            let job = match store.jobs.iter_mut().find(|job| job.id == job_id) {
                Some(job) => job,
                None => return Ok(None),
            };
            if !matches!(job.status, StoredScheduleStatus::Active) || job.active_run.is_some() {
                return Ok(None);
            }
            let Some(next_run_at) = job.next_run_at else {
                return Ok(None);
            };
            if next_run_at > now {
                return Ok(None);
            }

            let run_id = Uuid::new_v4().to_string();
            job.active_run = Some(ActiveScheduleRun {
                id: run_id.clone(),
                started_at: now,
                mode,
                thread_id: None,
                sub_id: sub_id.clone(),
                report: None,
            });
            job.updated_at = now;

            Ok(Some(ClaimedScheduleRun {
                job_id: job.id.clone(),
                run_id,
                prompt: job.prompt.clone(),
                trigger: job.trigger.clone(),
                turn: job.turn.clone(),
            }))
        })
        .await
    }

    async fn attach_parallel_thread(
        &self,
        thread_id: ThreadId,
        job_id: &str,
        run_id: &str,
        child_thread_id: ThreadId,
    ) -> Result<(), String> {
        self.with_store_mut(thread_id, |store| {
            let job = find_job_mut(store, job_id)?;
            let Some(active_run) = job.active_run.as_mut() else {
                return Ok(());
            };
            if active_run.id == run_id {
                active_run.thread_id = Some(child_thread_id);
                job.updated_at = OffsetDateTime::now_utc().unix_timestamp();
            }
            Ok(())
        })
        .await
    }

    async fn finalize_run(
        &self,
        thread_id: ThreadId,
        job_id: &str,
        run_id: &str,
        reason: FinalizeReason,
    ) -> Result<(), String> {
        self.with_store_mut(thread_id, |store| {
            let job = match store.jobs.iter_mut().find(|job| job.id == job_id) {
                Some(job) => job,
                None => return Ok(()),
            };
            let Some(active_run) = job.active_run.clone() else {
                return Ok(());
            };
            if active_run.id != run_id {
                return Ok(());
            }

            let now = OffsetDateTime::now_utc().unix_timestamp();
            let mut summary = match &reason {
                FinalizeReason::Completed { summary } | FinalizeReason::Aborted { summary } => {
                    summary.clone()
                }
                FinalizeReason::LaunchFailed { summary } => Some(summary.clone()),
            };

            let paused = matches!(job.status, StoredScheduleStatus::Paused);
            let disposition = match &job.trigger {
                StoredScheduleTrigger::Time { schedule } => {
                    let disposition = match &reason {
                        FinalizeReason::Completed { .. } => ScheduleExecutionDisposition::Executed,
                        FinalizeReason::Aborted { .. } => ScheduleExecutionDisposition::Aborted,
                        FinalizeReason::LaunchFailed { .. } => ScheduleExecutionDisposition::Failed,
                    };
                    job.run_count = job.run_count.saturating_add(1);
                    job.next_run_at = match schedule {
                        TimeScheduleTrigger::At { .. } | TimeScheduleTrigger::In { .. } => None,
                        TimeScheduleTrigger::Every {
                            every_seconds,
                            anchor_at,
                        } => Some(next_every_occurrence(*anchor_at, *every_seconds, now)),
                    };
                    job.status = match schedule {
                        TimeScheduleTrigger::At { .. } | TimeScheduleTrigger::In { .. } => {
                            match &reason {
                                FinalizeReason::LaunchFailed { .. } => StoredScheduleStatus::Failed,
                                FinalizeReason::Completed { .. }
                                | FinalizeReason::Aborted { .. } => StoredScheduleStatus::Completed,
                            }
                        }
                        TimeScheduleTrigger::Every { .. } => {
                            if paused {
                                StoredScheduleStatus::Paused
                            } else {
                                StoredScheduleStatus::Active
                            }
                        }
                    };
                    disposition
                }
                StoredScheduleTrigger::Poll { poll } => {
                    let (next_status, next_run_at, disposition, limit_summary) =
                        finalize_poll_run(job, &active_run, poll, now, &reason);
                    if summary.is_none() {
                        summary = limit_summary;
                    }
                    job.run_count = job.run_count.saturating_add(1);
                    job.status = next_status;
                    job.next_run_at = next_run_at;
                    disposition
                }
            };

            push_history_record(
                job,
                ScheduleExecutionRecord {
                    run_id: active_run.id,
                    started_at: Some(active_run.started_at),
                    finished_at: Some(now),
                    disposition,
                    summary,
                    thread_id: active_run.thread_id,
                },
            );
            job.active_run = None;
            job.updated_at = now;
            Ok(())
        })
        .await?;
        self.wake.notify_waiters();
        Ok(())
    }

    async fn skip_occurrence(
        &self,
        thread_id: ThreadId,
        job_id: &str,
        summary: String,
        now: i64,
    ) -> Result<(), String> {
        self.with_store_mut(thread_id, |store| {
            let job = find_job_mut(store, job_id)?;
            if !matches!(job.status, StoredScheduleStatus::Active) || job.active_run.is_some() {
                return Ok(());
            }

            let disposition = ScheduleExecutionDisposition::Skipped;
            match &job.trigger {
                StoredScheduleTrigger::Time { schedule } => match schedule {
                    TimeScheduleTrigger::At { .. } | TimeScheduleTrigger::In { .. } => {
                        job.next_run_at = None;
                        job.status = StoredScheduleStatus::Completed;
                    }
                    TimeScheduleTrigger::Every {
                        every_seconds,
                        anchor_at,
                    } => {
                        job.next_run_at =
                            Some(next_every_occurrence(*anchor_at, *every_seconds, now));
                    }
                },
                StoredScheduleTrigger::Poll { poll } => {
                    if poll_timeout_expired(poll, now) {
                        job.next_run_at = None;
                        job.status = StoredScheduleStatus::Failed;
                    } else {
                        job.next_run_at =
                            Some(now.saturating_add(
                                i64::try_from(poll.every_seconds).unwrap_or(i64::MAX),
                            ));
                    }
                }
            }

            push_history_record(
                job,
                ScheduleExecutionRecord {
                    run_id: Uuid::new_v4().to_string(),
                    started_at: None,
                    finished_at: Some(now),
                    disposition,
                    summary: Some(summary),
                    thread_id: None,
                },
            );
            job.updated_at = now;
            Ok(())
        })
        .await?;
        self.wake.notify_waiters();
        Ok(())
    }

    async fn fail_inactive_poll_job(
        &self,
        thread_id: ThreadId,
        job_id: &str,
        summary: String,
        now: i64,
    ) -> Result<(), String> {
        self.with_store_mut(thread_id, |store| {
            let job = find_job_mut(store, job_id)?;
            let StoredScheduleTrigger::Poll { .. } = job.trigger else {
                return Ok(());
            };
            if job.active_run.is_some() {
                return Ok(());
            }
            job.next_run_at = None;
            job.status = StoredScheduleStatus::Failed;
            push_history_record(
                job,
                ScheduleExecutionRecord {
                    run_id: Uuid::new_v4().to_string(),
                    started_at: None,
                    finished_at: Some(now),
                    disposition: ScheduleExecutionDisposition::Failed,
                    summary: Some(summary),
                    thread_id: None,
                },
            );
            job.updated_at = now;
            Ok(())
        })
        .await
    }

    async fn next_wait_duration(&self, thread_id: ThreadId) -> Option<Duration> {
        let now = OffsetDateTime::now_utc().unix_timestamp();
        let jobs = self.read_jobs(thread_id).await.ok()?;
        let next_run_at = jobs
            .into_iter()
            .filter(|job| {
                matches!(job.status, StoredScheduleStatus::Active) && job.active_run.is_none()
            })
            .filter_map(|job| job.next_run_at)
            .min()?;
        let seconds = if next_run_at <= now {
            LOOP_TICK_SECONDS
        } else {
            u64::try_from(next_run_at.saturating_sub(now)).ok()?
        };
        Some(Duration::from_secs(seconds.min(DEFAULT_IDLE_WAIT_SECONDS)))
    }

    async fn read_jobs(&self, thread_id: ThreadId) -> Result<Vec<StoredScheduleJob>, String> {
        self.with_store(thread_id, |store| Ok(store.jobs.clone()))
            .await
    }

    async fn with_store<T, F>(&self, thread_id: ThreadId, read: F) -> Result<T, String>
    where
        F: FnOnce(&ScheduleStore) -> Result<T, String>,
    {
        let path = schedule_store_path(self.codex_home.as_path(), thread_id);
        let lock = store_lock(path.as_path());
        let _guard = lock.lock().await;
        let store = read_store(path.as_path()).await?;
        read(&store)
    }

    async fn with_store_mut<T, F>(&self, thread_id: ThreadId, mutate: F) -> Result<T, String>
    where
        F: FnOnce(&mut ScheduleStore) -> Result<T, String>,
    {
        let path = schedule_store_path(self.codex_home.as_path(), thread_id);
        let lock = store_lock(path.as_path());
        let _guard = lock.lock().await;
        let mut store = read_store(path.as_path()).await?;
        let result = mutate(&mut store)?;
        write_store(path.as_path(), &store).await?;
        Ok(result)
    }

    fn ensure_enabled(&self) -> Result<(), String> {
        if self.enabled {
            Ok(())
        } else {
            Err("scheduler tools are disabled; enable the `scheduler` feature first".to_string())
        }
    }
}

fn capture_turn_config(turn: &TurnContext) -> ScheduledTurnConfig {
    ScheduledTurnConfig {
        cwd: turn.cwd.clone(),
        approval_policy: turn.approval_policy.value(),
        sandbox_policy: turn.sandbox_policy.get().clone(),
        collaboration_mode: turn.collaboration_mode.clone(),
        reasoning_summary: turn.reasoning_summary,
        service_tier: turn.config.service_tier,
        personality: turn.personality,
    }
}

fn build_trigger(
    args: &ScheduleCreateArgs,
    now: i64,
) -> Result<(StoredScheduleTrigger, Option<i64>), String> {
    match args.trigger {
        ScheduleTriggerType::Time => {
            let schedule = args
                .schedule
                .as_ref()
                .ok_or_else(|| "time schedules require `schedule`".to_string())?;
            if args.poll.is_some() {
                return Err("time schedules do not accept `poll`".to_string());
            }
            let selected = [
                schedule.at.as_ref().map(|at| ("at", at.as_str())),
                schedule.after.as_ref().map(|after| ("in", after.as_str())),
                schedule
                    .every
                    .as_ref()
                    .map(|every| ("every", every.as_str())),
            ]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
            if selected.len() != 1 {
                return Err("time schedules require exactly one of `schedule.at`, `schedule.in`, or `schedule.every`".to_string());
            }
            let (kind, value) = selected[0];
            match kind {
                "at" => {
                    let ts = parse_rfc3339_timestamp(value)?;
                    Ok((
                        StoredScheduleTrigger::Time {
                            schedule: TimeScheduleTrigger::At { at: ts },
                        },
                        Some(ts),
                    ))
                }
                "in" => {
                    let delay_seconds = parse_delay_seconds(value)?;
                    Ok((
                        StoredScheduleTrigger::Time {
                            schedule: TimeScheduleTrigger::In { delay_seconds },
                        },
                        Some(now.saturating_add(i64::try_from(delay_seconds).unwrap_or(i64::MAX))),
                    ))
                }
                "every" => {
                    let every_seconds = parse_interval_seconds(value)?;
                    Ok((
                        StoredScheduleTrigger::Time {
                            schedule: TimeScheduleTrigger::Every {
                                every_seconds,
                                anchor_at: now,
                            },
                        },
                        Some(now.saturating_add(i64::try_from(every_seconds).unwrap_or(i64::MAX))),
                    ))
                }
                _ => unreachable!("validated schedule kind"),
            }
        }
        ScheduleTriggerType::Poll => {
            if args.schedule.is_some() {
                return Err("poll schedules do not accept `schedule`".to_string());
            }
            let poll = args
                .poll
                .as_ref()
                .ok_or_else(|| "poll schedules require `poll`".to_string())?;
            if poll.until.trim().is_empty() {
                return Err("poll.until must not be empty".to_string());
            }
            let every_seconds = parse_interval_seconds(poll.every.as_str())?;
            let timeout_at = match poll.timeout.as_deref() {
                Some(timeout) => {
                    let timeout_seconds = parse_delay_seconds(timeout)?;
                    Some(now.saturating_add(i64::try_from(timeout_seconds).unwrap_or(i64::MAX)))
                }
                None => None,
            };
            if matches!(poll.max_runs, Some(0)) {
                return Err("poll.max_runs must be greater than zero".to_string());
            }
            Ok((
                StoredScheduleTrigger::Poll {
                    poll: PollScheduleTrigger {
                        every_seconds,
                        until: poll.until.trim().to_string(),
                        timeout_at,
                        max_runs: poll.max_runs,
                    },
                },
                Some(now.saturating_add(i64::try_from(every_seconds).unwrap_or(i64::MAX))),
            ))
        }
    }
}

fn parse_rfc3339_timestamp(value: &str) -> Result<i64, String> {
    OffsetDateTime::parse(value.trim(), &Rfc3339)
        .map(OffsetDateTime::unix_timestamp)
        .map_err(|err| format!("invalid RFC3339 timestamp `{value}`: {err}"))
}

fn parse_delay_seconds(value: &str) -> Result<u64, String> {
    let seconds = parse_duration_seconds(value)?;
    if seconds < MIN_DELAY_SECONDS {
        return Err(format!(
            "duration `{value}` is too small; minimum delay is {MIN_DELAY_SECONDS}s"
        ));
    }
    Ok(seconds)
}

fn parse_interval_seconds(value: &str) -> Result<u64, String> {
    let seconds = parse_duration_seconds(value)?;
    if seconds < MIN_INTERVAL_SECONDS {
        return Err(format!(
            "duration `{value}` is too small; minimum interval is {MIN_INTERVAL_SECONDS}s"
        ));
    }
    Ok(seconds)
}

fn parse_duration_seconds(value: &str) -> Result<u64, String> {
    let mut total = 0_u64;
    let mut chars = value.trim().chars().peekable();
    let mut consumed_any = false;

    while chars.peek().is_some() {
        while chars.peek().is_some_and(|char| char.is_whitespace()) {
            chars.next();
        }
        if chars.peek().is_none() {
            break;
        }

        let mut digits = String::new();
        while chars.peek().is_some_and(char::is_ascii_digit) {
            let Some(next_char) = chars.next() else {
                break;
            };
            digits.push(next_char);
        }
        if digits.is_empty() {
            return Err(format!("invalid duration `{value}`"));
        }
        let amount = digits
            .parse::<u64>()
            .map_err(|err| format!("invalid duration `{value}`: {err}"))?;

        let mut unit = String::new();
        while chars.peek().is_some_and(char::is_ascii_alphabetic) {
            let Some(next_char) = chars.next() else {
                break;
            };
            unit.push(next_char);
        }
        let multiplier = match unit.to_ascii_lowercase().as_str() {
            "" | "s" | "sec" | "secs" | "second" | "seconds" => 1_u64,
            "m" | "min" | "mins" | "minute" | "minutes" => 60_u64,
            "h" | "hr" | "hrs" | "hour" | "hours" => 60_u64 * 60,
            "d" | "day" | "days" => 60_u64 * 60 * 24,
            _ => return Err(format!("invalid duration unit `{unit}` in `{value}`")),
        };
        total = total
            .checked_add(amount.saturating_mul(multiplier))
            .ok_or_else(|| format!("duration `{value}` is too large"))?;
        consumed_any = true;
    }

    if !consumed_any || total == 0 {
        return Err(format!("duration `{value}` must be greater than zero"));
    }
    Ok(total)
}

fn schedule_store_path(codex_home: &Path, thread_id: ThreadId) -> PathBuf {
    codex_home
        .join(SCHEDULES_DIR)
        .join(format!("{thread_id}.json"))
}

fn store_lock(path: &Path) -> Arc<Mutex<()>> {
    let mut locks = STORE_LOCKS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    locks
        .entry(path.to_path_buf())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

async fn read_store(path: &Path) -> Result<ScheduleStore, String> {
    let bytes = match fs::read(path).await {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ScheduleStore::default());
        }
        Err(err) => {
            return Err(format!(
                "failed reading schedule store {}: {err}",
                path.display()
            ));
        }
    };

    let store: ScheduleStore = serde_json::from_slice(&bytes)
        .map_err(|err| format!("failed parsing schedule store {}: {err}", path.display()))?;
    if store.version != STORE_VERSION {
        return Err(format!(
            "unsupported schedule store version {} in {}",
            store.version,
            path.display()
        ));
    }
    Ok(store)
}

async fn write_store(path: &Path, store: &ScheduleStore) -> Result<(), String> {
    if store.jobs.is_empty() {
        match fs::remove_file(path).await {
            Ok(()) => return Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(err) => {
                return Err(format!(
                    "failed removing empty schedule store {}: {err}",
                    path.display()
                ));
            }
        }
    }

    let Some(dir) = path.parent() else {
        return Err(format!(
            "schedule store path has no parent: {}",
            path.display()
        ));
    };
    fs::create_dir_all(dir).await.map_err(|err| {
        format!(
            "failed creating schedule directory {}: {err}",
            dir.display()
        )
    })?;

    let bytes = serde_json::to_vec_pretty(store).map_err(|err| {
        format!(
            "failed serializing schedule store {}: {err}",
            path.display()
        )
    })?;
    let temp_path = dir.join(format!(
        ".{}.{}.tmp",
        path.file_stem()
            .and_then(|stem| stem.to_str())
            .unwrap_or("schedule"),
        Uuid::new_v4()
    ));
    fs::write(&temp_path, bytes).await.map_err(|err| {
        format!(
            "failed writing temp schedule store {}: {err}",
            temp_path.display()
        )
    })?;
    fs::rename(&temp_path, path)
        .await
        .map_err(|err| format!("failed replacing schedule store {}: {err}", path.display()))
}

fn find_job_mut<'a>(
    store: &'a mut ScheduleStore,
    id: &str,
) -> Result<&'a mut StoredScheduleJob, String> {
    store
        .jobs
        .iter_mut()
        .find(|job| job.id == id)
        .ok_or_else(|| format!("schedule `{id}` not found"))
}

fn find_job_index(store: &ScheduleStore, id: &str) -> Result<usize, String> {
    store
        .jobs
        .iter()
        .position(|job| job.id == id)
        .ok_or_else(|| format!("schedule `{id}` not found"))
}

fn parse_thread_id(value: &str) -> Result<ThreadId, String> {
    ThreadId::try_from(value.to_string())
        .map_err(|err| format!("invalid thread_id `{value}`: {err}"))
}

fn should_skip_misfire(job: &StoredScheduleJob, now: i64) -> bool {
    matches!(job.misfire_policy, ScheduleMisfirePolicy::Skip)
        && job
            .next_run_at
            .is_some_and(|next_run_at| now.saturating_sub(next_run_at) > MISFIRE_GRACE_SECONDS)
}

fn poll_timeout_expired(poll: &PollScheduleTrigger, now: i64) -> bool {
    poll.timeout_at.is_some_and(|timeout_at| now >= timeout_at)
}

fn next_every_occurrence(anchor_at: i64, every_seconds: u64, now: i64) -> i64 {
    let every_seconds = i64::try_from(every_seconds).unwrap_or(i64::MAX);
    if now < anchor_at {
        return anchor_at;
    }
    let elapsed = now.saturating_sub(anchor_at);
    let steps = elapsed / every_seconds + 1;
    anchor_at.saturating_add(steps.saturating_mul(every_seconds))
}

fn next_run_after_resume(job: &StoredScheduleJob, now: i64) -> Option<i64> {
    match &job.trigger {
        StoredScheduleTrigger::Time { schedule } => match schedule {
            TimeScheduleTrigger::At { at } => Some(*at),
            TimeScheduleTrigger::In { delay_seconds } => {
                Some(now.saturating_add(i64::try_from(*delay_seconds).unwrap_or(i64::MAX)))
            }
            TimeScheduleTrigger::Every {
                every_seconds,
                anchor_at,
            } => Some(next_every_occurrence(*anchor_at, *every_seconds, now)),
        },
        StoredScheduleTrigger::Poll { poll } => {
            if poll_timeout_expired(poll, now) {
                None
            } else {
                Some(now.saturating_add(i64::try_from(poll.every_seconds).unwrap_or(i64::MAX)))
            }
        }
    }
}

fn push_history_record(job: &mut StoredScheduleJob, record: ScheduleExecutionRecord) {
    job.history.push(record);
    if job.history.len() > MAX_HISTORY_RECORDS {
        let overflow = job.history.len() - MAX_HISTORY_RECORDS;
        job.history.drain(0..overflow);
    }
}

fn finalize_poll_run(
    job: &StoredScheduleJob,
    active_run: &ActiveScheduleRun,
    poll: &PollScheduleTrigger,
    now: i64,
    reason: &FinalizeReason,
) -> (
    StoredScheduleStatus,
    Option<i64>,
    ScheduleExecutionDisposition,
    Option<String>,
) {
    if let Some(report) = active_run.report.as_ref() {
        return match report.status {
            ScheduleReportStatus::Completed => (
                StoredScheduleStatus::Completed,
                None,
                ScheduleExecutionDisposition::Completed,
                report.summary.clone(),
            ),
            ScheduleReportStatus::Failed => (
                StoredScheduleStatus::Failed,
                None,
                ScheduleExecutionDisposition::Failed,
                report.summary.clone(),
            ),
            ScheduleReportStatus::Continue => {
                if let Some(limit_summary) =
                    poll_limit_summary(poll, job.run_count.saturating_add(1), now)
                {
                    (
                        StoredScheduleStatus::Failed,
                        None,
                        ScheduleExecutionDisposition::Failed,
                        Some(limit_summary),
                    )
                } else {
                    (
                        if matches!(job.status, StoredScheduleStatus::Paused) {
                            StoredScheduleStatus::Paused
                        } else {
                            StoredScheduleStatus::Active
                        },
                        Some(
                            now.saturating_add(
                                i64::try_from(poll.every_seconds).unwrap_or(i64::MAX),
                            ),
                        ),
                        ScheduleExecutionDisposition::Continued,
                        report.summary.clone(),
                    )
                }
            }
        };
    }

    match reason {
        FinalizeReason::LaunchFailed { summary } => {
            match poll_limit_summary(poll, job.run_count.saturating_add(1), now) {
                Some(limit_summary) => (
                    StoredScheduleStatus::Failed,
                    None,
                    ScheduleExecutionDisposition::Failed,
                    Some(limit_summary),
                ),
                None => (
                    if matches!(job.status, StoredScheduleStatus::Paused) {
                        StoredScheduleStatus::Paused
                    } else {
                        StoredScheduleStatus::Active
                    },
                    Some(now.saturating_add(i64::try_from(poll.every_seconds).unwrap_or(i64::MAX))),
                    ScheduleExecutionDisposition::Failed,
                    Some(summary.clone()),
                ),
            }
        }
        FinalizeReason::Completed { summary } => {
            match poll_limit_summary(poll, job.run_count.saturating_add(1), now) {
                Some(limit_summary) => (
                    StoredScheduleStatus::Failed,
                    None,
                    ScheduleExecutionDisposition::Failed,
                    Some(limit_summary),
                ),
                None => (
                    if matches!(job.status, StoredScheduleStatus::Paused) {
                        StoredScheduleStatus::Paused
                    } else {
                        StoredScheduleStatus::Active
                    },
                    Some(now.saturating_add(i64::try_from(poll.every_seconds).unwrap_or(i64::MAX))),
                    ScheduleExecutionDisposition::Continued,
                    summary.clone().or_else(|| {
                        Some(
                            "No schedule_report was recorded; continuing the poll schedule."
                                .to_string(),
                        )
                    }),
                ),
            }
        }
        FinalizeReason::Aborted { summary } => {
            match poll_limit_summary(poll, job.run_count.saturating_add(1), now) {
                Some(limit_summary) => (
                    StoredScheduleStatus::Failed,
                    None,
                    ScheduleExecutionDisposition::Failed,
                    Some(limit_summary),
                ),
                None => (
                    if matches!(job.status, StoredScheduleStatus::Paused) {
                        StoredScheduleStatus::Paused
                    } else {
                        StoredScheduleStatus::Active
                    },
                    Some(now.saturating_add(i64::try_from(poll.every_seconds).unwrap_or(i64::MAX))),
                    ScheduleExecutionDisposition::Aborted,
                    summary.clone(),
                ),
            }
        }
    }
}

fn poll_limit_summary(poll: &PollScheduleTrigger, next_run_count: u32, now: i64) -> Option<String> {
    if poll.timeout_at.is_some_and(|timeout_at| now >= timeout_at) {
        return Some("Poll schedule timed out.".to_string());
    }
    if let Some(max_runs) = poll.max_runs
        && next_run_count >= max_runs
    {
        return Some(format!(
            "Poll schedule reached its max_runs limit of {max_runs}."
        ));
    }
    None
}

fn scheduled_input(
    owner_thread_id: ThreadId,
    job_id: &str,
    run_id: &str,
    prompt: &str,
    trigger: &StoredScheduleTrigger,
) -> Vec<UserInput> {
    let prelude = match trigger {
        StoredScheduleTrigger::Time { schedule } => {
            let schedule_label = match schedule {
                TimeScheduleTrigger::At { at } => format!(
                    "one-shot at {}",
                    format_timestamp(Some(*at)).unwrap_or_else(|| at.to_string())
                ),
                TimeScheduleTrigger::In { delay_seconds } => {
                    format!("one-shot after {}", format_duration(*delay_seconds))
                }
                TimeScheduleTrigger::Every { every_seconds, .. } => {
                    format!("recurring every {}", format_duration(*every_seconds))
                }
            };
            format!(
                "<scheduled_execution>\n\
This turn was started automatically by the persistent scheduler.\n\
thread_id: {owner_thread_id}\n\
schedule_id: {job_id}\n\
run_id: {run_id}\n\
schedule: {schedule_label}\n\
</scheduled_execution>"
            )
        }
        StoredScheduleTrigger::Poll { poll } => format!(
            "<scheduled_execution>\n\
This turn was started automatically by the persistent scheduler.\n\
thread_id: {owner_thread_id}\n\
schedule_id: {job_id}\n\
run_id: {run_id}\n\
poll_every: {}\n\
until: {}\n\
Before finishing, call `schedule_report` exactly once with:\n\
- `thread_id`: \"{owner_thread_id}\"\n\
- `id`: \"{job_id}\"\n\
- `run_id`: \"{run_id}\"\n\
- `status`: \"completed\" if the condition is satisfied, \"continue\" if it is not yet satisfied, or \"failed\" if the schedule should stop.\n\
- `summary`: one concise sentence describing the result.\n\
</scheduled_execution>",
            format_duration(poll.every_seconds),
            poll.until
        ),
    };

    vec![UserInput::Text {
        text: format!("{prelude}\n\n{prompt}"),
        text_elements: Vec::new(),
    }]
}

fn schedule_job_summary(job: &StoredScheduleJob) -> ScheduleJobSummary {
    ScheduleJobSummary {
        id: job.id.clone(),
        status: job.status,
        prompt: job.prompt.clone(),
        trigger: schedule_trigger_summary(&job.trigger),
        concurrency: job.concurrency,
        misfire_policy: job.misfire_policy,
        durable: job.durable,
        created_at: format_timestamp(Some(job.created_at)),
        updated_at: format_timestamp(Some(job.updated_at)),
        next_run_at: format_timestamp(job.next_run_at),
        run_count: job.run_count,
        active_run: job.active_run.as_ref().map(schedule_active_run_summary),
        history: job
            .history
            .iter()
            .map(schedule_execution_record_summary)
            .collect(),
    }
}

fn schedule_trigger_summary(trigger: &StoredScheduleTrigger) -> ScheduleTriggerSummary {
    match trigger {
        StoredScheduleTrigger::Time { schedule } => ScheduleTriggerSummary::Time {
            schedule: match schedule {
                TimeScheduleTrigger::At { at } => TimeScheduleSummary::At {
                    at: format_timestamp(Some(*at)).unwrap_or_else(|| at.to_string()),
                },
                TimeScheduleTrigger::In { delay_seconds } => TimeScheduleSummary::In {
                    after: format_duration(*delay_seconds),
                },
                TimeScheduleTrigger::Every {
                    every_seconds,
                    anchor_at,
                } => TimeScheduleSummary::Every {
                    every: format_duration(*every_seconds),
                    anchor_at: format_timestamp(Some(*anchor_at))
                        .unwrap_or_else(|| anchor_at.to_string()),
                },
            },
        },
        StoredScheduleTrigger::Poll { poll } => ScheduleTriggerSummary::Poll {
            every: format_duration(poll.every_seconds),
            until: poll.until.clone(),
            timeout_at: format_timestamp(poll.timeout_at),
            max_runs: poll.max_runs,
        },
    }
}

fn schedule_active_run_summary(active_run: &ActiveScheduleRun) -> ScheduleActiveRunSummary {
    ScheduleActiveRunSummary {
        run_id: active_run.id.clone(),
        started_at: format_timestamp(Some(active_run.started_at)),
        mode: active_run.mode,
        thread_id: active_run.thread_id,
        report: active_run
            .report
            .as_ref()
            .map(|report| SchedulePendingReportSummary {
                status: report.status,
                summary: report.summary.clone(),
                reported_at: format_timestamp(Some(report.reported_at)),
            }),
    }
}

fn schedule_execution_record_summary(
    record: &ScheduleExecutionRecord,
) -> ScheduleExecutionRecordSummary {
    ScheduleExecutionRecordSummary {
        run_id: record.run_id.clone(),
        started_at: format_timestamp(record.started_at),
        finished_at: format_timestamp(record.finished_at),
        disposition: record.disposition,
        summary: record.summary.clone(),
        thread_id: record.thread_id,
    }
}

fn format_timestamp(timestamp: Option<i64>) -> Option<String> {
    let timestamp = timestamp?;
    OffsetDateTime::from_unix_timestamp(timestamp)
        .ok()
        .and_then(|timestamp| timestamp.format(&Rfc3339).ok())
}

fn format_duration(seconds: u64) -> String {
    format!("{seconds}s")
}

pub(crate) fn serialize_output<T: Serialize>(value: &T) -> Result<String, FunctionCallError> {
    serde_json::to_string_pretty(value).map_err(|err| {
        FunctionCallError::Fatal(format!("failed to serialize scheduler output: {err}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codex::make_session_and_context;
    use pretty_assertions::assert_eq;

    #[test]
    fn parse_duration_supports_compound_values() {
        assert_eq!(parse_duration_seconds("1h 30m").expect("duration"), 5400);
        assert_eq!(parse_duration_seconds("45s").expect("duration"), 45);
        assert_eq!(parse_duration_seconds("2d4h").expect("duration"), 187200);
    }

    #[tokio::test]
    async fn create_list_pause_resume_and_delete_schedule() {
        let (session, turn) = make_session_and_context().await;
        let service =
            SchedulerService::new(true, session.codex_home().await, session.conversation_id);

        let created = service
            .create_job(
                &turn,
                ScheduleCreateArgs {
                    trigger: ScheduleTriggerType::Time,
                    prompt: "check repo health".to_string(),
                    schedule: Some(ScheduleTimeArgs {
                        at: None,
                        after: Some("15m".to_string()),
                        every: None,
                    }),
                    poll: None,
                    concurrency: Some(ScheduleConcurrency::Queue),
                    misfire_policy: Some(ScheduleMisfirePolicy::RunOnce),
                    durable: Some(true),
                },
            )
            .await
            .expect("create schedule");
        assert_eq!(created.status, StoredScheduleStatus::Active);

        let listed = service
            .list_jobs(session.conversation_id)
            .await
            .expect("list schedules");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id, created.id);

        let paused = service
            .pause_job(session.conversation_id, created.id.as_str())
            .await
            .expect("pause schedule");
        assert_eq!(paused.status, StoredScheduleStatus::Paused);

        let resumed = service
            .resume_job(session.conversation_id, created.id.as_str())
            .await
            .expect("resume schedule");
        assert_eq!(resumed.status, StoredScheduleStatus::Active);

        let deleted = service
            .delete_job(session.conversation_id, created.id.as_str())
            .await
            .expect("delete schedule");
        assert_eq!(deleted.id, created.id);

        let listed = service
            .list_jobs(session.conversation_id)
            .await
            .expect("list schedules after delete");
        assert!(listed.is_empty());
    }

    #[tokio::test]
    async fn poll_reports_are_recorded_against_active_runs() {
        let (session, turn) = make_session_and_context().await;
        let service =
            SchedulerService::new(true, session.codex_home().await, session.conversation_id);
        let created = service
            .create_job(
                &turn,
                ScheduleCreateArgs {
                    trigger: ScheduleTriggerType::Poll,
                    prompt: "check CI".to_string(),
                    schedule: None,
                    poll: Some(SchedulePollArgs {
                        every: "10m".to_string(),
                        until: "all checks are green".to_string(),
                        timeout: Some("1h".to_string()),
                        max_runs: Some(5),
                    }),
                    concurrency: Some(ScheduleConcurrency::Queue),
                    misfire_policy: Some(ScheduleMisfirePolicy::RunOnce),
                    durable: Some(true),
                },
            )
            .await
            .expect("create poll schedule");
        let claimed = service
            .claim_run(
                session.conversation_id,
                created.id.as_str(),
                OffsetDateTime::now_utc()
                    .unix_timestamp()
                    .saturating_add(10 * 60),
                ScheduleExecutionMode::Queue,
                Some("schedule-turn".to_string()),
            )
            .await
            .expect("claim schedule")
            .expect("claimed run");

        let ack = service
            .record_report(ScheduleReportArgs {
                thread_id: session.conversation_id.to_string(),
                id: created.id.clone(),
                run_id: claimed.run_id.clone(),
                status: ScheduleReportStatus::Completed,
                summary: Some("CI is green".to_string()),
            })
            .await
            .expect("record report");
        assert_eq!(ack.status, ScheduleReportStatus::Completed);

        let listed = service
            .list_jobs(session.conversation_id)
            .await
            .expect("list schedules");
        assert_eq!(
            listed[0]
                .active_run
                .as_ref()
                .and_then(|active_run| active_run.report.as_ref())
                .map(|report| report.status),
            Some(ScheduleReportStatus::Completed)
        );
    }
}
