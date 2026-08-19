use super::*;

/// Parameters for the `start_job` JSON-RPC method.
#[derive(Debug, Deserialize)]
pub(in crate::rpc) struct StartJobParams {
    /// Discriminator selecting which worker kind to dispatch.
    pub(in crate::rpc) kind: crate::jobs::JobKind,
    /// Free-form per-kind parameters forwarded to the worker.
    #[serde(default)]
    pub(in crate::rpc) params: Value,
}

/// Parameters for `get_job` and `cancel_job` (a bare job id).
#[derive(Debug, Deserialize)]
pub(in crate::rpc) struct JobIdParams {
    /// The id of the job to inspect or cancel.
    pub(in crate::rpc) job_id: uuid::Uuid,
}

/// Parameters for the `list_jobs` JSON-RPC method.
#[derive(Debug, Deserialize, Default)]
pub(in crate::rpc) struct ListJobsParams {
    /// If true, finished (terminal) jobs are also returned.
    #[serde(default)]
    pub(in crate::rpc) include_finished: bool,
    /// Optional cap on the number of jobs returned.
    #[serde(default)]
    pub(in crate::rpc) limit: Option<usize>,
}

pub(in crate::rpc) async fn dispatch_jobs(
    method: RpcMethod,
    id: Option<Value>,
    request: &JsonRpcRequest,
    state: &Arc<DaemonState>,
    _transport: &DaemonTransport<'_>,
) -> JsonRpcResponse {
    match method {
        RpcMethod::StartJob => {
            match serde_json::from_value::<StartJobParams>(request.params.clone()) {
                Ok(p) => {
                    let runner = crate::jobs::AppRunner {
                        state: state.clone(),
                    };
                    let job_id =
                        crate::jobs::spawn(state.jobs.clone(), p.kind, p.params, runner).await;
                    JsonRpcResponse::ok(id, json!({ "job_id": job_id }))
                }
                Err(err) => invalid_params(id, err),
            }
        }
        RpcMethod::GetJob => match serde_json::from_value::<JobIdParams>(request.params.clone()) {
            Ok(p) => match state.jobs.get(p.job_id).await {
                Some(job) => match serde_json::to_value(job) {
                    Ok(value) => JsonRpcResponse::ok(id, value),
                    Err(err) => internal_error(id, err),
                },
                None => JsonRpcResponse::ok(id, Value::Null),
            },
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::ListJobs => {
            let p: ListJobsParams =
                serde_json::from_value(request.params.clone()).unwrap_or_default();
            let jobs = state
                .jobs
                .list(crate::jobs::ListJobsFilter {
                    include_finished: p.include_finished,
                    limit: p.limit,
                })
                .await;
            match serde_json::to_value(jobs) {
                Ok(value) => JsonRpcResponse::ok(id, value),
                Err(err) => internal_error(id, err),
            }
        }
        RpcMethod::CancelJob => match serde_json::from_value::<JobIdParams>(request.params.clone())
        {
            Ok(p) => {
                let cancelled = state.jobs.cancel(p.job_id).await;
                JsonRpcResponse::ok(id, json!({ "cancelled": cancelled }))
            }
            Err(err) => invalid_params(id, err),
        },
        _ => unreachable!("method routed to the wrong RPC feature handler"),
    }
}
