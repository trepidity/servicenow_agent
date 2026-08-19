use super::handlers::*;
use super::*;

pub(crate) async fn dispatch(request: JsonRpcRequest, state: &Arc<DaemonState>) -> JsonRpcResponse {
    let id = request.id.clone();
    let transport = DaemonTransport::new(state.core.as_ref());
    let method = RpcMethod::from_method(&request.method);
    match method {
        RpcMethod::ContractInfo
        | RpcMethod::Ping
        | RpcMethod::Shutdown
        | RpcMethod::CatalogItemsSearch
        | RpcMethod::CatalogItemGet
        | RpcMethod::RefreshAll
        | RpcMethod::SchedulerStatus
        | RpcMethod::SchedulerTriggerNow
        | RpcMethod::PlanGet
        | RpcMethod::CatalogPlanRequest
        | RpcMethod::CatalogSubmitRequest
        | RpcMethod::WorkNotePlanAdd
        | RpcMethod::WorkNoteApplyAdd
        | RpcMethod::ChangeRequestPlanCreate
        | RpcMethod::ChangeRequestPlanUpdate
        | RpcMethod::ChangeTaskPlanCreate
        | RpcMethod::ChangeTaskPlanUpdate
        | RpcMethod::IncidentPlanUpdate
        | RpcMethod::ChangeRequestApplyCreate
        | RpcMethod::ChangeRequestApplyUpdate
        | RpcMethod::ChangeTaskApplyCreate
        | RpcMethod::ChangeTaskApplyUpdate
        | RpcMethod::IncidentApplyUpdate
        | RpcMethod::ResourcePlanPlanCreate
        | RpcMethod::ResourcePlanPlanUpdate
        | RpcMethod::ResourcePlanApplyCreate
        | RpcMethod::ResourcePlanApplyUpdate
        | RpcMethod::StoryPlanCreate
        | RpcMethod::StoryPlanUpdate
        | RpcMethod::StoryTaskPlanCreate
        | RpcMethod::StoryTaskPlanUpdate
        | RpcMethod::StoryApplyCreate
        | RpcMethod::StoryApplyUpdate
        | RpcMethod::StoryTaskApplyCreate
        | RpcMethod::StoryTaskApplyUpdate
        | RpcMethod::TimecardList
        | RpcMethod::TimecardSetHours
        | RpcMethod::TimecardPlanSetHours
        | RpcMethod::TimecardApplySetHours
        | RpcMethod::Unknown => dispatch_system(method, id, &request, state, &transport).await,
        RpcMethod::VaultPath
        | RpcMethod::GetDegradedReads
        | RpcMethod::CacheInfo
        | RpcMethod::RepairVault
        | RpcMethod::VerifyVault
        | RpcMethod::PruneOrphans => {
            dispatch_cache_vault(method, id, &request, state, &transport).await
        }
        RpcMethod::GetRecord
        | RpcMethod::GetRecordFresh
        | RpcMethod::TaskSlaStatus
        | RpcMethod::TaskSlaStatusForTasks
        | RpcMethod::SearchRecords
        | RpcMethod::UserLookup
        | RpcMethod::UserSearch
        | RpcMethod::ResourcePlanList
        | RpcMethod::GetChildren
        | RpcMethod::GetWorkNotes
        | RpcMethod::ListRecords
        | RpcMethod::RecordQuery
        | RpcMethod::MyTasks
        | RpcMethod::MyTasksFresh
        | RpcMethod::ListMyTasks
        | RpcMethod::MyApprovals
        | RpcMethod::MyApprovalsFresh
        | RpcMethod::ListMyApprovals
        | RpcMethod::MyProjects
        | RpcMethod::MyProjectsFresh
        | RpcMethod::ListMyProjects
        | RpcMethod::MyStoriesFresh
        | RpcMethod::ListMyStories
        | RpcMethod::MyIncidentsFresh
        | RpcMethod::ListMyIncidents
        | RpcMethod::AddWorkNote
        | RpcMethod::AttachmentList
        | RpcMethod::AttachmentUpload
        | RpcMethod::SetState
        | RpcMethod::FieldChoices => {
            dispatch_records(method, id, &request, state, &transport).await
        }
        RpcMethod::GetKnowledgeArticle
        | RpcMethod::GetArticle
        | RpcMethod::GetKnowledgeArticleFresh
        | RpcMethod::GetArticleFresh
        | RpcMethod::SearchKnowledge
        | RpcMethod::KbSemanticSearch
        | RpcMethod::ListKnowledgeBases
        | RpcMethod::ListCategories
        | RpcMethod::ListKnowledgeArticles
        | RpcMethod::KbSync
        | RpcMethod::KbListTags
        | RpcMethod::KbStatus
        | RpcMethod::KbSemanticStatus
        | RpcMethod::KbSemanticRebuild => {
            dispatch_knowledge(method, id, &request, state, &transport).await
        }
        RpcMethod::BusinessApplicationGet
        | RpcMethod::BusinessApplicationGetFresh
        | RpcMethod::BusinessApplicationSearch
        | RpcMethod::BusinessApplicationQuery
        | RpcMethod::BusinessApplicationServers
        | RpcMethod::BusinessApplicationServersCached
        | RpcMethod::BusinessApplicationsForServer
        | RpcMethod::BusinessApplicationSync
        | RpcMethod::BusinessApplicationFields => {
            dispatch_business_applications(method, id, &request, state, &transport).await
        }
        RpcMethod::IncidentListByAssignmentGroup
        | RpcMethod::IncidentAssignmentGroups
        | RpcMethod::IncidentAssignmentGroupQueue => {
            dispatch_incidents(method, id, &request, state, &transport).await
        }
        RpcMethod::ServerGet
        | RpcMethod::ServerGetFresh
        | RpcMethod::ServerSearch
        | RpcMethod::ServerQuery
        | RpcMethod::ServerFields => {
            dispatch_servers(method, id, &request, state, &transport).await
        }
        RpcMethod::GetApproval
        | RpcMethod::Approve
        | RpcMethod::ApprovalApprove
        | RpcMethod::Reject
        | RpcMethod::ApprovalReject => {
            dispatch_approvals(method, id, &request, state, &transport).await
        }
        RpcMethod::StartJob | RpcMethod::GetJob | RpcMethod::ListJobs | RpcMethod::CancelJob => {
            dispatch_jobs(method, id, &request, state, &transport).await
        }
    }
}
