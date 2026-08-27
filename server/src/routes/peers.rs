use crate::state::AppState;
use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};

pub async fn get_peers(
    State(state): State<AppState>, Path(info_hash): Path<String>,
) -> impl IntoResponse {
    let info_hash = info_hash.to_lowercase();

    if let Some(engine) = state.stream_engine().get_engine(&info_hash).await {
        let peers = engine.get_peer_stats().await;
        (StatusCode::OK, Json(peers)).into_response()
    } else {
        (StatusCode::NOT_FOUND, "Engine not found").into_response()
    }
}
