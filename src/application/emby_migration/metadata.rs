//! Read-only, bounded Emby metadata transport. No direct provider/image requests.
use super::*;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const MAX_PROFILE_BYTES: usize = 10 * 1024 * 1024;
static READS: OnceLock<Arc<Semaphore>> = OnceLock::new();
static IMAGES: OnceLock<Arc<Semaphore>> = OnceLock::new();
type PeopleCache = (Option<EmbyClientCacheKey>, BTreeMap<String, Value>);
static PEOPLE: OnceLock<Mutex<PeopleCache>> = OnceLock::new();

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PageRequest {
    source: EmbySource,
    #[serde(default)]
    discovery_only: bool,
    #[serde(default)]
    start_index: u32,
    #[serde(default = "default_page_size")]
    limit: u32,
    #[serde(default)]
    source_library_ids: Vec<String>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ImageRequest {
    source: EmbySource,
    id: String,
    #[serde(default)]
    image_type: Option<String>,
}
async fn permit(image: bool) -> Result<OwnedSemaphorePermit, PluginRpcError> {
    let pool = if image {
        IMAGES.get_or_init(|| Arc::new(Semaphore::new(4)))
    } else {
        READS.get_or_init(|| Arc::new(Semaphore::new(MAX_USER_READ_CONCURRENCY)))
    };
    Arc::clone(pool)
        .acquire_owned()
        .await
        .map_err(|_| invalid_response())
}
fn text(raw: &Value, key: &str) -> Option<String> {
    raw.get(key)
        .and_then(Value::as_str)
        .filter(|v| !v.trim().is_empty() && v.len() <= MAX_TEXT_LENGTH)
        .map(str::to_owned)
}
fn ids(raw: &Value) -> Value {
    let mut result = BTreeMap::new();
    if let Some(values) = raw.get("ProviderIds").and_then(Value::as_object) {
        for (key, value) in values.iter().take(32) {
            if let Some(value) = value.as_str().filter(|v| v.len() <= MAX_ID_LENGTH) {
                result.insert(key.clone(), value.to_owned());
            }
        }
    }
    json!(result)
}
fn identity(raw: &Value) -> Value {
    json!({"id":text(raw,"Id"),"name":text(raw,"Name"),"itemType":text(raw,"Type"),
        "productionYear":raw.get("ProductionYear"),"providerIds":ids(raw),"parentId":text(raw,"ParentId"),
        "seriesId":text(raw,"SeriesId"),"seasonId":text(raw,"SeasonId"),"indexNumber":raw.get("IndexNumber"),
        "parentIndexNumber":raw.get("ParentIndexNumber"),"userData":null})
}
async fn person(
    client: EmbyClient,
    key: EmbyClientCacheKey,
    raw: Value,
) -> Result<Value, PluginRpcError> {
    let id = text(&raw, "Id").ok_or_else(invalid_response)?;
    validate_identifier(&id).map_err(|_| invalid_response())?;
    let cache = PEOPLE.get_or_init(|| Mutex::new((None, BTreeMap::new())));
    let mut cached = cache.lock().await;
    if cached.0.as_ref() != Some(&key) {
        *cached = (Some(key.clone()), BTreeMap::new());
    }
    let found = cached.1.get(&id).cloned();
    drop(cached);
    let detail = if let Some(value) = found {
        value
    } else {
        let _permit = permit(false).await?;
        let value: Value = client
            .get_json(
                &format!("Items/{id}"),
                &[("Fields", "ProviderIds,ImageTags".to_owned())],
            )
            .await
            .map_err(to_rpc_error)?;
        let mut cached = cache.lock().await;
        if cached.0.as_ref() == Some(&key) {
            if cached.1.len() >= 8192 {
                if let Some(first) = cached.1.keys().next().cloned() {
                    cached.1.remove(&first);
                }
            }
            cached.1.insert(id.clone(), value.clone());
        }
        value
    };
    Ok(
        json!({"id":id,"name":text(&raw,"Name"),"role":text(&raw,"Role"),
        "personType":text(&raw,"Type").unwrap_or_else(||"Actor".to_owned()),
        "sortOrder":raw.get("SortOrder"),"providerIds":ids(&detail),
        "hasPrimary":raw.get("PrimaryImageTag").and_then(Value::as_str).is_some()
            || detail.pointer("/ImageTags/Primary").and_then(Value::as_str).is_some()}),
    )
}
fn stream(raw: &Value) -> Value {
    json!({"index":raw.get("Index").and_then(Value::as_i64).unwrap_or(0),
        "streamType":text(raw,"Type").unwrap_or_default().to_uppercase(),"codec":text(raw,"Codec"),
        "language":text(raw,"Language"),"title":text(raw,"Title"),"isDefault":raw.get("IsDefault").and_then(Value::as_bool).unwrap_or(false),
        "isForced":raw.get("IsForced").and_then(Value::as_bool).unwrap_or(false),
        "details":{"source":"emby","width":raw.get("Width"),"height":raw.get("Height"),
            "channels":raw.get("Channels"),"channelLayout":raw.get("ChannelLayout"),"bitRate":raw.get("BitRate"),
            "videoRange":raw.get("VideoRange"),"videoRangeType":raw.get("VideoRangeType"),
            "colorTransfer":raw.get("ColorTransfer"),"colorPrimaries":raw.get("ColorPrimaries"),"colorSpace":raw.get("ColorSpace"),
            "profile":raw.get("Profile"),"level":raw.get("Level")}})
}
fn source(raw: &Value) -> Value {
    json!({"id":text(raw,"Id"),"path":text(raw,"Path").filter(|p|p.starts_with('/')),
        "container":text(raw,"Container"),"runtimeTicks":raw.get("RunTimeTicks"),"bitrate":raw.get("Bitrate"),
        "streams":raw.get("MediaStreams").and_then(Value::as_array).map(|s|s.iter().take(256).map(stream).collect::<Vec<_>>()).unwrap_or_default()})
}
fn page(items: Vec<Value>, start: u32, total: Option<u32>, raw_count: usize) -> Value {
    let next = start.saturating_add(raw_count as u32);
    json!({"items":items,"startIndex":start,"totalRecordCount":total,
        "nextStartIndex":if raw_count>0 && total.is_none_or(|t|next<t) {Some(next)}else{None}})
}
async fn read_page(
    client: &EmbyClient,
    request: &PageRequest,
    kind: &str,
    fields: &str,
) -> Result<Value, PluginRpcError> {
    let _permit = permit(false).await?;
    let mut query = vec![
        ("IncludeItemTypes", kind.to_owned()),
        ("Recursive", "true".to_owned()),
        ("StartIndex", request.start_index.to_string()),
        ("Limit", request.limit.clamp(1, MAX_PAGE_SIZE).to_string()),
        ("Fields", fields.to_owned()),
        ("EnableUserData", "false".to_owned()),
    ];
    if request.source_library_ids.len() > 1 {
        return Err(invalid_request());
    }
    for id in &request.source_library_ids {
        validate_identifier(id).map_err(|_| invalid_request())?;
    }
    if !request.source_library_ids.is_empty() {
        query.push(("ParentId", request.source_library_ids[0].clone()));
    }
    client.get_json("Items", &query).await.map_err(to_rpc_error)
}
fn raw_items(raw: &Value, limit: u32) -> Result<&[Value], PluginRpcError> {
    let items = raw
        .get("Items")
        .and_then(Value::as_array)
        .ok_or_else(invalid_response)?;
    if items.len() > limit.clamp(1, MAX_PAGE_SIZE) as usize {
        return Err(invalid_response());
    }
    Ok(items)
}
pub async fn item_metadata(params: Value) -> Result<Value, PluginRpcError> {
    let request: PageRequest = serde_json::from_value(params).map_err(|_| invalid_request())?;
    let key = EmbyClientCacheKey::from(&request.source);
    let client = cached_emby_client(request.source.clone())
        .await
        .map_err(to_rpc_error)?;
    if request.discovery_only {
        let _permit = permit(false).await?;
        let raw: Vec<RawLibraryFolder> = client
            .get_json("Library/VirtualFolders", &[])
            .await
            .map_err(to_rpc_error)?;
        if raw.len() > MAX_LIBRARY_FOLDER_COUNT {
            return Err(invalid_response());
        }
        let folders = raw
            .into_iter()
            .filter_map(map_library_folder)
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(
            json!({"items":[],"startIndex":0,"totalRecordCount":0,"nextStartIndex":null,"libraryFolders":folders}),
        );
    }
    let raw=read_page(&client,&request,"Movie,Series,Season,Episode",
        "Path,ProviderIds,People,RunTimeTicks,MediaSources,MediaStreams,Container,Width,Height,ProductionYear,ParentId,SeriesId,SeasonId,IndexNumber,ParentIndexNumber").await?;
    let items = raw_items(&raw, request.limit)?;
    let mut output = Vec::with_capacity(items.len());
    for item in items {
        let raw_people = item
            .get("People")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        if raw_people.len() > 1000 {
            return Err(invalid_response());
        }
        let mut jobs = JoinSet::new();
        let mut people = Vec::new();
        for (order, value) in raw_people.into_iter().enumerate() {
            let client = client.clone();
            let key = key.clone();
            jobs.spawn(async move { person(client, key, value).await.map(|p| (order, p)) });
            if jobs.len() >= 8 {
                let (order, p) = jobs
                    .join_next()
                    .await
                    .ok_or_else(invalid_response)?
                    .map_err(|_| invalid_response())??;
                people.push((order, p));
            }
        }
        while let Some(value) = jobs.join_next().await {
            people.push(value.map_err(|_| invalid_response())??);
        }
        people.sort_by_key(|(order, _)| *order);
        let mut sources = item
            .get("MediaSources")
            .and_then(Value::as_array)
            .map(|s| s.iter().take(64).map(source).collect::<Vec<_>>())
            .unwrap_or_default();
        if sources.is_empty() {
            sources.push(source(item));
        }
        output.push(json!({"item":identity(item),"path":text(item,"Path"),"people":people.into_iter().map(|(_,p)|p).collect::<Vec<_>>(),
            "runtimeTicks":item.get("RunTimeTicks"),"sources":sources}));
    }
    Ok(page(
        output,
        request.start_index,
        raw.get("TotalRecordCount")
            .and_then(Value::as_u64)
            .and_then(|n| n.try_into().ok()),
        items.len(),
    ))
}
pub async fn collections(params: Value) -> Result<Value, PluginRpcError> {
    let request: PageRequest = serde_json::from_value(params).map_err(|_| invalid_request())?;
    let client = cached_emby_client(request.source.clone())
        .await
        .map_err(to_rpc_error)?;
    // BoxSets can live outside selected virtual folders. Filter members in the host whitelist.
    let raw = read_page(
        &client,
        &PageRequest {
            source: request.source.clone(),
            source_library_ids: Vec::new(),
            discovery_only: false,
            start_index: request.start_index,
            limit: request.limit,
        },
        "BoxSet",
        "ProviderIds,Overview,ImageTags",
    )
    .await?;
    let items = raw_items(&raw, request.limit)?;
    let mut output = Vec::new();
    for item in items {
        let id = text(item, "Id").ok_or_else(invalid_response)?;
        validate_identifier(&id).map_err(|_| invalid_response())?;
        let mut members = Vec::new();
        let mut start = 0u32;
        loop {
            let _permit = permit(false).await?;
            let children: Value = client
                .get_json(
                    "Items",
                    &[
                        ("ParentId", id.clone()),
                        ("Recursive", "false".to_owned()),
                        ("Fields", "ProviderIds".to_owned()),
                        ("EnableUserData", "false".to_owned()),
                        ("StartIndex", start.to_string()),
                        ("Limit", "500".to_owned()),
                    ],
                )
                .await
                .map_err(to_rpc_error)?;
            let rows = raw_items(&children, 500)?;
            for row in rows {
                if let Some(id) = text(row, "Id") {
                    members.push(id);
                }
            }
            if members.len() > 100_000 {
                return Err(invalid_response());
            }
            let next = start.saturating_add(rows.len() as u32);
            if rows.is_empty()
                || children
                    .get("TotalRecordCount")
                    .and_then(Value::as_u64)
                    .is_some_and(|n| u64::from(next) >= n)
            {
                break;
            }
            start = next;
        }
        output.push(json!({"id":id,"name":text(item,"Name"),"overview":text(item,"Overview"),"providerIds":ids(item),"memberIds":members,
            "hasPrimary":item.pointer("/ImageTags/Primary").is_some(),"hasBackdrop":item.get("BackdropImageTags").and_then(Value::as_array).is_some_and(|v|!v.is_empty())}));
    }
    Ok(page(
        output,
        request.start_index,
        raw.get("TotalRecordCount")
            .and_then(Value::as_u64)
            .and_then(|n| n.try_into().ok()),
        items.len(),
    ))
}
async fn image(params: Value, collection: bool) -> Result<Value, PluginRpcError> {
    let request: ImageRequest = serde_json::from_value(params).map_err(|_| invalid_request())?;
    validate_identifier(&request.id).map_err(|_| invalid_request())?;
    let kind = request.image_type.as_deref().unwrap_or("Primary");
    if !matches!(kind, "Primary" | "Backdrop") || (!collection && kind != "Primary") {
        return Err(invalid_request());
    }
    let _image = permit(true).await?;
    let _read = permit(false).await?;
    let client = cached_emby_client(request.source)
        .await
        .map_err(to_rpc_error)?;
    let url = client
        .base_url
        .join(&format!("Items/{}/Images/{kind}", request.id))
        .map_err(|_| invalid_request())?;
    let mut response = client
        .client
        .get(url)
        .header("X-Emby-Token", &client.api_key)
        .query(&[
            ("maxWidth", if collection { 1000 } else { 400 }),
            ("quality", 90),
        ])
        .send()
        .await
        .map_err(|_| to_rpc_error(MigrationError::Upstream))?;
    if response.status() == StatusCode::NOT_FOUND {
        return Ok(json!({"status":"NO_IMAGE","contentType":null,"bytesBase64":null}));
    }
    if !response.status().is_success() {
        return Err(to_rpc_error(MigrationError::Upstream));
    }
    if response
        .content_length()
        .is_some_and(|n| n > MAX_PROFILE_BYTES as u64)
    {
        return Ok(json!({"status":"TOO_LARGE","contentType":null,"bytesBase64":null}));
    }
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .split(';')
        .next()
        .unwrap_or_default()
        .to_owned();
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| to_rpc_error(MigrationError::Upstream))?
    {
        if bytes.len().saturating_add(chunk.len()) > MAX_PROFILE_BYTES {
            return Ok(json!({"status":"TOO_LARGE","contentType":null,"bytesBase64":null}));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(json!({"status":"IMAGE","contentType":content_type,"bytesBase64":STANDARD.encode(bytes)}))
}
pub async fn person_image(params: Value) -> Result<Value, PluginRpcError> {
    image(params, false).await
}
pub async fn collection_image(params: Value) -> Result<Value, PluginRpcError> {
    image(params, true).await
}
