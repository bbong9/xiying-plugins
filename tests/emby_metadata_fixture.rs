use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use xiyingd::application::emby_migration::{collections, item_metadata, person_image};

async fn fixture<F>(handler: F) -> (Value, tokio::task::JoinHandle<()>)
where
    F: Fn(&str) -> (u16, String, String) + Send + Sync + 'static,
{
    let (source, server, _) = observed_fixture(handler).await;
    (source, server)
}

type Requests = Arc<Mutex<Vec<String>>>;

async fn observed_fixture<F>(handler: F) -> (Value, tokio::task::JoinHandle<()>, Requests)
where
    F: Fn(&str) -> (u16, String, String) + Send + Sync + 'static,
{
    let requests: Requests = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&requests);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            loop {
                let mut buffer = [0; 2048];
                let n = stream.read(&mut buffer).await.unwrap();
                if n == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..n]);
                if request.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            let request = String::from_utf8(request).unwrap();
            assert!(request.starts_with("GET "), "metadata is GET-only");
            observed
                .lock()
                .unwrap()
                .push(request.lines().next().unwrap().to_owned());
            let url = request_url(&request);
            let unsupported_item = url
                .path()
                .strip_prefix("/Items/")
                .is_some_and(|id| !id.is_empty() && id.bytes().all(|byte| byte.is_ascii_digit()));
            // Emby 4.11 does not expose GET /Items/{Id}; image subroutes remain valid.
            let (status, content_type, body) = if unsupported_item {
                (404, "application/json".into(), "{}".into())
            } else {
                handler(&request)
            };
            let response = format!(
                "HTTP/1.1 {status} OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes()).await;
        }
    });
    (
        json!({"baseUrl":format!("http://{addr}/"),"apiKey":"fixture-only-not-a-real-key","allowPrivateNetwork":true}),
        task,
        requests,
    )
}

fn request_url(request: &str) -> reqwest::Url {
    reqwest::Url::parse(&format!(
        "http://fixture{}",
        request.split_whitespace().nth(1).unwrap()
    ))
    .unwrap()
}

fn batch_ids(request: &str) -> Option<Vec<String>> {
    request_url(request)
        .query_pairs()
        .find(|(name, _)| name == "Ids")
        .map(|(_, ids)| ids.split(',').map(str::to_owned).collect())
}
#[tokio::test]
async fn card21_metadata_paginates_and_projects_people_streams_without_urls() {
    let (source,server)=fixture(|request|{
        let body=if batch_ids(request).is_some() {json!({"Items":[{"Id":"101","ProviderIds":{"Tmdb":"12"},"ImageTags":{"Primary":"tag"}}]})} else {
            assert!(request.contains("IncludeItemTypes=Movie%2CSeries%2CSeason%2CEpisode"));assert!(request.contains("ParentId=lib1"));assert!(request.contains("StartIndex=5"));
            json!({"TotalRecordCount":7,"Items":[{"Id":"m1","Name":"Film","Type":"Movie","Path":"/media/Films/a.strm","RunTimeTicks":72000000000i64,
                "People":[{"Id":"101","Name":"Actor","Type":"Actor","Role":"Lead"}],"MediaSources":[{"Id":"s1","Container":"mkv","MediaStreams":[{"Index":0,"Type":"Video","Codec":"hevc","Width":3840,"Height":2160}]}]}]})
        };(200,"application/json".into(),body.to_string())
    }).await;
    let result = item_metadata(
        json!({"source":source,"startIndex":5,"limit":1,"sourceLibraryIds":["lib1"]}),
    )
    .await
    .unwrap();
    assert_eq!(result["nextStartIndex"], 6);
    assert_eq!(result["items"][0]["people"][0]["providerIds"]["Tmdb"], "12");
    assert_eq!(
        result["items"][0]["sources"][0]["streams"][0]["details"]["width"],
        3840
    );
    assert!(result.to_string().find("image.tmdb.org").is_none());
    server.abort();
}
#[tokio::test]
async fn card21_metadata_empty_page_stops() {
    let (source, server) = fixture(|_| {
        (
            200,
            "application/json".into(),
            json!({"Items":[],"TotalRecordCount":0}).to_string(),
        )
    })
    .await;
    let result = item_metadata(json!({"source":source,"limit":500}))
        .await
        .unwrap();
    assert_eq!(result["items"], json!([]));
    assert_eq!(result["nextStartIndex"], Value::Null);
    server.abort();
}
#[tokio::test]
async fn card21_image_404_is_explicit_no_image() {
    let (source, server) = fixture(|r| {
        assert!(r.starts_with("GET /Items/p1/Images/Primary?"));
        assert!(r.contains("maxWidth=400&quality=90"));
        (404, "image/jpeg".into(), String::new())
    })
    .await;
    assert_eq!(
        person_image(json!({"source":source,"id":"p1"}))
            .await
            .unwrap()["status"],
        "NO_IMAGE"
    );
    server.abort();
}
#[tokio::test]
async fn card21_image_over_limit_is_not_returned() {
    let (source, server) =
        fixture(|_| (200, "image/jpeg".into(), "x".repeat(10 * 1024 * 1024 + 1))).await;
    let result = person_image(json!({"source":source,"id":"p1"}))
        .await
        .unwrap();
    assert_eq!(result["status"], "TOO_LARGE");
    assert_eq!(result["bytesBase64"], Value::Null);
    server.abort();
}
#[tokio::test]
async fn card21_collections_read_members_with_pagination() {
    let (source,server)=fixture(|r|{
        let body=if r.contains("IncludeItemTypes=BoxSet") {json!({"Items":[{"Id":"c1","Name":"Collection","ProviderIds":{"Tmdb":"99"}}],"TotalRecordCount":1})}
            else if r.contains("StartIndex=0") {json!({"Items":[{"Id":"m1"}],"TotalRecordCount":2})}
            else {assert!(r.contains("StartIndex=1"));json!({"Items":[{"Id":"m2"}],"TotalRecordCount":2})};
        (200,"application/json".into(),body.to_string())
    }).await;
    let result = collections(json!({"source":source,"limit":1}))
        .await
        .unwrap();
    assert_eq!(result["items"][0]["memberIds"], json!(["m1", "m2"]));
    server.abort();
}
#[tokio::test]
async fn card21_cancel_aborts_pending_image_read() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = [0; 2048];
        let n = socket.read(&mut buf).await.unwrap();
        assert!(String::from_utf8_lossy(&buf[..n]).starts_with("GET "));
        ready_tx.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), socket.read(&mut buf))
            .await
            .unwrap()
            .unwrap()
    });
    let request = tokio::spawn(person_image(
        json!({"source":{"baseUrl":format!("http://{addr}/"),"apiKey":"fixture-only","allowPrivateNetwork":true},"id":"p1"}),
    ));
    ready_rx.await.unwrap();
    request.abort();
    let _ = request.await;
    assert_eq!(server.await.unwrap(), 0);
}

#[tokio::test]
async fn card21_jsonl_process_really_has_four_concurrent_image_gets() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::io::{AsyncBufReadExt, BufReader};
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let active = Arc::new(AtomicUsize::new(0));
    let seen = active.clone();
    let server = tokio::spawn(async move {
        let mut jobs = tokio::task::JoinSet::new();
        for _ in 0..4 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let seen = seen.clone();
            jobs.spawn(async move {
                let mut buf = [0; 8192];
                let n = stream.read(&mut buf).await.unwrap();
                assert!(std::str::from_utf8(&buf[..n]).unwrap().starts_with("GET "));
                seen.fetch_add(1, Ordering::SeqCst);
                while seen.load(Ordering::SeqCst) < 4 {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                stream
                    .write_all(
                        b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .await
                    .unwrap();
            });
        }
        while let Some(r) = jobs.join_next().await {
            r.unwrap();
        }
    });
    let mut child =
        tokio::process::Command::new(env!("CARGO_BIN_EXE_xiying-plugin-emby-migration"))
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
    let mut input = child.stdin.take().unwrap();
    for i in 0..4 {
        let request = json!({"id":format!("request-{i}"),"method":"migration.person_image","params":{"source":{"baseUrl":format!("http://{addr}"),"apiKey":"fixture-only","allowPrivateNetwork":true},"id":format!("person-{i}")}});
        input
            .write_all(format!("{request}\n").as_bytes())
            .await
            .unwrap();
    }
    input.flush().await.unwrap();
    drop(input);
    let mut output = BufReader::new(child.stdout.take().unwrap()).lines();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        for _ in 0..4 {
            let response: Value =
                serde_json::from_str(&output.next_line().await.unwrap().unwrap()).unwrap();
            assert_eq!(response["result"]["status"], "NO_IMAGE");
        }
    })
    .await
    .unwrap();
    assert_eq!(active.load(Ordering::SeqCst), 4);
    assert!(child.wait().await.unwrap().success());
    server.await.unwrap();
}

#[tokio::test]
async fn card22_virtual_folders_preserve_collection_type_get_only() {
    let (source, server) = fixture(|request| {
        let body = if request.starts_with("GET /Library/VirtualFolders") {
            json!([
                {"ItemId":"movies","Name":"Movies","Locations":["/media/Films"],"CollectionType":"movies"},
                {"ItemId":"series","Name":"Series","Locations":["/media/TV"],"CollectionType":"tvshows"},
                {"ItemId":"boxes","Name":"Collections","Locations":[],"CollectionType":"boxsets"},
                {"ItemId":"unknown","Name":"Other","Locations":[],"CollectionType":"FutureType"},
                {"ItemId":"empty","Name":"Mixed","Locations":[]}
            ])
        } else { json!([]) };
        (200, "application/json".into(), body.to_string())
    }).await;
    let page = xiyingd::application::emby_migration::list_users(json!({"source":source.clone()}))
        .await
        .unwrap();
    assert_eq!(page["libraryFolders"][0]["collectionType"], "movies");
    assert_eq!(page["libraryFolders"][1]["collectionType"], "tvshows");
    assert_eq!(page["libraryFolders"][2]["collectionType"], "boxsets");
    assert_eq!(page["libraryFolders"][3]["collectionType"], "FutureType");
    assert!(page["libraryFolders"][4]["collectionType"].is_null());
    let discovery = item_metadata(json!({"source":source,"discoveryOnly":true}))
        .await
        .unwrap();
    assert_eq!(discovery["libraryFolders"], page["libraryFolders"]);
    server.abort();
}

fn people_page() -> Value {
    json!({"TotalRecordCount":3,"Items":[
        {"Id":"1","People":[{"Id":"101","Name":"A"},{"Id":"102","Name":"B"}]},
        {"Id":"2","People":[{"Id":"102","Name":"B"},{"Id":"103","Name":"C"}]},
        {"Id":"3","People":[{"Id":"104","Name":"D"},{"Id":"105","Name":"E","PrimaryImageTag":"from-item"}]}
    ]})
}

fn person_details(ids: &[String]) -> Value {
    json!({"Items":ids.iter().map(|id| json!({"Id":id,"ProviderIds":{"Tmdb":id}})).collect::<Vec<_>>()})
}

fn json_response(body: Value) -> (u16, String, String) {
    (200, "application/json".into(), body.to_string())
}

#[tokio::test]
async fn card22c_fixture_rejects_numeric_single_item_get() {
    let (source, server) = fixture(|_| json_response(json!({}))).await;
    let response = reqwest::get(format!(
        "{}Items/12505?Fields=ProviderIds,ImageTags",
        source["baseUrl"].as_str().unwrap()
    ))
    .await
    .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
    server.abort();
}

#[tokio::test]
async fn card22c_people_are_batched_once_per_page_and_cached() {
    let (source, server, requests) = observed_fixture(|request| {
        if let Some(ids) = batch_ids(request) {
            assert!(
                request_url(request)
                    .query_pairs()
                    .any(|(name, value)| name == "Fields" && value == "ProviderIds,ImageTags")
            );
            json_response(person_details(&ids))
        } else {
            json_response(people_page())
        }
    })
    .await;
    for _ in 0..2 {
        let result = item_metadata(json!({"source":source.clone(),"limit":3}))
            .await
            .unwrap();
        assert_eq!(result["items"].as_array().unwrap().len(), 3);
        assert_eq!(
            result["items"][0]["people"][0]["providerIds"]["Tmdb"],
            "101"
        );
        assert_eq!(
            result["items"][2]["people"][1]["providerIds"]["Tmdb"],
            "105"
        );
    }
    let requests = requests.lock().unwrap();
    let batches = requests
        .iter()
        .filter_map(|request| batch_ids(request))
        .collect::<Vec<_>>();
    assert_eq!(batches, vec![vec!["101", "102", "103", "104", "105"]]);
    assert!(
        requests
            .iter()
            .all(|request| request_url(request).path() == "/Items")
    );
    server.abort();
}

#[tokio::test]
async fn card22c_missing_person_keeps_page_and_item_image_hint() {
    let (source, server, requests) = observed_fixture(|request| {
        if let Some(ids) = batch_ids(request) {
            json_response(person_details(
                &ids.into_iter().filter(|id| id != "105").collect::<Vec<_>>(),
            ))
        } else {
            json_response(people_page())
        }
    })
    .await;
    let result = item_metadata(json!({"source":source,"limit":3}))
        .await
        .unwrap();
    assert_eq!(result["items"].as_array().unwrap().len(), 3);
    assert_eq!(
        result["items"][2]["people"][0]["providerIds"]["Tmdb"],
        "104"
    );
    assert_eq!(result["items"][2]["people"][0]["hasPrimary"], false);
    assert_eq!(result["items"][2]["people"][1]["providerIds"], json!({}));
    assert_eq!(result["items"][2]["people"][1]["hasPrimary"], true);
    assert_eq!(
        requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| batch_ids(request).is_some())
            .count(),
        1
    );
    server.abort();
}

#[tokio::test]
async fn card22c_batches_are_bounded_to_100_and_deduplicated() {
    let (source, server, requests) = observed_fixture(|request| {
        if let Some(ids) = batch_ids(request) {
            assert!(ids.len() <= 100);
            json_response(person_details(&ids))
        } else {
            let mut people = (1000..1205)
                .map(|id| json!({"Id":id.to_string(),"Name":"Person"}))
                .collect::<Vec<_>>();
            people.push(people[0].clone());
            json_response(json!({"Items":[{"Id":"1","People":people}],"TotalRecordCount":1}))
        }
    })
    .await;
    let result = item_metadata(json!({"source":source,"limit":1}))
        .await
        .unwrap();
    assert_eq!(result["items"][0]["people"].as_array().unwrap().len(), 206);
    assert_eq!(
        result["items"][0]["people"][204]["providerIds"]["Tmdb"],
        "1204"
    );
    let requests = requests.lock().unwrap();
    let mut sizes = requests
        .iter()
        .filter_map(|request| batch_ids(request).map(|ids| ids.len()))
        .collect::<Vec<_>>();
    sizes.sort_unstable();
    assert_eq!(sizes, vec![5, 100, 100]);
    server.abort();
}

#[tokio::test]
async fn card22c_identity_404_and_invalid_data_fall_back() {
    for (status, body) in [
        (404, "{}"),
        (200, "not-json"),
        (200, "{\"Items\":null}"),
        (200, "{\"Items\":[null,{\"Id\":123}]}"),
        (400, "{}"),
    ] {
        let (source, server) = fixture(move |request| {
            if batch_ids(request).is_some() {
                (status, "application/json".into(), body.into())
            } else {
                json_response(people_page())
            }
        })
        .await;
        let result = item_metadata(json!({"source":source,"limit":3}))
            .await
            .unwrap();
        assert_eq!(result["items"].as_array().unwrap().len(), 3);
        assert_eq!(result["items"][0]["people"][0]["providerIds"], json!({}));
        assert_eq!(result["items"][0]["people"][0]["hasPrimary"], false);
        assert_eq!(result["items"][2]["people"][1]["providerIds"], json!({}));
        assert_eq!(result["items"][2]["people"][1]["hasPrimary"], true);
        server.abort();
    }
}

#[tokio::test]
async fn card22c_batch_authentication_and_retryable_errors_propagate() {
    for (status, code) in [
        (401, "PLUGIN_AUTH_FAILED"),
        (403, "PLUGIN_AUTH_FAILED"),
        (429, "PLUGIN_RATE_LIMITED"),
        (500, "PLUGIN_RATE_LIMITED"),
        (503, "PLUGIN_RATE_LIMITED"),
    ] {
        let (source, server) = fixture(move |request| {
            if batch_ids(request).is_some() {
                (status, "application/json".into(), "{}".into())
            } else {
                json_response(people_page())
            }
        })
        .await;
        let error = item_metadata(json!({"source":source,"limit":3}))
            .await
            .unwrap_err();
        assert_eq!(error.code, code, "HTTP {status}");
        server.abort();
    }
}

#[tokio::test]
async fn card22c_invalid_person_id_is_never_requested() {
    let (source, server, requests) = observed_fixture(|request| {
        if let Some(ids) = batch_ids(request) { json_response(person_details(&ids)) }
        else { json_response(json!({"Items":[{"Id":"1","People":[{"Id":"../bad?x=1","Name":"Invalid"},{"Id":"101","Name":"Valid"}]}],"TotalRecordCount":1})) }
    }).await;
    let result = item_metadata(json!({"source":source,"limit":1}))
        .await
        .unwrap();
    assert_eq!(result["items"][0]["people"].as_array().unwrap().len(), 1);
    assert_eq!(
        result["items"][0]["people"][0]["providerIds"]["Tmdb"],
        "101"
    );
    assert_eq!(
        requests
            .lock()
            .unwrap()
            .iter()
            .filter_map(|request| batch_ids(request))
            .collect::<Vec<_>>(),
        vec![vec!["101"]]
    );
    server.abort();
}
