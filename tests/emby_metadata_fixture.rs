use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use xiyingd::application::emby_migration::{collections, item_metadata, person_image};

async fn fixture<F>(handler: F) -> (Value, tokio::task::JoinHandle<()>)
where
    F: Fn(&str) -> (u16, String, String) + Send + Sync + 'static,
{
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
            let (status, content_type, body) = handler(&request);
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
    )
}
#[tokio::test]
async fn card21_metadata_paginates_and_projects_people_streams_without_urls() {
    let (source,server)=fixture(|request|{
        let body=if request.starts_with("GET /Items/p1?") {json!({"ProviderIds":{"Tmdb":"12"},"ImageTags":{"Primary":"tag"}})} else {
            assert!(request.contains("IncludeItemTypes=Movie%2CSeries%2CSeason%2CEpisode"));assert!(request.contains("ParentId=lib1"));assert!(request.contains("StartIndex=5"));
            json!({"TotalRecordCount":7,"Items":[{"Id":"m1","Name":"Film","Type":"Movie","Path":"/media/Films/a.strm","RunTimeTicks":72000000000i64,
                "People":[{"Id":"p1","Name":"Actor","Type":"Actor","Role":"Lead"}],"MediaSources":[{"Id":"s1","Container":"mkv","MediaStreams":[{"Index":0,"Type":"Video","Codec":"hevc","Width":3840,"Height":2160}]}]}]})
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
