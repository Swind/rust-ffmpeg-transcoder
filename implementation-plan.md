# HTTP Streaming Transcoder 實作規劃

本計畫配合 `streaming-transcoder-implementation-spec.md` 使用；API、狀態機、安全及 T01–T18 驗收以原規格為準。不實作持久化、斷點續傳、任意 FFmpeg 參數、媒體落盤或多節點排程。

## 工作方式

- Implementation agents 負責所有程式碼、測試、部署與操作文件；主 agent 只負責本計畫與最終驗收。
- 單一 Rust package，edition 2024，產出 `transcoder-server` 與 `transcoder-client`。
- Server agent 是整合 owner，擁有 Cargo.toml/Cargo.lock、src/lib.rs、src/bin/server.rs、所有 server 模組、Rust 測試、Dockerfile、compose.yaml、config.example.toml。
- Client/acceptance agent 擁有 src/bin/client.rs、scripts/ 下的驗收與 fixture 工具、README.md；不得修改 Cargo manifest 或 server 模組。
- 兩組依以下契約並行；契約缺漏先以訊息協調，不自行製造第二套介面。
- 各 agent 不在實作中途反覆執行 build/lint/test/formatter；完成整合後由主 agent 執行最終驗收，發現問題交回原 owner 修正。

## 技術與責任

Tokio + Axum + Serde + Tracing；CLI 使用 Reqwest rustls HTTP client。固定 profile command argument vectors，直接 spawn FFmpeg，不經 shell。

建議 server 模組：config、api、auth、error、session、scheduler、supervisor、stream、ffmpeg。採簡單 owned state，metadata lock 不得跨 network/pipe await。

Server manifest 應提供 client 所需依賴：tokio（full）、reqwest（json、stream、rustls-tls，default-features=false）、tokio-util（io、rt）、futures-util、bytes、serde（derive）、serde_json。其他依賴由 server owner 選擇相容版本；提交 Cargo.lock。

## 跨組可執行契約

### Server

```sh
cargo run --bin transcoder-server -- --config config.example.toml
```

- `--config PATH` 可選，省略時使用原規格預設值；未知或錯誤參數明確報錯。
- TOML section/key 沿用原規格第 10 節。補充 `[scheduler].max_sessions = 1024` 限制含 terminal retention 的總 metadata 數量。
- `[security].allow_insecure_no_auth = true` 只容許 loopback；若要在非 loopback 無認證運作，還需明確設定 `allow_insecure_non_loopback = true`。有 token 時仍使用認證。
- `[timeouts].max_session_secs = 0` 代表停用總時限；其他 timeout 與容量應驗證合理值。
- `[ffmpeg].max_input_bytes = 0` 代表不設上傳大小上限，非零時套用 per-session 限制；不得依 Content-Length 預先分配媒體容量。
- `/healthz`、`/readyz` 不需認證；其餘路由需 token。錯誤 envelope 和 status 路由 schema 沿用原規格。
- `/v1/profiles` 回傳 `{"profiles":[{"id":"h264-cpu-mkv","available":true}]}`；設定但探測失敗的 profile 為 available=false 並帶 reason。建立 unavailable profile 不啟動程序，回報 422 invalid_profile。
- DELETE 統一回傳 204，包括已存在的 terminal session；未知 ID 為 404。
- 強制 output 先附接才可 PUT；否則 409，code=`output_not_attached`，不消耗 input 所有權。
- READY attach deadline 涵蓋 output 與 input 的附接，避免只開 output 就永遠占用 slot。
- input/output connected 為目前連線狀態，attach-once flags 分開保存；completed 時兩者為 false。
- status 提供原規格必需欄位；可額外提供有界 stderr_tail 及 timings，禁止暴露 token。

### Client

```sh
TRANSCODER_API_TOKEN=... cargo run --bin transcoder-client -- \
  --base-url http://127.0.0.1:8080 \
  --profile h264-cpu-mkv --input source.ts --output converted.mkv
```

- token 從 TRANSCODER_API_TOKEN 讀取；本機明確無認證 server 可不設定。
- 建立 session、等待 READY、先啟動 output GET 並確認成功 headers，再並發串流 PUT（檔案大小作 Content-Length）與讀取下載。
- 驗證 upload 204、正常 download EOF、最終 completed；其他結果 nonzero。失敗盡力 DELETE 並刪除本次 partial output。
- 不覆蓋既有 output；不刪除使用者原有檔案。client 端寫檔是容許的，server 端不得落盤。
- 輸入檔案串流讀取，不 collect 全檔；輸出逐段寫入。
- 全流程有 deadline，等待 headers 需容許尚未開始 upload 的狀態；不能因 request orchestration 卡死。

### 驗收入口

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
python3 scripts/acceptance.py --server target/debug/transcoder-server --client target/debug/transcoder-client
```

最後一項由 client/acceptance agent 實作，使用 Python 標準函式庫、真實 FFmpeg/ffprobe、暫存目錄與獨立 server process；可用自訂實際清楚命名的參數，但需同步 README 並通知主 agent。驗收素材只存在 client/test temp dir，測試後清理。每個情境有 deadline，失敗 nonzero。GPU smoke opt-in。

## 核心不變量與 timeout

1. 每個 active session 一個 supervisor 獨占 Child、一個 scheduler permit。stdin/stdout 各 handoff 一次，控制通道有界。
2. Supervisor 是唯一 terminal transition owner。清理中維持非終態，停止及 reap child、回收 I/O 工作後發布 terminal 並釋放 permit，失敗不可被後來的成功覆寫。
3. stderr 一直並發排空，尾端大小有上限；媒體走 bounded streaming adapters，不使用無界 media channel、read_to_end、collect 或 wait_with_output。
4. Upload counter 按實際成功寫入 stdin 的 bytes 增加，處理 partial write 與 checked arithmetic；正常 EOF 且長度精確匹配後才關 stdin。
5. stdout EOF 不等於成功；完整 upload、child exit=0、stdout EOF、response body 正常結束共同決定 completed。HTTP 200 後出錯必須使 response stream 失敗而非正常結束。
6. Response stream premature drop 通知 supervisor，採 failed/output_disconnected；不可重新 attach。
7. Input idle 計時只涵蓋等待下一段 body 資料，不涵蓋背壓造成 stdin write 等待。
8. Output stall 只在資料已產出而 downstream 未繼續消費時啟動，由獨立 supervisor timer 偵測，不把等待 codec 產出當作 consumer stall，也不能依賴停止被 poll 的 stream 自己計時。
9. 總時限從 READY 開始，涵蓋 running/draining；queue 與 metadata 數量均有界。
10. SIGTERM 停止新 session、取消 queue/worker，在 grace 內清理，必要時 kill 並 reap。kill_on_drop 只是保險，不代替受控清理。
11. 限制 FFmpeg 輸入 protocol，避免受控媒體引用外部檔案或 URL；固定 CLI 不能獨自防止此問題。兼容性以真實 TS/MKV fixture 驗證。

## 交付階段

### Phase 0：基礎環境

Cargo、config、tracing、認證、health/readiness、CPU encoder/ffprobe 探測、fixture 產生。驗收 pipe:0 → CPU libx264 → pipe:1 的 MKV 可被 ffprobe 解析。記錄 non-seekable MP4 限制。

### Phase 1：單 session 串流閉環

完整 create/status/input/output/cancel、supervisor、stdin/stdout/stderr 並行、真實 EOF 與 exit 判定、斷線清理、最小 CLI。限速 upload 完成前收到 output，音訊與無音訊輸入都可產出有效容器。

### Phase 2：排程與韌性

有界 FIFO、max concurrent/queued/session、timeout、retention、attach race、shutdown、counter 與 timings。驗收所有拒絕與失敗路徑、PID reaping、配額重用，不只 happy path。

### Phase 3：部署與操作

非 root Docker、多階段 build、CPU FFmpeg、healthcheck、Compose 的 bind 與 port 一致、token、資源限制、read-only filesystem 與 shutdown grace。README 包含 quick start、API、兩終端 curl、CLI、錯誤診斷及明確計數語義。

### Phase 4：可選 VAAPI

固定 h264-vaapi-mkv profile、可設定 render node、encoder/device/實際 synthetic encode 探測、容器 GPU mapping 與 group permissions。GPU profile 不可用不影響 CPU 工作；readiness 明確呈現已設定 profile 的失敗。無真實 GPU 環境時交付可執行 opt-in smoke，不聲稱硬體驗證或效能數據。

## 最終驗收矩陣

- T01/T02/T06/T07：API、auth/profile、length、競爭附接，不污染 stdin。
- T03/T04/T05/T16：真實 FFmpeg，記錄 first-output 與 upload-finished 時間，ffprobe 驗證 H.264/AAC 與 final completed。
- T08/T09/T10/T14：input/output disconnect、FFmpeg spawn/nonzero、queued/active cancel；終態穩定、child reaped、permit reused。
- T11：暫停下載直到填滿有限 HTTP/socket/pipe 緩衝，確認 upload 受背壓；恢復消費後成功。區分 server RSS 與 FFmpeg RSS，不假設 FFmpeg 內部 buffer 無限小。
- T12/T13/T15：attach timeout、FIFO/concurrency、SIGTERM cleanup。
- T17：已知 tail-metadata/nonseekable MP4 fixture，確定失敗且診斷清楚，不宣稱所有 MP4 必然失敗。
- T18：真實 GPU opt-in；設備或 Docker 缺失要明確報告未驗證，不以 mock 當作通過。

測試使用隔離 process/port/temp dir，不依賴外部網路，全部有 deadline。永久 regression tests 聚焦可觀察的競態、錯誤與資源回收；不測 source text、接線或文字措辭。

## 完成定義

規格 MVP API、CPU 串流、FIFO、bounded memory、認證、取消/reaping、CLI、部署文件與自動驗收工具全部交付；Cargo.lock 存在。主 agent 執行最終格式、lint、測試與實際 CLI/HTTP 場景驗收，失敗交回 implementation owner 修正。報告只列實際執行的結果，Docker/GPU 環境限制獨立列出。未要求自動 commit。
