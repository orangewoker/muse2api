# Docker Compose 服务器部署（维护分支 v1.5.4）

适用于已安装 Docker Engine 和 Compose v2 插件的 Linux 服务器。
不需要在宿主机安装 Python 或 Chromium；浏览器和依赖都在镜像中。

## 首次安装

```bash
git clone https://github.com/orangewoker/muse2api.git
cd muse2api
cp .env.example .env
```

编辑 `.env`，至少确认以下配置：

```dotenv
MUSE2API_KEY=替换为你自己的随机密钥
MUSE2API_PORT=18610
MUSE2API_BIND=0.0.0.0
MUSE2API_PUBLIC_BASE=http://你的服务器IP:18610
MUSE2API_REPO=https://github.com/orangewoker/muse2api
```

可以用 `openssl rand -hex 24` 生成随机密钥，手动填入 `MUSE2API_KEY`。
若通过 HTTPS 域名访问，将 `MUSE2API_PUBLIC_BASE` 改成实际外部地址，末尾不要加 `/v1`。
若只通过同机 Nginx 反代，可以把 `MUSE2API_BIND` 设为 `127.0.0.1`。
`MUSE2API_PORT` 只配置宿主机映射端口，容器内部始终监听 `18610`。

```bash
docker compose config --quiet
docker compose up -d --build
docker compose ps
docker compose logs --tail=100 muse2api
curl http://127.0.0.1:18610/healthz
```

访问 `http://你的服务器IP:18610/admin`，在页面输入密钥。通过页面下载 Chrome 扩展，
在自己电脑的浏览器里登录 Muse，再用扩展把 Cookie 导入服务器。
服务器本身不需要图形桌面或人工登录浏览器。

客户端 Base URL 为 `http://你的服务器IP:18610/v1`，API Key 使用同一密钥。
账号导入前 `/readyz` 返回 `no_account` 是正常的；健康检查通过不代表已验证上游账号。

## 配置读取和数据保留

- Compose 插值优先级：宿主环境变量 > 项目 `.env` > 默认值。
  如出现密钥不一致，先检查 `printenv MUSE2API_KEY`；移除不需要的宿主环境变量后重新创建容器。
- 密钥留空时自动生成，启动日志会显示；值保存在 `data/.api_key`，容器重建继续使用。
- 显式配置的 `MUSE2API_KEY` 优先于自动密钥文件。若在管理页轮换密钥，同时修改宿主机 `.env`，
  否则下次重建时仍会加载原显式配置。
- 账号、任务、媒体、自动密钥和浏览器 profile 均保留在宿主机 `./data`。
  更新前备份 `.env` 和整个 `data`；不要上传这两个位置的实际凭据。
- 此版本是单浏览器 FIFO **串行**生成，不要给 uvicorn 增加多个 workers。

## 更新

```bash
git pull --ff-only
docker compose up -d --build
docker compose ps
```

管理页在线升级默认读取本维护仓库，不会自动切回原发布者仓库。
Docker 推荐使用上面的重建方式，使正在运行的代码与镜像内容一致；容器内在线更新的代码
在下一次重建时会被镜像替换，但挂载的账号和媒体数据不受影响。

反向代理配置参考 `deploy/nginx.example.conf`：关闭 `proxy_buffering` 并增加超时。
长时间生图优先使用 `POST /v1/images/tasks` 后轮询 `GET /v1/images/tasks/{id}`，
可通过 `Idempotency-Key` 去重，避免长连接超时后重复生成。

## 任务状态与诊断

- 图片/视频任务状态：`queued` → `processing` → `completed` / `failed`。
- 排队中进度为 `0`；超时或服务重启时会写入失败终态，不会永久留在队列。
- 视频超过 120 秒无心跳时，查询返回 `stalled` 诊断；持续产生心跳的慢任务不会被误判。
- `GET /admin/scheduler`（Bearer 鉴权）可查看队列、当前任务及完成/失败/超时计数。
- 时长允许 5/6/8/10 秒请求，实际成片仍由 Muse 上游决定。
- 比例和参考图在入队前验证；HTTP 远程参考图不在请求入口下载，下载/上传失败在任务中明确报错。

## 离线回归测试

安装 Python 依赖后在仓库根目录执行，不会读取真实账号或消耗生成额度：

```bash
python tests/test_scheduler.py
python tests/test_cdp.py
python tests/test_api_concurrency.py
python tests/test_async_images.py
python tests/test_session_health.py
python tests/test_vm_wait.py engine.py --assert
python tests/test_compose_config.py
python tests/test_media_selection.py
```

最后一项需要 Chromium；`test_compose_config.py` 只需要 Compose CLI，不需要 Docker daemon。
可用 `python tests/run_regressions.py` 一次运行全部离线测试。
构建镜像后，`python tests/test_docker_smoke.py` 还会用独立临时数据目录和随机本机端口验证
Compose 启动、鉴权、自动密钥及账号/媒体的重建持久化；不会使用仓库中的实际账号数据。
