# dsh-session-sync

在多台电脑之间同步 DeepSeek Harness 的会话历史。默认使用 OneDrive 里的 `dsh-session-sync` 文件夹；也可以指向任何被网盘或 Syncthing 同步的目录。

## 安装

1. 把整个目录复制到目标电脑的任意位置（例如 `D:\tools\dsh-session-sync`）。
2. 在该电脑的 DSH 会话里让 agent 用 `plugin_manager` 的 `install_bundle` 安装这个目录；安装后插件会出现在 Plugin Manager 里，标题为“会话同步”。
3. 确保两台电脑的 OneDrive 登录的是同一个账号，并且 `dsh-session-sync` 目录已同步到本地。

安装后 `/sync status` 会显示共享目录、账号命名空间和设备 id。两台机器的 `accountKey` 必须一致：登录 DeepSeek 账号时是 `user-<id>`，未登录或只用 API key 时是 `local`。

## 工作方式

- 读取和写入都走 `ctx.sessionPersistence`，本地 JSONL 日志的锁、格式校验和崩溃恢复仍然由 Harness 负责。
- 跨机器复制的是**逻辑事件**（`SessionEvent`），不是 `.jsonl.zstd` 文件。因此两台机器可以接着同一个 session id 继续对话。
- 共享目录里只放不可变文件：每个会话一个 `header.json`，以及按 seq 区间命名的内容寻址事件段。没有需要合并的远端状态文件。
- 本机的同步水位、冲突状态、设备 id 存在 `$DSH_HOME/storages/session-sync/`。

## 配置

在 `cordis.patch.yml` 中覆盖 `session-sync` 这一行的配置：

```yaml
- id: session-sync
  config:
    enabled: true
    remoteDir: 'C:/Users/me/OneDrive/dsh-session-sync'
    intervalMs: 60000
    includeSubagents: false
    workspaceMap:
      - { remote: 'D:/work/project', local: 'D:/work/project' }
```

| 字段 | 默认 | 说明 |
|---|---|---|
| `enabled` | `true` | 关闭自动同步；`/sync` 仍可手动跑一次 |
| `remoteDir` | `<OneDrive>/dsh-session-sync` | 共享根目录；未配置时自动检测 OneDrive |
| `intervalMs` | `60000` | 自动同步间隔，最小 15000 |
| `includeSubagents` | `false` | 是否同步子代理会话 |
| `workspaceMap` | `[]` | 远端 cwd 到本机 cwd 的映射 |
| `accountKey` | 账号 userId 或 `local` | 远端命名空间；两台机器要一致 |
| `deviceName` | 主机名 | 共享目录里显示的设备名 |

## 同步规则

- 一台机器只追加、不改写；本地日志仍然是唯一事实来源。
- 正常情况下，谁的日志更长就补齐给另一方。
- 如果两台机器都在上次同步之后追加了不同事件，该会话会被**冻结**：本地后缀写进共享目录的 `conflicts/`，不再自动上传或下载。`/sync status` 会列出来。
- 两个工作目录不同的机器，导入时用 `workspaceMap` 改写会话头的 `cwd`；没有映射的会话仍会导入，但可能不归属任何项目。
- 不同 Harness 格式版本的会话会被拒绝，不做跨版本事件迁移。

## 命令

- `/sync`：立即同步一次。
- `/sync status`：查看共享目录、账号、设备、上次结果和冲突。

## 已知限制

- 同一会话在两台机器上同时打开时不做实时协作；冻结策略只保证不丢数据。
- 只同步会话记录，不同步工作区文件、附件、终端和后台任务。
- 删除不会同步：DSH 没有会话删除 API，共享目录只增不减。
- 冲突目前需要人工处理，且两侧数据都会保留：本地后缀已写进共享目录的 `conflicts/<sessionId>/`，远端分支仍在共享目录里。确认保留哪一侧后手动处理；当前版本不会自动合并或自动生成冲突副本。
