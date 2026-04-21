# digdag-wrapper

A wrapper suite for Digdag that provides automated server lifecycle management, enhanced CLI commands, and a web-based monitoring dashboard — optimized for multi-user HPC environments.

## 📦 Contents

| File                         | Description                                 |
| ---------------------------- | ------------------------------------------- |
| `digdag.sh`                  | Main wrapper script with custom subcommands |
| `digdag_dashboard.py`        | FastAPI-based web monitoring dashboard      |
| `digdag_dashboard_launch.sh` | Launcher script for the dashboard           |
| `xvfb_lib.sh`                | Xvfb helper library for headless execution  |

## 🔧 Requirements

- **Java** 8+ (required for Digdag)
- **Python 3.8+** (for dashboard)
- **Dependencies**: `fastapi`, `uvicorn`
- **Optional**: `xvfb-run` (for headless browser access)

```bash
# Install Python dependencies
pip install fastapi uvicorn
```

## 🚀 Quick Start

### 1. Server Management

```bash
# Start a persistent server (reuses existing if available)
./digdag.sh start_server

# Run workflow: start server → push → start
./digdag.sh run_workflow my_project my_workflow

# Disposable mode: dedicated server per run, auto-shutdown
./digdag.sh run_workflow --once my_project my_workflow

# Kill your server
./digdag.sh kill_server
```

### 2. Job Management

```bash
# List running jobs
./digdag.sh list_job

# List all jobs (including completed)
./digdag.sh list_job --all

# Kill a job
./digdag.sh kill_job

# Kill all jobs for a specific project
./digdag.sh kill_job --all -p my_project
```

### 3. Dashboard (Web UI)

```bash
# Start dashboard and open browser
./digdag_dashboard_launch.sh

# Force restart dashboard
./digdag_dashboard_launch.sh --restart

# Stop dashboard
./digdag_dashboard_launch.sh --stop
```

Access: `http://<host>:8765`

### 4. Open Digdag UI

```bash
./digdag.sh browse
```

## 📋 Command Reference

### digdag.sh

| Command                                    | Description                                     |
| ------------------------------------------ | ----------------------------------------------- |
| `start_server`                             | Start a persistent server (one per user)        |
| `kill_server`                              | Stop the server with confirmation               |
| `list_server`                              | Show all running servers (table view)           |
| `run_workflow <project> <workflow>`        | Boot server → push → start                      |
| `run_workflow --once <project> <workflow>` | Disposable mode: auto-shutdown after completion |
| `list_job`                                 | Show attempts (auto-select if single server)    |
| `list_job --all`                           | Show all attempts including completed           |
| `kill_job`                                 | Kill running attempts                           |
| `browse`                                   | Open Digdag UI in browser                       |

**Options for `run_workflow`:**

| Option                     | Description                                    |
| -------------------------- | ---------------------------------------------- |
| `-d, --project <dir>`      | Project directory (default: current directory) |
| `-P, --params-file <file>` | External parameter file                        |
| `-L, --log <file>`         | Log file path (--once mode only)               |

**Options for `list_job` / `kill_job`:**

| Option          | Description                                    |
| --------------- | ---------------------------------------------- |
| `--all`         | Show/kill all statuses (default: running only) |
| `-p <project>`  | Filter by project name                         |
| `-w <workflow>` | Filter by workflow name                        |

### digdag_dashboard_launch.sh

| Flag        | Description                       |
| ----------- | --------------------------------- |
| (none)      | Start or reuse existing dashboard |
| `--restart` | Kill existing and restart         |
| `--stop`    | Stop dashboard only               |

## 🏗 Architecture

### File Structure

```
/tmp/digdag_<user>/
├── server.log.<PID>   # Server log
├── server.info        # PORT / PID / URL / STARTED
├── server.lock        # Race condition prevention
├── task-logs/         # Task execution logs
├── jvm-tmp/           # JVM temp directory
├── dashboard/         # Dashboard files
│   ├── dashboard.pid  # "<PID> <PORT>"
│   └── dashboard.log
└── once.<timestamp>/  # Disposable server directories
```

### Key Features

- **Per-user isolation**: Each OS user has their own server instance
- **Race condition protection**: Lock mechanism prevents duplicate servers
- **Auto port assignment**: Automatically finds free ports (base: 65432)
- **Persistent execution**: `setsid` + `disown` detaches server from parent
- **NFS-friendly**: Uses `/tmp` instead of home directory to save quota

### Environment Variables

| Variable         | Default                                      | Description                 |
| ---------------- | -------------------------------------------- | --------------------------- |
| `DIGDAG_JAR`     | `/user/qarepo/usr/local/digdag-0.10.5.1.jar` | Path to Digdag JAR          |
| `DIGDAG_SH`      | `/user/qarepo/usr/local/bin/digdag.sh`       | Path to wrapper (dashboard) |
| `DASHBOARD_PORT` | `8765`                                       | Dashboard web server port   |
| `DASHBOARD_HOST` | `0.0.0.0`                                    | Dashboard bind address      |

## 📄 License

See [LICENSE](LICENSE) file.
