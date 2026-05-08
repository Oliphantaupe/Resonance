# run_local.ps1 — Run the pipeline locally without Docker
# Requires: Python 3.10+, Java 17+ (both verified present)
#
# Usage:
#   .\run_local.ps1              # full pipeline + start dashboard
#   .\run_local.ps1 -Stage bronze
#   .\run_local.ps1 -Stage dashboard

param(
    [string]$Stage = "all"
)

$ErrorActionPreference = "Stop"
$Root = $PSScriptRoot
$SrcPath = Join-Path $Root "src"

# Point DATA_ROOT at the local data directory
$env:DATA_ROOT  = Join-Path $Root "data"
$env:PYTHONPATH = $SrcPath

# Windows: Hadoop winutils + hadoop.dll required for Spark local-mode writes.
# Download from: https://github.com/cdarlint/winutils (hadoop-3.3.4 matches PySpark 3.5.x)
if (-not $env:HADOOP_HOME) {
    $defaultHadoop = "C:\hadoop"
    if (Test-Path "$defaultHadoop\bin\winutils.exe") {
        $env:HADOOP_HOME = $defaultHadoop
        $env:PATH = "$defaultHadoop\bin;" + $env:PATH
        Write-Host "HADOOP_HOME = $env:HADOOP_HOME  (auto-detected)"
    } else {
        Write-Warning "HADOOP_HOME not set and C:\hadoop\bin\winutils.exe not found."
        Write-Warning "Download hadoop-3.3.4 binaries from https://github.com/cdarlint/winutils"
        Write-Warning "and place winutils.exe + hadoop.dll in C:\hadoop\bin"
    }
} else {
    $env:PATH = "$env:HADOOP_HOME\bin;" + $env:PATH
    Write-Host "HADOOP_HOME = $env:HADOOP_HOME"
}

Write-Host "DATA_ROOT  = $env:DATA_ROOT"
Write-Host "PYTHONPATH = $env:PYTHONPATH"

function Install-Deps {
    Write-Host "`n[setup] Installing Python dependencies..."
    pip install -q pyspark==3.5.1 delta-spark==3.2.0 pandas==2.2.2 pyarrow==16.1.0 `
        deltalake==0.18.2 dash==2.17.1 dash-bootstrap-components==1.6.0 plotly==5.22.0
    Write-Host "[setup] Done."
}

function Run-Pipeline {
    param([string]$RunBronze="true", [string]$RunSilver="true", [string]$RunGold="true")
    $env:RUN_BRONZE = $RunBronze
    $env:RUN_SILVER = $RunSilver
    $env:RUN_GOLD   = $RunGold
    Write-Host "`n[pipeline] Starting (Bronze=$RunBronze Silver=$RunSilver Gold=$RunGold)..."
    python -m bigdata_music.pipeline
}

function Run-Dashboard {
    Write-Host "`n[dashboard] Starting at http://localhost:8050 ..."
    python (Join-Path $Root "dashboard\app.py")
}

function Run-Tests {
    Write-Host "`n[tests] Running pytest..."
    pytest (Join-Path $Root "tests") -v
}

# Install if not already present
try { python -c "import pyspark" 2>$null } catch { Install-Deps }

switch ($Stage) {
    "all"       { Run-Pipeline; Run-Dashboard }
    "bronze"    { Run-Pipeline -RunSilver false -RunGold false }
    "silver"    { Run-Pipeline -RunBronze false -RunGold false }
    "gold"      { Run-Pipeline -RunBronze false -RunSilver false }
    "pipeline"  { Run-Pipeline }
    "dashboard" { Run-Dashboard }
    "test"      { Run-Tests }
    default     { Write-Host "Unknown stage: $Stage. Use: all|bronze|silver|gold|pipeline|dashboard|test" }
}
