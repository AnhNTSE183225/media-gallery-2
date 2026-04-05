$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$serverDir = Join-Path $root "server"
$clientDir = Join-Path $root "client"

function Get-ProcessCommandLine {
  param([int]$ProcessId)

  $proc = Get-CimInstance Win32_Process -Filter "ProcessId = $ProcessId" -ErrorAction SilentlyContinue
  if ($proc) {
    return [string]$proc.CommandLine
  }

  return ""
}

function Stop-ProcessTree {
  param(
    [int]$ProcessId,
    [string]$Label
  )

  if (-not $ProcessId) {
    return
  }

  try {
    taskkill /PID $ProcessId /T /F | Out-Null
    Write-Host "Stopped stale $Label process (PID $ProcessId)." -ForegroundColor Yellow
  } catch {
    Write-Host "Could not stop stale $Label process (PID $ProcessId): $($_.Exception.Message)" -ForegroundColor Yellow
  }
}

function Test-CommandLineMatchesAny {
  param(
    [string]$CommandLine,
    [string[]]$Tokens
  )

  if (-not $CommandLine) {
    return $false
  }

  $normalized = $CommandLine.ToLower()
  foreach ($token in $Tokens) {
    if ($normalized -like "*$($token.ToLower())*") {
      return $true
    }
  }

  return $false
}

function Remove-StaleDevProcesses {
  $portRules = @(
    @{ Port = 3001; Label = 'Backend'; MatchAny = @('sbt-launch.jar', 'media-gallery-2\\server', 'mg2.main') },
    @{ Port = 5173; Label = 'Frontend'; MatchAny = @('vite', 'npm', 'media-gallery-2\\client') }
  )

  foreach ($rule in $portRules) {
    $listeners = Get-NetTCPConnection -State Listen -LocalPort $rule.Port -ErrorAction SilentlyContinue |
      Select-Object -ExpandProperty OwningProcess -Unique

    foreach ($listenerProcessId in $listeners) {
      $commandLine = Get-ProcessCommandLine -ProcessId $listenerProcessId
      if (Test-CommandLineMatchesAny -CommandLine $commandLine -Tokens $rule.MatchAny) {
        Stop-ProcessTree -ProcessId $listenerProcessId -Label $rule.Label
      } else {
        throw "Port $($rule.Port) is in use by PID $listenerProcessId ($commandLine). Resolve this conflict and run again."
      }
    }
  }
}

function Get-NpmExecutable {
  $npmCommand = Get-Command npm.cmd -CommandType Application -ErrorAction SilentlyContinue |
    Select-Object -First 1

  if ($npmCommand -and $npmCommand.Source) {
    return $npmCommand.Source
  }

  throw "Could not locate npm.cmd in PATH. Ensure Node.js is installed."
}

function Get-SbtExecutable {
  $sbtCommand = Get-Command sbt.bat -CommandType Application -ErrorAction SilentlyContinue |
    Select-Object -First 1

  if ($sbtCommand -and $sbtCommand.Source) {
    return $sbtCommand.Source
  }

  $coursierSbt = Join-Path $env:USERPROFILE "AppData\Local\Coursier\data\bin\sbt.bat"
  if (Test-Path $coursierSbt) {
    return $coursierSbt
  }

  throw "Could not locate sbt.bat. Install sbt or Coursier and ensure sbt is available."
}

function Get-TimestampedLogPath {
  $logsDir = Join-Path $root "logs"
  if (-not (Test-Path $logsDir)) {
    New-Item -ItemType Directory -Path $logsDir -Force | Out-Null
  }
  
  $timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
  return Join-Path $logsDir "log-$timestamp.log"
}

function Assert-DockerAvailable {
  $docker = Get-Command docker -CommandType Application -ErrorAction SilentlyContinue |
    Select-Object -First 1

  if (-not $docker) {
    throw "Docker CLI not found in PATH. Install Docker Desktop and ensure docker is available."
  }

  try {
    docker info | Out-Null
  } catch {
    throw "Docker daemon is not available. Start Docker Desktop and retry."
  }
}

function Wait-ContainerHealthy {
  param(
    [string]$Service,
    [int]$TimeoutSeconds = 90
  )

  $containerId = docker compose -f (Join-Path $serverDir "docker-compose.yml") ps -q $Service
  if (-not $containerId) {
    throw "Service '$Service' has no container ID."
  }

  $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
  while ((Get-Date) -lt $deadline) {
    $status = docker inspect -f "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}" $containerId 2>$null
    if ($status -eq "healthy" -or $status -eq "running") {
      Write-Host "Service '$Service' status: $status" -ForegroundColor Green
      return
    }

    Write-Host "Waiting for service '$Service' health. Current status: $status" -ForegroundColor DarkGray
    Start-Sleep -Seconds 1
  }

  throw "Service '$Service' did not become healthy in time."
}

function Wait-LocalPortListening {
  param(
    [int]$Port,
    [int]$ExpectedProcessId = 0,
    [int]$TimeoutSeconds = 120
  )

  $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
  while ((Get-Date) -lt $deadline) {
    if ($ExpectedProcessId -gt 0 -and -not (Test-ProcessRunning -ProcessId $ExpectedProcessId)) {
      throw "Backend process exited before opening port $Port."
    }

    $listeners = Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue
    if ($listeners) {
      Write-Host "Backend port $Port is listening." -ForegroundColor Green
      return
    }

    Write-Host "Waiting for backend to listen on port $Port..." -ForegroundColor DarkGray
    Start-Sleep -Seconds 1
  }

  throw "Timeout waiting for backend port $Port to become ready."
}

function Ensure-DockerInfra {
  Write-Host "Ensuring Docker infrastructure is up..." -ForegroundColor Green
  $composeFile = Join-Path $serverDir "docker-compose.yml"
  docker compose -f $composeFile up -d | Out-Null

  Wait-ContainerHealthy -Service "postgres"
  Wait-ContainerHealthy -Service "redis"
}

function Start-ManagedProcess {
  param(
    [string]$Name,
    [string]$Prefix,
    [string]$Color,
    [string]$WorkingDirectory,
    [string]$FileName,
    [string]$Arguments,
    [hashtable]$EnvironmentOverrides = @{},
    [string]$LogFilePath = ""
  )

  $psi = New-Object System.Diagnostics.ProcessStartInfo
  $psi.FileName = $FileName
  $psi.Arguments = $Arguments
  $psi.WorkingDirectory = $WorkingDirectory
  $psi.UseShellExecute = $false
  $psi.RedirectStandardOutput = $true
  $psi.RedirectStandardError = $true
  $psi.CreateNoWindow = $true

  foreach ($key in $EnvironmentOverrides.Keys) {
    $psi.Environment[$key] = [string]$EnvironmentOverrides[$key]
  }

  $process = New-Object System.Diagnostics.Process
  $process.StartInfo = $psi
  $process.EnableRaisingEvents = $true

  if (-not $process.Start()) {
    throw "Failed to start $Name"
  }

  $outEvent = Register-ObjectEvent -InputObject $process -EventName OutputDataReceived -Action {
    if ($EventArgs.Data) {
      $message = "[$($Event.MessageData.Prefix)] $($EventArgs.Data)"
      Write-Host $message -ForegroundColor $Event.MessageData.Color
      if ($Event.MessageData.LogFile) {
        $message | Add-Content -Path $Event.MessageData.LogFile -Encoding UTF8
      }
    }
  } -MessageData @{ Prefix = $Prefix; Color = $Color; LogFile = $LogFilePath }

  $errEvent = Register-ObjectEvent -InputObject $process -EventName ErrorDataReceived -Action {
    if ($EventArgs.Data) {
      $message = "[$($Event.MessageData.Prefix)] $($EventArgs.Data)"
      Write-Host $message -ForegroundColor Red
      if ($Event.MessageData.LogFile) {
        $message | Add-Content -Path $Event.MessageData.LogFile -Encoding UTF8
      }
    }
  } -MessageData @{ Prefix = $Prefix; Color = $Color; LogFile = $LogFilePath }

  $process.BeginOutputReadLine()
  $process.BeginErrorReadLine()

  return [PSCustomObject]@{
    Name = $Name
    Pid = $process.Id
    Process = $process
    OutputEvent = $outEvent
    ErrorEvent = $errEvent
  }
}

function Test-ProcessRunning {
  param([int]$ProcessId)

  if (-not $ProcessId) {
    return $false
  }

  $existing = Get-Process -Id $ProcessId -ErrorAction SilentlyContinue
  return $null -ne $existing
}

function Stop-ManagedProcess {
  param($ManagedProcess)

  if ($null -eq $ManagedProcess) {
    return
  }

  if ($ManagedProcess.OutputEvent) {
    Unregister-Event -SubscriptionId $ManagedProcess.OutputEvent.Id -ErrorAction SilentlyContinue
  }

  if ($ManagedProcess.ErrorEvent) {
    Unregister-Event -SubscriptionId $ManagedProcess.ErrorEvent.Id -ErrorAction SilentlyContinue
  }

  if ($ManagedProcess.Pid) {
    Write-Host "Stopping $($ManagedProcess.Name)..." -ForegroundColor Yellow
    $processId = $ManagedProcess.Pid
    try {
      taskkill /PID $processId /T /F 2>&1 | Out-Null
    } catch {
      try {
        Stop-Process -Id $processId -Force -ErrorAction Stop
      } catch {
        # Process may already be gone.
      }
    }

    try {
      if (-not $ManagedProcess.Process.HasExited) {
        $null = $ManagedProcess.Process.WaitForExit(3000)
      }
    } catch {
      Write-Host "Timed out waiting for $($ManagedProcess.Name) to stop." -ForegroundColor Yellow
    }
  }

  if ($ManagedProcess.Process) {
    $ManagedProcess.Process.Dispose()
  }
}

Write-Host "Starting media-gallery-2 dev stack in one terminal..." -ForegroundColor Green

$backend = $null
$frontend = $null
$script:shutdownRequested = $false
$script:backendRef = $null
$script:frontendRef = $null

Register-EngineEvent PowerShell.Exiting -Action {
  if ($script:frontendRef -and $script:frontendRef.Pid) {
    try { taskkill /PID $script:frontendRef.Pid /T /F 2>&1 | Out-Null } catch {}
  }

  if ($script:backendRef -and $script:backendRef.Pid) {
    try { taskkill /PID $script:backendRef.Pid /T /F 2>&1 | Out-Null } catch {}
  }
} | Out-Null

try {
  trap [System.Management.Automation.PipelineStoppedException] {
    $script:shutdownRequested = $true
    Write-Host "`nCtrl+C interrupt detected. Shutting down app processes..." -ForegroundColor Yellow
    continue
  }

  Assert-DockerAvailable
  Ensure-DockerInfra
  Remove-StaleDevProcesses

  $sbtExecutable = Get-SbtExecutable
  $npmExecutable = Get-NpmExecutable
  $logFilePath = Get-TimestampedLogPath

  Write-Host "Logs will be exported to: $logFilePath" -ForegroundColor DarkGray

  $backend = Start-ManagedProcess `
    -Name "Backend" `
    -Prefix "Backend" `
    -Color "Cyan" `
    -WorkingDirectory $serverDir `
    -FileName $sbtExecutable `
    -Arguments "-Dsbt.supershell=false -Dsbt.log.noformat=true --batch run" `
    -LogFilePath $logFilePath
  $script:backendRef = $backend

  Wait-LocalPortListening -Port 3001 -ExpectedProcessId $backend.Pid

  $frontend = Start-ManagedProcess `
    -Name "Frontend" `
    -Prefix "Frontend" `
    -Color "Magenta" `
    -WorkingDirectory $clientDir `
    -FileName $npmExecutable `
    -Arguments "run dev" `
    -EnvironmentOverrides @{ SASS_SILENCE_DEPRECATIONS = "legacy-js-api" } `
    -LogFilePath $logFilePath
  $script:frontendRef = $frontend

  Write-Host "Press Ctrl+C to stop backend and frontend. Docker stays running." -ForegroundColor Yellow
  Write-Host "Backend PID: $($backend.Pid) | Frontend PID: $($frontend.Pid)" -ForegroundColor DarkGray
  Write-Host "Frontend URL: http://localhost:5173" -ForegroundColor Green

  while (-not $script:shutdownRequested) {
    if (-not (Test-ProcessRunning -ProcessId $backend.Pid)) {
      $exitCode = if ($backend.Process.HasExited) { $backend.Process.ExitCode } else { 'unknown' }
      Write-Host "Backend exited with code $exitCode." -ForegroundColor Red
      break
    }

    if (-not (Test-ProcessRunning -ProcessId $frontend.Pid)) {
      $exitCode = if ($frontend.Process.HasExited) { $frontend.Process.ExitCode } else { 'unknown' }
      Write-Host "Frontend exited with code $exitCode." -ForegroundColor Red
      break
    }

    Start-Sleep -Milliseconds 250
  }
} finally {
  Stop-ManagedProcess -ManagedProcess $frontend
  Stop-ManagedProcess -ManagedProcess $backend
  $script:frontendRef = $null
  $script:backendRef = $null
  Write-Host "App processes stopped. Docker containers were left running." -ForegroundColor Green
}
