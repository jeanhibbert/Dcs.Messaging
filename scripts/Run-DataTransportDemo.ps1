<#
.SYNOPSIS
Runs the Dcs.DataTransport demo server and client over TCP and gRPC in three scenarios.

.DESCRIPTION
Scenarios, each run for -Duration seconds on this machine (loopback):
  all-fast   3 fast subscribers at a fixed publish rate (the baseline).
  one-slow   2 fast + 1 slow subscriber at the same rate: do the fast subscribers notice the slow one?
  max-rate   2 fast + 1 slow subscriber with the publisher unthrottled: maximum delivered throughput.
A slow subscriber spends -SlowWorkUs microseconds of CPU per message, so it can handle about 1e6 / SlowWorkUs msg/s.
#>
param(
    [int]$Duration = 10,
    [int]$Rate = 200000,
    [int]$Queue = 8192,
    [int]$SlowWorkUs = 20
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
foreach ($project in 'Dcs.DataTransport.DemoServer', 'Dcs.DataTransport.DemoClient') {
    dotnet build (Join-Path $root $project) -c Release --nologo -v q | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "Build of $project failed." }
}

$serverExe = Join-Path $root 'Dcs.DataTransport.DemoServer\bin\Release\net10.0\Dcs.DataTransport.DemoServer.exe'
$clientExe = Join-Path $root 'Dcs.DataTransport.DemoClient\bin\Release\net10.0\Dcs.DataTransport.DemoClient.exe'
$serverLog = Join-Path ([IO.Path]::GetTempPath()) 'dcs-datatransport-server.txt'

$scenarios = @(
    @{ Name = 'all-fast'; Rate = $Rate; Fast = 3; Slow = 0 },
    @{ Name = 'one-slow'; Rate = $Rate; Fast = 2; Slow = 1 },
    @{ Name = 'max-rate'; Rate = 0; Fast = 2; Slow = 1 }
)

$port = 5200
foreach ($transport in 'Tcp', 'Grpc') {
    foreach ($scenario in $scenarios) {
        $port++
        Write-Host "`n=== $transport / $($scenario.Name) ===" -ForegroundColor Cyan

        $server = Start-Process -FilePath $serverExe -NoNewWindow -PassThru -RedirectStandardOutput $serverLog -ArgumentList @(
            '--transport', $transport, '--port', $port, '--rate', $scenario.Rate, '--duration', $Duration,
            '--subscriberQueueCapacity', $Queue, '--subscribers', ($scenario.Fast + $scenario.Slow))

        while (-not ((Test-Path $serverLog) -and (Select-String -Path $serverLog -Pattern '^Listening' -Quiet))) {
            if ($server.HasExited) { throw 'Server exited before it started listening.' }
            Start-Sleep -Milliseconds 100
        }

        & $clientExe --transport $transport --port $port --fast $scenario.Fast --slow $scenario.Slow --slowWorkUs $SlowWorkUs
        if (-not $server.WaitForExit(30000)) { $server.Kill(); throw 'Server did not exit.' }

        # The server's results start at its blank separator line.
        (Get-Content $serverLog -Raw) -split "\r?\n\r?\n", 2 | Select-Object -Last 1 | Write-Host
    }
}
