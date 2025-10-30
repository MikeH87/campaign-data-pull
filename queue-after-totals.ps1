param(
  [Parameter(Mandatory=$true)][string]$From,
  [Parameter(Mandatory=$true)][string]$To
)

Write-Host "Watching for recalc-totals-simple.js to finish ..." -ForegroundColor Cyan

function IsTotalsRunning {
  try {
    # Use CIM to read full command lines for node.exe
    $procs = Get-CimInstance Win32_Process -Filter "Name='node.exe'"
    foreach ($p in $procs) {
      if ($p.CommandLine -match "recalc-totals-simple\.js") {
        return $true
      }
    }
    return $false
  } catch {
    # Fallback: assume still running if we canâ€™t check
    return $true
  }
}

# Require a period of sustained idle (no process) to avoid races
$idleRequiredSeconds = 20
$idleSoFar = 0
$pollMs = 2000

while ($true) {
  if (IsTotalsRunning) {
    $idleSoFar = 0
    Start-Sleep -Milliseconds $pollMs
    continue
  } else {
    $idleSoFar += $pollMs/1000
    if ($idleSoFar -ge $idleRequiredSeconds) {
      break
    }
    Start-Sleep -Milliseconds $pollMs
  }
}

Write-Host "âœ… Totals backfill finished. Starting spend backfill range..." -ForegroundColor Green

# IMPORTANT: use 2022-10-28 as earliest valid start (18th caused Bing 'InvalidCustomDateRangeEnd')
$normalizedFrom = $From
if ([DateTime]::Parse($From) -lt [DateTime]::Parse("2022-10-28")) {
  Write-Host "âš ï¸ Adjusting From from $From to 2022-10-28 (earliest valid date detected)" -ForegroundColor Yellow
  $normalizedFrom = "2022-10-28"
}

# Run the spend backfill (daily)
node .\backfill-range.js --from=$normalizedFrom --to=$To
