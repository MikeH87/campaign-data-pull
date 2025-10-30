param(
  [string]$Token = $env:HUBSPOT_PRIVATE_APP_TOKEN,
  [string]$Base = "https://api.hubapi.com",
  [string]$GroupName = "Twitter Campaign Data",
  [string]$GroupLabel = "Twitter Campaign Data"
)

if (-not $Token) { throw "HUBSPOT_PRIVATE_APP_TOKEN not set and -Token not provided." }

# HubSpot object type for Marketing Campaigns
$ObjectType = "marketing-campaigns"

function Invoke-HS {
  param(
    [string]$Method,
    [string]$Path,
    [object]$Body = $null,
    [int[]]$Ok = @(200,201,204)
  )
  $uri = "$Base$Path"
  $headers = @{ Authorization = "Bearer $Token"; "Content-Type"="application/json" }
  $json = $null
  if ($Body -ne $null) { $json = ($Body | ConvertTo-Json -Depth 8) }
  try {
    $resp = Invoke-WebRequest -Method $Method -Uri $uri -Headers $headers -Body $json -ErrorAction Stop
  } catch {
    if ($_.Exception.Response -and $_.Exception.Response.Content) {
      $content = [System.Text.Encoding]::UTF8.GetString($_.Exception.Response.Content)
      throw "HubSpot $Method $Path failed ($($_.Exception.Response.StatusCode)): $content"
    } else {
      throw
    }
  }
  if ($Ok -notcontains [int]$resp.StatusCode) {
    throw "HubSpot $Method $Path failed ($($resp.StatusCode)): $($resp.Content)"
  }
  if ($resp.Content) { return ($resp.Content | ConvertFrom-Json) }
  return $null
}

function Ensure-Group {
  param([string]$Name,[string]$Label)
  try {
    $g = Invoke-HS -Method GET -Path "/crm/v3/properties/$ObjectType/groups/$Name"
    Write-Host "✔ Group exists: $Name"
    return $g
  } catch {
    Write-Host "… creating group: $Name"
    $body = @{ name=$Name; label=$Label }
    return Invoke-HS -Method POST -Path "/crm/v3/properties/$ObjectType/groups" -Body $body -Ok @(201)
  }
}

function Ensure-Property {
  param([string]$Name,[hashtable]$Spec)
  $created = $false
  try {
    $existing = Invoke-HS -Method GET -Path "/crm/v3/properties/$ObjectType/$Name"
    Write-Host "✔ Property exists: $Name"
  } catch {
    Write-Host "… creating property: $Name"
    $null = Invoke-HS -Method POST -Path "/crm/v3/properties/$ObjectType" -Body $Spec -Ok @(201)
    $existing = Invoke-HS -Method GET -Path "/crm/v3/properties/$ObjectType/$Name"
    $created = $true
  }

  # If the property exists but in a different group, move it
  if ($existing.groupName -ne $Spec.groupName) {
    Write-Host "… moving $Name from group '$($existing.groupName)' to '$($Spec.groupName)'"
    $patch = @{ groupName = $Spec.groupName }
    $null = Invoke-HS -Method PATCH -Path "/crm/v3/properties/$ObjectType/$Name" -Body $patch -Ok @(200)
  }

  # If the types don’t match (rare), patch basic metadata to match requested
  if (($existing.type -ne $Spec.type) -or ($existing.fieldType -ne $Spec.fieldType)) {
    Write-Host "… patching $Name type/fieldType to $($Spec.type)/$($Spec.fieldType)"
    $patch = @{ type = $Spec.type; fieldType = $Spec.fieldType }
    $null = Invoke-HS -Method PATCH -Path "/crm/v3/properties/$ObjectType/$Name" -Body $patch -Ok @(200)
  }

  if ($created) { Write-Host "✔ Created $Name" } else { Write-Host "✔ Ensured $Name" }
}

# 1) Ensure target group
Ensure-Group -Name $GroupName -Label $GroupLabel | Out-Null

# 2) Twitter-specific properties
$props = @(
  @{
    name      = "twitter_last_status"
    label     = "Twitter Last Status"
    type      = "string"
    fieldType = "text"
    groupName = $GroupName
  },
  @{
    name      = "twitter_last_processed"
    label     = "Twitter Last Processed (epoch ms)"
    type      = "number"
    fieldType = "number"
    groupName = $GroupName
  }
)

foreach ($p in $props) {
  Ensure-Property -Name $p.name -Spec $p
}

Write-Host "`nDone. Twitter properties ensured in group '$GroupName'."
