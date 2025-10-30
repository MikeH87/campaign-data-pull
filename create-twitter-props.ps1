param(
  [string]$Token = $env:HUBSPOT_PRIVATE_APP_TOKEN,
  [string]$GroupName = "twitter_campaign_data",
  [string]$GroupLabel = "Twitter Campaign Data"
)
if (-not $Token) { throw "Set HUBSPOT_PRIVATE_APP_TOKEN in your environment first." }

$base = "https://api.hubapi.com"
$h = @{ "Authorization" = "Bearer $Token"; "Content-Type" = "application/json" }

# 1) Ensure group
$groupBody = @{
  name = $GroupName
  label = $GroupLabel
  objectType = "0-35" # Campaign object
  displayOrder = -1
}.ConvertTo-Json
$grp = Invoke-RestMethod -Method Post -Uri "$base/crm/v3/properties/campaign/groups" -Headers $h -Body $groupBody -ErrorAction SilentlyContinue
if ($grp.name) { Write-Host "Group ensured: $($grp.name)" } else { Write-Host "Group may already exist (that's ok)." }

# helper to create or move a property to the group
function Ensure-Prop($name, $label, $type, $fieldType) {
  $body = @{
    name = $name
    label = $label
    type = $type
    fieldType = $fieldType
    groupName = $GroupName
  } | ConvertTo-Json
  $r = Invoke-RestMethod -Method Post -Uri "$base/crm/v3/properties/campaign" -Headers $h -Body $body -ErrorAction SilentlyContinue
  if (-not $r.name) {
    # try update group if already exists
    $patch = @{ groupName = $GroupName } | ConvertTo-Json
    $r2 = Invoke-RestMethod -Method Patch -Uri "$base/crm/v3/properties/campaign/$name" -Headers $h -Body $patch -ErrorAction SilentlyContinue
    if ($r2.name) { Write-Host "Property moved: $name -> $GroupLabel" } else { Write-Host "Property ensured/moved: $name (OK if it already existed)" }
  } else {
    Write-Host "Property created: $($r.name)"
  }
}

Ensure-Prop -name "twitter_last_status" -label "Twitter Last Status" -type "string" -fieldType "text"
Ensure-Prop -name "twitter_last_processed" -label "Twitter Last Processed" -type "date" -fieldType "date"
