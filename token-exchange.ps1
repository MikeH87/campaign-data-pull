# token-exchange.ps1  (public client: NO client_secret)

# ======= EDIT THESE 2 LINES ONLY (keep quotes) =======
$clientId = 'a3e0076b-f932-49e1-b5c9-24287749f518'
$authCode = '1.AXkAneY-McuP9USJEB1lpIe332sH4KMy-eFJtckkKHdJ9RgMAap5AA.AgABBAIAAABlMNzVhAPUTrARzfQjWPtKAwDs_wUA9P-gqZ5HkV66cN5q5i1BKKsysQK0pv3KYu7w4n6HS3sbXY6Yc1oMOUb6yJwlbp2cIx5lsDXDnprdLnmrc2GvZICYQMntz9NhzMRy5b68iRxFc7TUigqITMhenRPSqFohCndmIEupAny6smqwKz58feWJaG_BAyfkcsxau5szEFokyCyisF0j8Kdiwhkp8JJczJQYdBTqMKiDkyi8Dks34BYNFo4NYFTxDou0TZ7K28CqOLGl5TNoDepQjITRrzCeOKAE-nfhuDNelINPOnHVXKZ4Fgls6tHtc1cMq3hpMQCtdGbuVaL6M5NvUKeyHG0M3FaXEYonDFlSre2hyQkjd3U25A1C6ERsp0DZHWJ5HZ4IH0Y07Fp6HU7IgEv0yaqlWfALzyZz6yIaZCf-Pib65f6I_7zan7mLhYuQ4s1MOItxWB2cEaWsjTiS8NSBsx21BRQZOmlDHa3zTYf1r6DwunM5hWUNiMhLGItUmOX_WyQL3x_FWalrGCKBHrGEuyI-CmS9odxO4Q7003jDZa1Ez9qYhBjtVXCoWCwPOnsDrQ_Fsd_I77CZexT6z2yL6jnODAXlHiVGfd2v36Sbb9u8cxEJeCU5CL6w2qe26MFHsu8SjevuzGCbbaobOjmxrqV_GkGLNJ-kgRU5SogSv7dAE1LBGNlZ4cHSBwoSaYr2WSB-FN7KRYh0JeJ7mryc3i96at1jmFGH7tX_5O5LAn3afhDbKdnSwbJxLY9ovMs6QAAmlwwfIkiMy-ozLzKSlGXeou1bQn24dfy0X2F8_gM-2sPWsG3ckPxg2xCPs-N2yO3JHennLIocdmfkVYIFgBIM63ZFH6AoUd2-rZuZly5ovvW8a3E7hVkPUu7ZOZT88NFVYkd0TEC1lqCEUa4ElH_JpuryckqX2_jbeuBi4QIl0VhWowoLAXatHEvKUgcyWxPutmtyq1lulsPD-IaKdL-EkmNVhcFXVFTg8ltjYA'  

# =====================================================

# Fixed values
$redirectUri = 'https://login.microsoftonline.com/common/oauth2/nativeclient'
$tokenUrl    = 'https://login.microsoftonline.com/common/oauth2/v2.0/token'
$scope       = 'https://ads.microsoft.com/msads.manage offline_access'

# Basic validation
if (-not $clientId -or $clientId -eq 'PASTE-YOUR-APPLICATION-CLIENT-ID-HERE') { Write-Error 'clientId not set'; exit 1 }
if (-not $authCode -or $authCode -eq 'PASTE-THE-FRESH-AUTH-CODE-HERE') { Write-Error 'authCode not set'; exit 1 }

# IMPORTANT: Public client must NOT send client_secret
$body = @{
  client_id    = $clientId
  grant_type   = 'authorization_code'
  code         = $authCode
  redirect_uri = $redirectUri
  scope        = $scope
}

try {
  $response = Invoke-RestMethod -Method Post -Uri $tokenUrl -Body $body -ContentType 'application/x-www-form-urlencoded'

  Write-Host ''
  Write-Host '✅ Token exchange successful!' -ForegroundColor Green
  if ($response.access_token) {
    $short = $response.access_token.Substring(0,60)
    Write-Host ('Access token (short-lived): {0}...' -f $short)
  } else {
    Write-Host 'Access token not present in response.'
  }

  if ($response.refresh_token) {
    Write-Host ''
    Write-Host 'Copy this refresh token into your .env as MSADS_REFRESH_TOKEN:' -ForegroundColor Yellow
    Write-Host '--------------------------------------------------------------------------------'
    Write-Host $response.refresh_token
    Write-Host '--------------------------------------------------------------------------------'
  } else {
    Write-Host 'No refresh_token present. Ensure scope includes offline_access.'
  }

} catch {
  Write-Host '❌ Token exchange failed' -ForegroundColor Red
  if ($_.Exception.Response) {
    $status = $_.Exception.Response.StatusCode.value__
    Write-Host ("Status: {0}" -f $status)
  }
  $errBody = ($_ | Select-Object -ExpandProperty ErrorDetails).Message
  if ($errBody) { Write-Host "Body: $errBody" } else { Write-Host 'No error body' }
  exit 2
}
