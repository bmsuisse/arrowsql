# Installs SQL Server 2022 Express on a GitHub Windows runner with SQL authentication
# and a fixed TCP port 1433, so the tests can use "Server=localhost,1433;Uid=sa".
param([Parameter(Mandatory = $true)][string]$SaPassword)
$ErrorActionPreference = "Stop"

$existing = Get-Service -Name 'MSSQL$*', 'MSSQLSERVER' -ErrorAction SilentlyContinue
if ($existing) {
    Write-Host "SQL Server already present:" ($existing | ForEach-Object { $_.Name })
} else {
    $work = Join-Path $env:RUNNER_TEMP "sqlsetup"
    New-Item -ItemType Directory -Force $work | Out-Null
    $boot = Join-Path $work "SQL2022-SSEI-Expr.exe"
    Invoke-WebRequest -Uri "https://go.microsoft.com/fwlink/p/?linkid=2216019" -OutFile $boot
    $media = Join-Path $work "media"
    New-Item -ItemType Directory -Force $media | Out-Null
    $dl = Start-Process -FilePath $boot -ArgumentList "/Action=Download", "/MediaPath=$media", "/MediaType=Core", "/Quiet" -Wait -PassThru
    Write-Host "bootstrapper exit code $($dl.ExitCode)"
    $pkg = Get-ChildItem $media -Filter "SQLEXPR*.exe" -Recurse | Select-Object -First 1
    if (-not $pkg) { throw "SQL Server Express media not downloaded" }
    $extract = Join-Path $work "extract"
    Start-Process -FilePath $pkg.FullName -ArgumentList "/q", "/x:$extract" -Wait
    $setupArgs = @(
        "/Q", "/ACTION=Install", "/IACCEPTSQLSERVERLICENSETERMS", "/FEATURES=SQLEngine",
        "/INSTANCENAME=SQLEXPRESS", "/SECURITYMODE=SQL", "/SAPWD=$SaPassword",
        "/TCPENABLED=1", "/SQLSYSADMINACCOUNTS=BUILTIN\Administrators", "/UPDATEENABLED=0"
    )
    $p = Start-Process -FilePath (Join-Path $extract "setup.exe") -ArgumentList $setupArgs -Wait -PassThru
    if ($p.ExitCode -ne 0) {
        Get-ChildItem "C:\Program Files\Microsoft SQL Server\*\Setup Bootstrap\Log\Summary.txt" -ErrorAction SilentlyContinue |
            ForEach-Object { Get-Content $_ | Select-Object -First 60 }
        throw "SQL Server setup failed with exit code $($p.ExitCode)"
    }
}

# Fixed port 1433 on every instance found, SQL auth on, then restart.
$names = (Get-ItemProperty 'HKLM:\SOFTWARE\Microsoft\Microsoft SQL Server\Instance Names\SQL').PSObject.Properties |
    Where-Object { $_.Name -notlike 'PS*' }
foreach ($n in $names) {
    $root = "HKLM:\SOFTWARE\Microsoft\Microsoft SQL Server\$($n.Value)\MSSQLServer"
    Set-ItemProperty "$root\SuperSocketNetLib\Tcp" -Name Enabled -Value 1
    Set-ItemProperty "$root\SuperSocketNetLib\Tcp\IPAll" -Name TcpPort -Value "1433"
    Set-ItemProperty "$root\SuperSocketNetLib\Tcp\IPAll" -Name TcpDynamicPorts -Value ""
    Set-ItemProperty $root -Name LoginMode -Value 2
    $svc = if ($n.Name -eq 'MSSQLSERVER') { 'MSSQLSERVER' } else { "MSSQL`$$($n.Name)" }
    Restart-Service $svc -Force
    Write-Host "configured $svc on 1433"
}
Start-Sleep 5
netstat -ano | Select-String ":1433"
