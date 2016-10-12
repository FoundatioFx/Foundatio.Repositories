$es_version = "5.0.0-rc1"
If ($env:ES_VERSION) {
    $es_version = $env:ES_VERSION
}

If ($env:JAVA_HOME -eq $null -or !(Test-Path -Path $env:JAVA_HOME)) {
    Write-Error "Please ensure the latest version of java is installed and the JAVA_HOME environmental variable has been set."
    Return
}

Push-Location $PSScriptRoot

If (!(Test-Path -Path "elasticsearch-$es_version") -And !(Test-Path -Path "elasticsearch.zip")) {
    Invoke-WebRequest "https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-$es_version.zip" -OutFile "elasticsearch-$es_version.zip"
}

If ((Test-Path -Path "elasticsearch-$es_version.zip") -And !(Test-Path -Path "elasticsearch-$es_version")) {
    Add-Type -assembly "system.io.compression.filesystem"
    [io.compression.zipfile]::ExtractToDirectory("$PSScriptRoot\elasticsearch-$es_version.zip", $PSScriptRoot)
    cp .\elasticsearch.yml .\elasticsearch-$es_version\config -Force
    rm elasticsearch-$es_version.zip

    cp .\elasticsearch-$es_version .\elasticsearch-node1 -Recurse
    cp .\elasticsearch-$es_version .\elasticsearch-node2 -Recurse
    cp .\elasticsearch-$es_version .\elasticsearch-node3 -Recurse
}

Write-Output "Starting node 1..."
Start-Process "$(Get-Location)\elasticsearch-node1\bin\elasticsearch.bat" -RedirectStandardError "$(Get-Location)\elasticsearch-node1\error.txt" -RedirectStandardOutput "$(Get-Location)\elasticsearch-node1\log.txt"
Write-Output "Starting node 2..."
Start-Process "$(Get-Location)\elasticsearch-node2\bin\elasticsearch.bat" -RedirectStandardError "$(Get-Location)\elasticsearch-node2\error.txt" -RedirectStandardOutput "$(Get-Location)\elasticsearch-node2\log.txt"
Write-Output "Starting node 3..."
Start-Process "$(Get-Location)\elasticsearch-node3\bin\elasticsearch.bat" -RedirectStandardError "$(Get-Location)\elasticsearch-node3\error.txt" -RedirectStandardOutput "$(Get-Location)\elasticsearch-node3\log.txt"

Pop-Location
