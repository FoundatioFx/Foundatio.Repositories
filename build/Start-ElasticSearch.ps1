Param(
  [string]$Version = "1.7.5",
  [int]$NodeCount = 1,
  [int]$StartPort = 9200,
  [bool]$InstallPlugins = $true,
  [bool]$OpenSense = $true,
  [bool]$ResetData = $false
)

If ($env:JAVA_HOME -eq $null -Or -Not (Test-Path -Path $env:JAVA_HOME)) {
    Write-Error "Please ensure the latest version of java is installed and the JAVA_HOME environmental variable has been set."
    Return
}

Push-Location $PSScriptRoot

If (-Not (Test-Path -Path "elasticsearch-$Version") -And -Not (Test-Path -Path "elasticsearch-$Version.zip")) {
    Write-Output "Downloading Elasticsearch $Version..."
    Invoke-WebRequest "https://download.elastic.co/elasticsearch/elasticsearch/elasticsearch-$Version.zip" -OutFile "elasticsearch-$Version.zip"
} Else {
    Write-Output "Using already downloaded Elasticsearch $Version..."
}

If ((Test-Path -Path "elasticsearch-$Version.zip") -And -Not (Test-Path -Path "elasticsearch-$Version")) {
    Write-Output "Extracting Elasticsearch $Version..."
    Add-Type -assembly "system.io.compression.filesystem"
    [io.compression.zipfile]::ExtractToDirectory("$PSScriptRoot\elasticsearch-$Version.zip", $PSScriptRoot)
    Remove-Item elasticsearch-$Version.zip
} Else {
    Write-Output "Using already downloaded and extracted Elasticsearch $Version..."
}

For ($i = 1; $i -le $NodeCount; $i++) {
    $nodePort = $StartPort + $i - 1
	Write-Output "Starting Elasticsearch $Version node $i port $nodePort"
	If (-Not (Test-Path -Path ".\elasticsearch-$Version-node$i")) {
		Copy-Item .\elasticsearch-$Version .\elasticsearch-$Version-node$i -Recurse
        Copy-Item .\elasticsearch.yml .\elasticsearch-$Version-node$i\config -Force
        Add-Content .\elasticsearch-$Version-node$i\config\elasticsearch.yml "`nhttp.port: $nodePort"

        If ($InstallPlugins) {
            Push-Location .\elasticsearch-$Version-node$i
            bin/plugin install elasticsearch/marvel/latest
            bin/plugin install elasticsearch/elasticsearch-cloud-aws/2.7.1
            Pop-Location
        }
	}

    If ($ResetData -And (Test-Path -Path "$(Get-Location)\elasticsearch-$Version-node$i\data")) {
		Write-Output "Resetting node $i data..."
        Remove-Item "$(Get-Location)\elasticsearch-$Version-node$i\data" -Recurse -ErrorAction Ignore
    }

	Start-Process "$(Get-Location)\elasticsearch-$Version-node$i\bin\elasticsearch.bat"

    $retries = 0
    Do {
        Write-Host "Waiting for Elasticsearch $Version node $i to respond..."
        $res = $null
        
        Try {
            $res = Invoke-WebRequest http://localhost:$nodePort -UseBasicParsing
        } Catch {
            $retries = $retries + 1
            Start-Sleep -s 1
        }
    } Until ($res -ne $null -And $res.StatusCode -eq 200 -And $retries -lt 10)
}

If ($OpenSense) {
    Start-Process "http://localhost:9200/_plugin/marvel/sense/index.html"
}

Pop-Location
