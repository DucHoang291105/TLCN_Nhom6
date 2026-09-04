$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
$venvPython = Join-Path $projectRoot ".venv\Scripts\python.exe"
$requirementsPath = Join-Path $projectRoot "requirements.txt"

if (-not (Test-Path -LiteralPath $venvPython -PathType Leaf)) {
    python -m venv (Join-Path $projectRoot ".venv")
}

& $venvPython -m pip install -r $requirementsPath
if ($LASTEXITCODE -ne 0) {
    throw "Could not install dependencies from requirements.txt"
}

$sparkHome = & $venvPython -X utf8 -c "import pathlib, pyspark; print(pathlib.Path(pyspark.__file__).parent)"
if ($LASTEXITCODE -ne 0 -or -not $sparkHome) {
    throw "Could not locate PySpark inside .venv"
}

$jarName = "hadoop-bare-naked-local-fs-0.1.0.jar"
$jarDestination = Join-Path $sparkHome "jars\$jarName"
$downloadPath = "$jarDestination.download"
$jarUrl = "https://repo1.maven.org/maven2/com/globalmentor/hadoop-bare-naked-local-fs/0.1.0/$jarName"
$expectedSha256 = "E0CC30FB0531EB0B59468DC0ABF5B257533D2365B5E9F45E795EDD707AA78C62"

if (Test-Path -LiteralPath $jarDestination -PathType Leaf) {
    $actualSha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $jarDestination).Hash
    if ($actualSha256 -ne $expectedSha256) {
        throw "Unexpected checksum for $jarDestination"
    }
} else {
    try {
        Invoke-WebRequest -Uri $jarUrl -OutFile $downloadPath
        $actualSha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $downloadPath).Hash
        if ($actualSha256 -ne $expectedSha256) {
            throw "Downloaded dependency checksum does not match"
        }
        Move-Item -LiteralPath $downloadPath -Destination $jarDestination
    } finally {
        if (Test-Path -LiteralPath $downloadPath -PathType Leaf) {
            Remove-Item -LiteralPath $downloadPath -Force
        }
    }
}

Write-Output "PYSPARK SETUP: PASS"
Write-Output "Python: $venvPython"
Write-Output "Spark home: $sparkHome"
Write-Output "Local filesystem JAR: $jarDestination"
