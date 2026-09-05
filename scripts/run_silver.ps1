param(
    [ValidatePattern("^[A-Za-z]$")]
    [string]$DriveLetter = "T",
    [switch]$SmokeTest
)

$ErrorActionPreference = "Stop"

$projectRoot = (Resolve-Path -LiteralPath (Split-Path -Parent $PSScriptRoot)).Path
$driveName = $DriveLetter.ToUpperInvariant()
$drivePrefix = "${driveName}:"

if (Get-PSDrive -Name $driveName -ErrorAction SilentlyContinue) {
    throw "Drive $drivePrefix is already in use. Pass another -DriveLetter."
}

& subst.exe $drivePrefix $projectRoot
if ($LASTEXITCODE -ne 0) {
    throw "Could not create project alias $drivePrefix"
}

$pipelineExitCode = 1

try {
    $mappedRoot = "${drivePrefix}\"
    $venvPython = Join-Path $mappedRoot ".venv\Scripts\python.exe"
    $pipelineScript = Join-Path $mappedRoot "src\silver\01_build_silver_core.py"

    if (-not (Test-Path -LiteralPath $venvPython -PathType Leaf)) {
        throw "Missing .venv. Run scripts/setup_spark_windows.ps1 first."
    }

    $arguments = @("-X", "utf8", $pipelineScript)
    if ($SmokeTest) {
        $arguments += "--smoke-test"
    }

    & $venvPython @arguments
    $pipelineExitCode = $LASTEXITCODE
} finally {
    & subst.exe $drivePrefix /D
}

if ($pipelineExitCode -ne 0) {
    throw "Silver command failed with exit code $pipelineExitCode"
}
