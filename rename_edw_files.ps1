# rename_edw_files.ps1
# Normalizes all NI_/NP_ source-to-target filenames to use " - " separator.
# Run from project root:  .\rename_edw_files.ps1
# Add -WhatIf to preview without renaming:  .\rename_edw_files.ps1 -WhatIf

param(
    [switch]$WhatIf
)

$dirs = @(
    "input\Initial",
    "input\Persistent"
)

$renamed = 0
$skipped = 0

foreach ($dir in $dirs) {
    if (-not (Test-Path $dir)) {
        Write-Host "  Skipping (not found): $dir"
        continue
    }

    Write-Host "`nProcessing: $dir"

    Get-ChildItem -Path $dir -File | Where-Object {
        $_.Name -imatch "^(NI|NP)_" -and
        $_.Name -imatch "source.+to.+target"
    } | ForEach-Object {
        $file = $_
        $name = $file.Name
        $ext  = $file.Extension  # .xls or .xlsx

        # Extract prefix (NI_ENTITYNAME or NP_ENTITYNAME)
        # Strip everything from the separator before "source" onwards
        $prefix = $name -ireplace "[\s_-]+source[\s_-]*to[\s_-]*target.*$", ""

        # Build normalized name: PREFIX - source to target.EXT
        $newName = "$prefix - source to target$ext"

        if ($name -eq $newName) {
            $skipped++
            return
        }

        $newPath = Join-Path $file.DirectoryName $newName

        # Handle collision (another file already has the target name)
        if ((Test-Path $newPath) -and ($newPath -ne $file.FullName)) {
            Write-Host "  SKIP (collision): $name -> $newName"
            $skipped++
            return
        }

        if ($WhatIf) {
            Write-Host "  [PREVIEW] $name"
            Write-Host "         -> $newName"
        } else {
            Rename-Item -Path $file.FullName -NewName $newName
            Write-Host "  RENAMED: $name"
            Write-Host "       ->: $newName"
        }
        $renamed++
    }
}

Write-Host ""
if ($WhatIf) {
    Write-Host "PREVIEW complete: $renamed would be renamed, $skipped already correct"
    Write-Host "Run without -WhatIf to apply changes."
} else {
    Write-Host "Done: $renamed renamed, $skipped already correct"
}