# KeyDirCollector (Portable Windows Tool)

## Download & Run (no install)
1. Click **Releases** on the right
2. Download the latest `KeyDirCollector-*-windows.zip`
3. Extract the zip
4. Double-click `Run_KeyDirCollector.bat` (or `KeyDirCollector.exe`)

## What it does
- Scans a top-level folder for subfolders whose name starts with a key prefix (default: `700`)
- Finds target files (default extension: `pdf`)
- Optional filename filter (wildcards / OR list / regex option)
- Output modes: Copy / Move / Shortcut (.lnk)
- Output naming: `<KeyDir> ;; <OriginalFileName>` (original name preserved)

## Notes
- Shortcut mode uses Windows PowerShell internally to create `.lnk` files.
- No admin privileges required.
