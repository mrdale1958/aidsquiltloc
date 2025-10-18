#!/bin/bash

cd /Users/dalemacdonald/Library/Mobile\ Documents/com~apple~CloudDocs/LOCData/artifacts
dest_root="/Volumes/Extreme SSD/LOCData/artifacts"
src_root="/Users/dalemacdonald/Library/Mobile Documents/com~apple~CloudDocs/LOCData/artifacts"
cd $dest_root
empty_dirs=()

for dir in block_*; do
  basename=$(basename "$dir")
  dest="$dest_root/$basename"
  src="$src_root/$basename"
  if [ ! -e "$src" ]; then
    echo "$src does not exist, skipping"
    continue
  fi
    if [ ! -e "$src" ]; then
    echo "$src does not exist, skipping"
    continue
  fi

  echo "Checking for missing or zero-length files in $dest"
  missing_files=()
  if [ -d "$dest" ]; then
    while IFS= read -r relpath; do
      srcfile="$src/$relpath"
      destfile="$dest/$relpath"
      if [ ! -e "$destfile" ] || [ ! -s "$destfile" ]; then
        missing_files+=("$relpath")
      fi
    done < <(cd "$src" && find . -type f -print | sed 's|^\./||')
  else
    echo "$dest does not exist, will copy entire block"
    missing_files=()
    while IFS= read -r relpath; do
      missing_files+=("$relpath")
    done < <(cd "$src" && find . -type f -print | sed 's|^\./||')
  fi

  if [ "${#missing_files[@]}" -gt 0 ]; then
    echo "Forcing iCloud cache for missing files in $dir"
    for relpath in "${missing_files[@]}"; do
      srcfile="$src/$relpath"
      if [ -e "$srcfile" ]; then
        echo "$srcfile"
        cat "$srcfile" > /dev/null
      fi
    done
    echo "Sleeping to allow iCloud to download files..."
    sleep 60
    echo "Rsyncing $dir to $dest"
    timeout 300 /opt/homebrew/bin/rsync -aX --no-perms --no-owner --no-group "$dir" "/Volumes/Extreme SSD/LOCData/artifacts/"
    # Recheck for missing files after rsync
    echo "Rechecking $dest for missing or zero-length files"
    re_missing=()
    while IFS= read -r relpath; do
      srcfile="$src/$relpath"
      destfile="$dest/$relpath"
      if [ ! -e "$destfile" ] || [ ! -s "$destfile" ]; then
        re_missing+=("$relpath")
      fi
    done < <(cd "$src" && find . -type f -print | sed 's|^\./||')
    if [ "${#re_missing[@]}" -gt 0 ]; then
      echo "Retrying iCloud cache and rsync for remaining missing files in $dir"
      for relpath in "${re_missing[@]}"; do
        srcfile="$src/$relpath"
        if [ -e "$srcfile" ]; then
          echo "$srcfile"
          cat "$srcfile" > /dev/null
        fi
      done
      sleep 60
      timeout 300 /opt/homebrew/bin/rsync -aX --no-perms --no-owner --no-group "$dir" "/Volumes/Extreme SSD/LOCData/artifacts/"
    fi
  else
    echo "No missing files in $dest for $dir, skipping iCloud cache and rsync."
  fi


  # Accumulate empty directories from source
  while IFS= read -r src_empty_dir; do
    empty_dirs+=("$src_empty_dir")
  done < <(find "$dir" -type d -empty)
done

# Report all empty directories found in source
if [ "${#empty_dirs[@]}" -gt 0 ]; then
  echo "Empty directories found in source:"
  for ed in "${empty_dirs[@]}"; do
    echo "$ed"
  done
else
  echo "No empty directories found in source."
fi