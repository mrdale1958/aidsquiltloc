cd /Users/dalemacdonald/Library/Mobile\ Documents/com~apple~CloudDocs/LOCData/artifacts
for dir in block_*; do
  basename=$(basename "$dir")
  dest="/Volumes/Extreme SSD/LOCData/artifacts/$basename"
  if [ ! -e "$dest" ]; then
    echo "Processing $dir"
    find "$dir" -type f -exec sh -c 'echo "$1"; cat "$1" > /dev/null' _ {} \;
    echo "Copying $dir to /Volumes/Extreme SSD/LOCData/artifacts/"
    sleep 60
    timeout 300 /opt/homebrew/bin/rsync -aX --no-perms --no-owner --no-group "$dir" "/Volumes/Extreme SSD/LOCData/artifacts/"
    echo "verifying $destdir"
    find "/Volumes/Extreme SSD/LOCData/artifacts/$dir" -type d | while read -r destdir; do
      # Check if the directory has no children
      if [ "$(find "$destdir" -mindepth 1 | wc -l)" -eq 0 ]; then
        echo "Leaf node directory found: $destdir"
        find "$dir" -type f -exec touch {} +
        sleep 60
        timeout 300 /opt/homebrew/bin/rsync -aX --no-perms --no-owner --no-group "$dir" "/Volumes/Extreme SSD/LOCData/artifacts/"
        break
      fi
    done
  else
    echo "$dest already exists, skipping"
  fi
done