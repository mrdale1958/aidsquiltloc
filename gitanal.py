#!/usr/bin/env python3
"""
Git large file analyzer for AIDS Memorial Quilt Records project
Identifies large files that Git is tracking vs. what should be ignored
"""

import subprocess
import sys
from pathlib import Path
from typing import List, Tuple, Set
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class GitLargeFileAnalyzer:
    """Analyzes large files in Git repository and working directory"""
    
    def __init__(self, repo_path: Path = Path('.')):
        self.repo_path = repo_path
        self.size_threshold_mb = 10  # Files larger than 10MB
    
    def run_git_command(self, command: List[str]) -> str:
        """Execute a git command and return output"""
        try:
            result = subprocess.run(
                ['git'] + command,
                cwd=self.repo_path,
                capture_output=True,
                text=True,
                check=True
            )
            return result.stdout.strip()
        except subprocess.CalledProcessError as e:
            logger.error("Git command failed: %s", e)
            return ""
    
    def get_tracked_large_files(self) -> List[Tuple[str, int]]:
        """Find large files that Git is currently tracking"""
        logger.info("🔍 Finding large files tracked by Git...")
        
        # Get all tracked files with their sizes
        tracked_files = []
        
        # List all files in the Git index
        files_output = self.run_git_command(['ls-files'])
        if not files_output:
            return tracked_files
        
        for file_path in files_output.split('\n'):
            if not file_path.strip():
                continue
                
            full_path = self.repo_path / file_path
            if full_path.exists():
                try:
                    size_bytes = full_path.stat().st_size
                    size_mb = size_bytes / (1024 * 1024)
                    
                    if size_mb > self.size_threshold_mb:
                        tracked_files.append((file_path, int(size_mb)))
                except OSError as e:
                    logger.warning("Cannot get size for %s: %s", file_path, e)
        
        return sorted(tracked_files, key=lambda x: x[1], reverse=True)
    
    def get_files_in_commit_range(self, commit_range: str = "HEAD") -> List[Tuple[str, int]]:
        """Find large files in specific commit range"""
        logger.info("🔍 Analyzing files in commit range: %s", commit_range)
        
        # Get files that would be pushed
        try:
            # Check what commits would be pushed
            commits_to_push = self.run_git_command([
                'rev-list', '--objects', f'{commit_range}', '--not', '--remotes'
            ])
            
            if not commits_to_push:
                logger.info("No new commits to push")
                return []
            
            large_files = []
            for line in commits_to_push.split('\n'):
                if not line.strip() or len(line.split()) < 2:
                    continue
                    
                parts = line.split(' ', 1)
                if len(parts) == 2:
                    obj_hash, file_path = parts
                    
                    # Get object size
                    try:
                        size_output = self.run_git_command([
                            'cat-file', '-s', obj_hash
                        ])
                        if size_output and size_output.isdigit():
                            size_bytes = int(size_output)
                            size_mb = size_bytes / (1024 * 1024)
                            
                            if size_mb > self.size_threshold_mb:
                                large_files.append((file_path, int(size_mb)))
                    except (ValueError, subprocess.CalledProcessError):
                        continue
            
            return sorted(large_files, key=lambda x: x[1], reverse=True)
            
        except subprocess.CalledProcessError as e:
            logger.error("Failed to analyze commit range: %s", e)
            return []
    
    def check_gitignore_coverage(self, file_path: str) -> bool:
        """Check if a file should be ignored by .gitignore"""
        try:
            # Use git check-ignore to see if file should be ignored
            result = subprocess.run(
                ['git', 'check-ignore', file_path],
                cwd=self.repo_path,
                capture_output=True,
                text=True
            )
            # Return code 0 means file is ignored
            return result.returncode == 0
        except subprocess.CalledProcessError:
            return False
    
    def analyze_repository(self) -> None:
        """Complete analysis of repository large files"""
        logger.info("🧹 AIDS Memorial Quilt Git Large File Analysis")
        
        print("\n" + "="*60)
        print("📊 LARGE FILES CURRENTLY TRACKED BY GIT")
        print("="*60)
        
        tracked_large = self.get_tracked_large_files()
        if tracked_large:
            for file_path, size_mb in tracked_large[:20]:  # Top 20
                ignored = self.check_gitignore_coverage(file_path)
                ignore_status = "🚫 IGNORED" if ignored else "⚠️  TRACKED"
                print(f"{size_mb:>6}MB  {ignore_status}  {file_path}")
        else:
            print("✅ No large files found in Git tracking")
        
        print("\n" + "="*60)
        print("📤 LARGE FILES THAT WOULD BE PUSHED")
        print("="*60)
        
        # Check files that would be pushed to origin/main
        push_files = self.get_files_in_commit_range("origin/main..HEAD")
        if push_files:
            for file_path, size_mb in push_files:
                ignored = self.check_gitignore_coverage(file_path)
                ignore_status = "🚫 IGNORED" if ignored else "⚠️  WILL PUSH"
                print(f"{size_mb:>6}MB  {ignore_status}  {file_path}")
        else:
            print("✅ No large files in commits to be pushed")
        
        print("\n" + "="*60)
        print("🔧 RECOMMENDED ACTIONS")
        print("="*60)
        
        # Find files that are tracked but should be ignored
        problematic_files = [
            (file_path, size_mb) for file_path, size_mb in tracked_large
            if self.check_gitignore_coverage(file_path)
        ]
        
        if problematic_files:
            print("📋 Files tracked by Git but should be ignored:")
            for file_path, size_mb in problematic_files[:10]:
                print(f"   • {file_path} ({size_mb}MB)")
            
            print(f"\n💡 To remove these {len(problematic_files)} files from Git:")
            print("   git rm --cached <file>")
            print("   git commit -m 'Remove large files from tracking'")
        
        # Check for node_modules specifically
        node_modules_files = [
            (fp, size) for fp, size in tracked_large 
            if 'node_modules' in fp or '.cache' in fp
        ]
        
        if node_modules_files:
            print(f"\n🎯 Found {len(node_modules_files)} node_modules/cache files in Git:")
            print("   git rm -r --cached dashboard/node_modules/")
            print("   git rm -r --cached dashboard/.cache/ 2>/dev/null || true")
            print("   git commit -m 'Remove node_modules and cache from tracking'")


def main() -> None:
    """Main execution function"""
    try:
        analyzer = GitLargeFileAnalyzer()
        analyzer.analyze_repository()
        
        print("\n" + "="*60)
        print("🏁 QUICK FIX COMMANDS")
        print("="*60)
        print("# Remove common large files from Git tracking:")
        print("git rm -r --cached dashboard/node_modules/ 2>/dev/null || true")
        print("git rm -r --cached dashboard/.cache/ 2>/dev/null || true") 
        print("git rm --cached *.log 2>/dev/null || true")
        print("git add .")
        print("git commit -m 'Remove large files from Git tracking'")
        print("git push origin main")
        
    except KeyboardInterrupt:
        logger.info("🛑 Analysis interrupted by user")
    except Exception as e:
        logger.error("💥 Analysis failed: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()