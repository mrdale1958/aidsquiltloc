"""
Patch script to rename all occurrences of 'artifact' to 'artifact' across the AIDS Memorial Quilt project.
Follows project coding standards for digital humanities research.
- Replaces in code, comments, docs, database schema, and API endpoints.
- Handles singular/plural and case variations.
- Use with version control!
"""

import os
import re

# File extensions to process
EXTENSIONS = ('.py', '.ts', '.tsx', '.md', '.sql', '.json', '.env')

# Replacement patterns (order matters: plural before singular, uppercase before lowercase)
REPLACEMENTS = [
    (r'\bArtifacts\b', 'Artifacts'),
    (r'\bArtifact\b', 'Artifact'),
    (r'\bartifacts\b', 'artifacts'),
    (r'\bartifact\b', 'artifact'),
    (r'Artifacts', 'Artifacts'),
    (r'Artifact', 'Artifact'),
    (r'artifacts', 'artifacts'),
    (r'artifact', 'artifact'),
]

def replace_in_file(filepath: str):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    original_content = content
    for pattern, repl in REPLACEMENTS:
        content = re.sub(pattern, repl, content)
    if content != original_content:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"Patched: {filepath}")

def walk_and_patch(root_dir: str):
    for dirpath, _, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.endswith(EXTENSIONS):
                filepath = os.path.join(dirpath, filename)
                replace_in_file(filepath)

if __name__ == "__main__":
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    print(f"Starting global 'artifact' → 'artifact' refactor in: {PROJECT_ROOT}")
    walk_and_patch(PROJECT_ROOT)
    print("Refactor complete. Please review changes with git diff before committing.")