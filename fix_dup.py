#!/usr/bin/env python3
"""Fix duplicate ResourceAlreadyExists exception"""

file_path = "cognite/databricks/udtf_registry.py"

with open(file_path, "r", encoding="utf-8") as f:
    content = f.read()

# Fix duplicate ResourceAlreadyExists
content = content.replace(
    "except (RuntimeError, ValueError, ResourceAlreadyExists) as e:",
    "except (RuntimeError, ValueError) as e:"
)

with open(file_path, "w", encoding="utf-8") as f:
    f.write(content)

print("Fixed duplicate exception")
