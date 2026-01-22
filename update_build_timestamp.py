"""Update build timestamp in _version.py before building."""

from datetime import datetime
from pathlib import Path

VERSION_FILE = Path(__file__).parent / "cognite" / "databricks" / "_version.py"


def update_build_timestamp() -> None:
    """Update __build_timestamp__ in _version.py with current datetime."""
    content = VERSION_FILE.read_text()
    timestamp = datetime.now().isoformat()

    # Simple replacement: find the line and replace it
    lines = content.splitlines()
    new_lines = []
    found = False
    for line in lines:
        if line.strip().startswith("__build_timestamp__"):
            new_lines.append(f'__build_timestamp__ = "{timestamp}"  # Set during build')
            found = True
        else:
            new_lines.append(line)

    # If not found, add it after __version__
    if not found:
        new_lines = []
        for line in lines:
            new_lines.append(line)
            if line.strip().startswith("__version__"):
                new_lines.append(f'__build_timestamp__ = "{timestamp}"  # Set during build')

    VERSION_FILE.write_text("\n".join(new_lines) + "\n")
    print(f"Updated build timestamp to: {timestamp}")


if __name__ == "__main__":
    update_build_timestamp()
