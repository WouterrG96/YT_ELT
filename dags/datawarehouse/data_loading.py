import json
from datetime import date
import logging

# Create a module-level logger (configured elsewhere in your app, typically in main entrypoint)
logger = logging.getLogger(__name__)


def load_data():
    # Build the expected input filename using *today's* date (e.g., ./data/YT_data_2025-12-01.json)
    file_path = f"./data/YT_data_{date.today()}.json"

    try:
        # Log what we're about to process to help with debugging and auditability
        logger.info(f"Processing file: YT_data_{date.today()}")

        # Open the JSON file safely (context manager ensures file handle is always closed)
        with open(file_path, "r", encoding="utf-8") as raw_data:
            # Parse JSON content into Python objects (dict/list)
            data = json.load(raw_data)

        # Return the parsed data to the caller
        return data

    except FileNotFoundError:
        # File doesn't exist at the computed path (common if date mismatch or data not generated yet)
        logger.error(f"File not found:{file_path}")
        # Re-raise so calling code can decide how to handle (fail fast vs. fallback behavior)
        raise

    except json.JSONDecodeError:
        # File exists but contains invalid JSON (partial write, corruption, wrong file contents, etc.)
        logger.error(f"Invalid JSON in file: {file_path}")
        # Re-raise to avoid silently continuing with bad/undefined data
        raise
