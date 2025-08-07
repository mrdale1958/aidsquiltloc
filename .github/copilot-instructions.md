<!-- Use this file to provide workspace-specific custom instructions to Copilot. For more details, visit https://code.visualstudio.com/docs/copilot/copilot-customization#_use-a-githubcopilotinstructionsmd-file -->

# AIDS Memorial Quilt Records Scraper - Copilot Instructions

This project scrapes images and metadata from the Library of Congress AIDS Memorial Quilt Records collection using their public APIs.

## Project Context

- **Domain**: Digital humanities, archival research, web scraping
- **APIs**: Library of Congress JSON API (https://www.loc.gov/apis/)
- **Collection**: AIDS Memorial Quilt Records (https://www.loc.gov/collections/aids-memorial-quilt-records/)
- **Purpose**: Educational and research use for digital preservation and analysis

## Code Style and Standards

- Follow PEP 8 Python style guidelines
- Use type hints for function parameters and return values
- Implement comprehensive error handling with specific exception types
- Use async/await patterns for I/O operations
- Include detailed docstrings for all classes and functions
- Use structured logging with appropriate log levels

## Architecture Patterns

- **Separation of concerns**: Distinct modules for API client, image downloading, and metadata extraction
- **Configuration management**: Centralized settings with environment variable support
- **Asynchronous processing**: Non-blocking I/O for API calls and file operations
- **Rate limiting**: Respectful API usage with configurable delays
- **Error resilience**: Graceful handling of network, file system, and data validation errors

## Key Libraries and Frameworks

- `aiohttp`: For asynchronous HTTP requests to LOC APIs
- `aiofiles`: For non-blocking file I/O operations
- `Pillow`: For image validation and processing
- `python-dotenv`: For environment variable management

## Naming Conventions

- Use descriptive variable and function names
- Class names in PascalCase (e.g., `LOCAPIClient`)
- Function and variable names in snake_case (e.g., `get_collection_items`)
- Constants in UPPER_SNAKE_CASE (e.g., `MAX_CONCURRENT_DOWNLOADS`)
- File names in snake_case (e.g., `loc_api_client.py`)

## Error Handling Guidelines

- Use specific exception types rather than broad `Exception` catching
- Log errors with appropriate context (URLs, item IDs, file paths)
- Implement retry logic for transient network errors
- Validate data before processing (image formats, JSON structure)
- Provide meaningful error messages for debugging

## Documentation Standards

- Include comprehensive docstrings for all public methods
- Document API endpoints and parameters
- Provide usage examples in README
- Comment complex logic and algorithmic decisions
- Maintain up-to-date type annotations

## Testing Considerations

- Mock external API calls for unit tests
- Test error conditions and edge cases
- Validate file I/O operations
- Test concurrent download scenarios
- Include integration tests for end-to-end workflows

## Security and Ethics

- Respect rate limits and API terms of service
- Include proper attribution to Library of Congress
- Handle sensitive metadata appropriately
- Implement safe file handling practices
- Follow ethical web scraping guidelines

## Performance Optimization

- Use concurrent downloads with semaphore limiting
- Implement efficient pagination for large collections
- Cache API responses when appropriate
- Optimize file I/O with asynchronous operations
- Monitor memory usage for large image downloads
