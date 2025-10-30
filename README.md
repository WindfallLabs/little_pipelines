# Little Pipelines - A Lightweight Task Pipeline Framework

Little Pipelines is a Python library for building and executing data pipelines with intelligent caching, dependency management, and execution tracking. It provides a simple, Luigi-inspired approach to orchestrating tasks while maintaining minimal dependencies and complexity.

## Highlightable Features

### 🔄 **Smart Caching & Checkpointing**
- Automatic result caching using `diskcache` for efficient pipeline resumption
- Intelligent cache invalidation based on:
  - Script changes (hashes the entire module file where tasks are defined)
  - Input file modifications (hashes input files when specified)
- Flexible expiration policies (session-based, time-based, or permanent)

### 📊 **Dependency Management**
- Declarative task dependencies with automatic topological sorting
- Tasks execute in the correct order based on their dependency graph
- Built-in validation to ensure all dependencies are satisfied

### ⚡ **Execution Control**
- **Selective execution**: Skip specific tasks or force re-execution of others
- **Force mode**: Clear all cached results and run everything fresh
- **Automatic skip**: Reuses cached results when scripts and inputs haven't changed
- Performance timing for each task's execution

### 🎯 **Flexible Expiry Strategies**
Rich set of cache expiration options:
- `session()` - Expires when Python exits
- `never()` - Permanent cache
- `at_midnight()` - Expires at next midnight
- `from_now()` / `from_today()` - Time-delta based expiration
- `at_datetime()` - Expire at specific date/time

### 📝 **Integrated Logging**
- Per-task loggers using `loguru`
- Performance metrics tracking (`PERF` level)
- Configurable log levels and output formatting

### 🖥️ **Interactive Shell** (Optional)
- Built-in CLI shell for pipeline inspection and execution
- Commands for task listing, execution, validation, and log level control
- Powered by Rich for beautiful terminal output

### 🏗️ **Clean Architecture**
- Task-based abstraction with `@process` decorator for custom functions
- Minimal external dependencies (diskcache, loguru, rich)
- Type hints throughout for better IDE support

This library is ideal for data scientists and engineers who need straightforward pipeline orchestration without the overhead of enterprise workflow tools, while still getting essential features like caching, dependency resolution, and execution tracking.
