"""Custom exception types used by the IE core runtime."""

class SchemaValidationError(ValueError):
    """Raised when configuration parameters do not match a JSON schema."""


class TaskValidationError(RuntimeError):
    """Raised when a task reports invalid inputs or outputs."""


class RegistryError(LookupError):
    """Raised when a task capability cannot be resolved."""


class PipelineError(RuntimeError):
    """Raised when the pipeline configuration is invalid."""
