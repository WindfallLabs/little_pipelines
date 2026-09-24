"""
Data - Define and document data.

A Data object represents a conceptual dataset and definition.
It's optional, but highly recommended for documentation.
It's a feature for the advanced beta-testers.

Examples:

    Parcels
    Transit Stops
    Routes
    Ridership

Data objects provide:

    - Documentation
    - Discovery
    - Optional validation
    - Data access via getter functions
    - Result creation via fulfill()


Use Data.fulfill() to create Result objects.

Example
-------

    parcels = Data(
        "Parcels",
        dtype=gpd.GeoDataFrame,
        doc="County parcel polygons."
    )

    @parcels.getter
    def get(d):
        return gpd.read_parquet("parcels.parquet")

    gdf = parcels.get()

    result = parcels.fulfill(gdf)
"""

from collections.abc import Callable
from typing import Any

from .caching.result import Result
from .policies import Policy, Status


class Data:
    """
    Defines a dataset.

    A Data object describes a dataset and provides
    a standard interface for retrieving or validating it.

    It may also create Result objects through fulfill().

    Notes
    -----
    Data objects should remain relatively stable after
    registration. They represent the identity and purpose
    of a dataset rather than a specific runtime value.
    """

    _registry: dict[str, "Data"] = {}

    def __init__(
        self,
        name: str,
        dtype: type | None = None,
        doc: str | None = None,
        source: str | None = None,
        owner: str | None = None,
        tags: list[str] | None = None,
        policy: Policy | None = None,  # Freshness / invalidation / expiry policy
        **kwargs,
    ):
        self.name = name
        self.dtype = dtype
        self.doc = doc

        # Future-facing metadata
        self.source = source
        self.owner = owner
        self.tags = tags or []

        self._getter: Callable | None = None
        self._validator: Callable | None = None

        self._kwargs = kwargs

        # Freshness / invalidation / expiry policy
        self.policy = policy
        
        Data._registry[name] = self

    # ============================================================
    # Registration

    def getter(self, func: Callable) -> Callable:
        """
        Register the dataset getter.

        Example
        -------

            @parcels.getter
            def get(data):
                return ...
        """
        self._getter = func
        return func

    def validator(self, func: Callable) -> Callable:
        """
        Register a validation function.

        Example
        -------

            @parcels.validator
            def validate(data, value):
                ...
                return value
        """
        self._validator = func
        return func

    # ============================================================
    # Access

    def get(self, validate: bool = False, *args, **kwargs) -> Any:
        """
        Retrieve dataset contents.

        Parameters
        ----------
        validate:
            Apply the registered validator before returning.
        """
        if self._getter is None:
            raise AttributeError(
                f"No getter registered for '{self.name}'."
            )

        value = self._getter(
            self,
            *args,
            **kwargs,
        )

        if validate:
            value = self.validate(value)

        return value

    def dependency_name(self):  # Recommended by Copilot
        return self.name

    def validate(self, value: Any) -> Any:
        """
        Validate a value using the registered validator.

        Returns
        -------
        Any
            The validated value.
        """
        if self._validator is None:
            return value

        return self._validator(
            self,
            value,
        )

    # ============================================================
    # Result creation

    def fulfill(
        self,
        value: Any,
        # TODO: validate: bool = True,
        *,
        name: str | None = None,
        extra: dict | None = None,
    ) -> "Result":
        """
        Simply creates a Result from this dataset definition.

        Notes
        -----
        This method:

        - Does NOT cache anything.
        - Does NOT mutate the Data object.
        - Does NOT validate automatically.

        It simply creates a Result and clarifies
        where that Result originated.

        Examples
        --------

            return parcels.fulfill(gdf)

            return (
                parcels.fulfill(parcel_gdf),
                centroids.fulfill(centroid_gdf),
            )
        """
        return Result(
            name=name or self.name,
            data=value,
            task_name=None,  # Task will populate this later
            extra=extra,
        )

    # ============================================================
    # Discovery

    @classmethod
    def lookup(cls, name: str) -> "Data":
        """
        Retrieve a registered Data definition.
        """
        return cls._registry[name]

    @classmethod
    def all(cls) -> list["Data"]:
        """
        Return all registered Data definitions.
        """
        return list(cls._registry.values())

    @classmethod
    def find_locals(cls, namespace: dict) -> list:
        """
        Useful when constructing __all__.

        Example
        -------

            __all__ = Data.find_locals(locals())
        """
        return [
            name
            for name, obj in namespace.items()
            if isinstance(obj, cls)
        ]

    # ============================================================
    # Lifecycle
    def status(self) -> Status:

        if self.policy is None:

            return Status.unknown(
                "No policy configured"
            )

        return self.policy.check()

    # ============================================================
    # Dunders

    def __getattr__(self, attr: str) -> Any:
        if attr in self._kwargs:
            return self._kwargs[attr]

        raise AttributeError(
            f"{self.__class__.__name__!s} has no attribute '{attr}'."
        )

    def __repr__(self) -> str:
        dtype = (
            self.dtype.__name__
            if self.dtype is not None
            else "Any"
        )

        return (
            f"<Data '{self.name}' "
            f"({dtype})>"
        )
