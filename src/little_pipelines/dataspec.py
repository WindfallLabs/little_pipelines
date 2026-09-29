"""
DataSpec - Define and document data.

A DataSpec object represents a conceptual dataset and definition.
It's optional, but highly recommended for documentation.
It's a feature for the advanced beta-testers.

Examples:

    Parcels
    Transit Stops
    Routes
    Ridership

DataSpec objects provide:

    - Documentation
    - Discovery
    - Optional validation
    - Data access via getter functions
    - Result creation via fulfill()


Use DataSpec.fulfill() to create Result objects.

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
from functools import wraps
from typing import Any

from . import exc
from .caching import Cache, Result
#from .policies import Policy, Status  # TODO: review


class DataSpec:
    """
    Defines a dataset.

    A DataSpec object describes a dataset and provides
    a standard interface for retrieving or validating it.

    It may also create Result objects through fulfill().

    Notes
    -----
    DataSpec objects should remain relatively stable after
    registration. They represent the identity and purpose
    of a dataset rather than a specific runtime value.
    """

    _registry: dict[str, "DataSpec"] = {}

    def __init__(
        self,
        name: str,
        dtype: type | None = None,
        doc: str | None = None,
        source: str | None = None,
        owner: str | None = None,
        tags: list[str] | None = None,
        #policy: Policy | None = None,  # Freshness / invalidation / expiry policy
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
        #self.policy = policy
        
        DataSpec._registry[name] = self

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
            d = DataSpec("Name")

            @d.validator
            def validate(this: DataSpec, value):
                assert value == 42, "Wrong!"
        """
        @wraps(func)
        def _validation_wrapper(value: Any) -> Any:
            return func(self, value)

        self._validator = _validation_wrapper
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

        value = self._getter(self, *args, **kwargs)

        if validate:
            value = self.validate(value)

        return value

    def get_from_cache(self, cache: Cache) -> Any:  # TODO: test
        """
        Gets the cached Result with the same name as this DataSpec.
        """
        r: Result = cache.get(self.name)
        value = self.validate(r.value)

        return value

    def validate(self, value: Any, validate_dtype=True) -> Any:
        """
        Validate a value using the registered validator.

        If there is no custom validator and dtype is provided, only type is validated.

        Returns
        -------
        Any
            The validated value.
        """
        if validate_dtype and self.dtype is not None:
            if self.dtype is not None and not isinstance(value, self.dtype):
                raise exc.DataSpecValidationError(f"Expected {self.dtype}, got {type(value)}")

        if not self._validator:
            return value

        return self._validator(value)

    # ============================================================
    # Result creation

    def fulfill(
        self,
        value: Any,
        #*,
        validate: bool = True,
        name: str | None = None,
        extra: dict | None = None,
    ) -> "Result":
        """
        Simply creates a Result from this dataset definition.

        Notes
        -----
        This method:

        - Does NOT cache anything.
        - Does NOT mutate the DataSpec object.
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
        if validate:
            value = self.validate(value)

        return Result(
            name=name or self.name,
            value=value,
            task_name=None,  # Task will populate this later
            extra=extra,
        )

    # ============================================================
    # Discovery

    @classmethod
    def lookup(cls, name: str) -> "DataSpec":
        """
        Retrieve a registered DataSpec definition.
        """
        return cls._registry[name]

    @classmethod
    def all(cls) -> list["DataSpec"]:
        """
        Return all registered DataSpec definitions.
        """
        return list(cls._registry.values())

    @classmethod
    def find_locals(cls, namespace: dict) -> list:
        """
        Useful when constructing __all__.

        Example
        -------

            __all__ = DataSpec.find_locals(locals())
        """
        return [
            name
            for name, obj in namespace.items()
            if isinstance(obj, cls)
        ]

    # ============================================================
    # Lifecycle
    # def status(self) -> Status:

    #     if self.policy is None:

    #         return Status.unknown(
    #             "No policy configured"
    #         )

    #     return self.policy.check()

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
            f"<DataSpec '{self.name}' "
            f"({dtype})>"
        )
