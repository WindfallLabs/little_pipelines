"""
.
"""

from dataclasses import dataclass
from typing import Any, Callable, Literal, Optional
from functools import cache, wraps

from little_pipelines.caching.cache import Cache
from little_pipelines.caching.result import Result


_NO_CACHE_ERR = "No cache set."



from typing import Any, Callable, Optional

from little_pipelines.caching.cache import Cache
from little_pipelines.caching.result import Result


class Datasource:
    """
    Named, documented access to one piece of data.

    Usage:
        raw_parcels = Datasource("raw_parcels", doc="As delivered by MDOR.")

        @raw_parcels.getter
        def get(d, *a, **kw):
            return gpd.read_file(d.raw_path, layer="raw_parcels")

        raw_parcels.get()
    """

    _registry: dict[str, "Datasource"] = {}

    def __init__(
        self,
        name: str,
        dtype: type,
        doc: Optional[str] = None,
        #schema: Optional[type] = None,
        cache: Optional[Cache] = None,
        **kwargs,
    ):
        self.name = name
        if not isinstance(dtype, type):
            raise TypeError(f"Not a type: {type}")
        self.dtype = dtype
        self.doc = doc
        #self.schema = schema
        self._cache = cache
        self._kwargs: dict = kwargs

        self._getter: Optional[Callable] = None
        self._validator: Optional[Callable] = None
        self._writer: Optional[Callable] = None

        Datasource._registry[name] = self

    def getter(self, func: Callable) -> Callable:
        self._getter = func
        return func

    def validator(self, func: Callable) -> Callable:
        self._validator = func
        return func

    def writer(self, func: Callable) -> Callable:
        self._writer = func
        return func

    @classmethod
    def get_datasource(cls, datasource_name: str) -> "Datasource":
        return cls._registry[datasource_name]

    @classmethod
    def get_data(cls, datasource_name: str, *args, **kwargs) -> Any:
        """Returns the data for the given Datasource name."""
        return cls._registry[datasource_name].get(*args, **kwargs)

    @classmethod
    def find_locals(cls, _locals: dict):
        """Useful for setting the `__all__` of a module.
        
        Use:
            `__all__ = Datasource.find_locals(locals())`
        """
        return [
            k for k, v in _locals.items()
            if isinstance(v, cls) and getattr(v, "__name__", None) != "Datasource"
        ]

    def get(self, validate: bool = False, *args, **kwargs) -> Any:
        if not hasattr(self, "_getter") or not isinstance(self, Datasource):
            raise AttributeError("`get` can only be called from instances")
        if self._getter is not None:
            data = self._getter(self, *args, **kwargs)
        elif self._cache is not None:
            data = self._cache.get(self.name)[0].data
        else:
            raise AttributeError(f"No getter registered for '{self.name}'.")

        if validate and self._validator is not None:
            data = self._validator(self, data, *args, **kwargs)
        return data

    def write(self, data: Any, *args, **kwargs) -> None:
        task = kwargs.get("task", None)
        if self._writer is not None:
            return self._writer(self, data, *args, **kwargs)
        elif self._cache is not None:
            self._cache.put(Result(name=self.name, task=task, data=data))
        else:
            raise AttributeError(f"No writer registered for '{self.name}' and no cache set.")
        return None

    def __getattr__(self, prop: str) -> Any:
        # Handle user-defined properties from kwargs
        if prop in self._kwargs:
            return self._kwargs[prop]
        # Handle all other property accessors
        else:
            return self.__getattribute__(prop)

    def __repr__(self) -> str:
        try:
            return f"<Datasource: '{self.name}' ({self.dtype.__name__})>"
        except Exception as e:
            print(e)
            return f"Error {self.name} {self.dtype}"



#__all__ = ["Datasource"]



@dataclass
class DataLayer:
    """Layers should represent alternate states of the same dataset, not multiple outputs of a Task."""
    name: str
    doc: Optional[str] = None
    dtype: Optional[type] = None
    getter: Optional[Callable] = None
    value: Optional[Any] = None
    validator: Optional[Callable] = None
    writer: Optional[Callable] = None

    def __repr__(self):
        return f"DataLayer(name='{self.name}')"


class Data:
    """
    A standard data access object.
    Primarily as a documented dataset definition and accessor.

    Usage:
        my_data = Data("MyData")

        @my_data.getter("default")
        def get(d: Data, *args, **kwargs):
            return "A"

        # Or with multiple layers
        @my_data.getter("b")
        def b(d: Data, *args, **kwargs):
            return "B"

        @my_data.getter("c")
        def c(d: Data, *args, **kwargs):
            return "C"

        # Access via:
        my_data.get()              # If only one layer (or an implicit default) exists

        ## Or using named layers:
        my_data.get("b")      # Explicit access by layer name
        my_data.b             # Property accessor
        my_data["b"]          # Dict-like accessor

    """
    # Registry
    _registry: dict[str, "Data"] = {}

    # Special names that signal "this is the default getter, not a named layer"
    _DEFAULT_SENTINELS = frozenset({"get", "cache"})

    def __init__(
        self,
        name: str,
        doc: Optional[str] = None,
        #default: Optional[str] = None,
        cache: Optional[Cache] = None,
        **kwargs
    ):
        """
        Initialize a Data object.

        Args:
            name: Name of the data entity (e.g., "Parcels")
            default: Optional default layer to use when .get() called without args.
                    If None and multiple layers exist, .get() will raise an error.
        """
        self._kwargs = kwargs
        self._name = name
        self._layers: dict[str, DataLayer] = dict()

        # Cache and cache-getter
        self.cache = cache
        ## A cache-getter is automatically set if a cache is provided
        if self.cache:
            self._layers["cache"] = DataLayer("cache", getter=self.get_from_cache)
        
        # Set default data layer
        self._layers["default"] = DataLayer("default")

        # Register
        Data._registry[name] = self

    @property
    def registered(self):  # Placeholder - e.g. MyData in lp.Data.registered
        ...

    @property
    def _default(self):
        return "default"

    @property  # TODO: cached_property or something
    def data(self) -> Any:
        """Returns the data (for the default layer)."""
        return self.get()

    @property
    def layers(self) -> list[str]:
        """List all registered layers."""
        #return list(self._layers.keys())
        return self._layers

    @property
    def name(self) -> str:
        """Get the name of this Data object"""
        return self._name

    @property
    def values(self):
        return self._values

    # def set_dtype(self, layer_or_type, type_=None):
    #     """Decorator to register a data type (class) for a layer.

    #     Supports three usage patterns:
    #         @data.set_dtype          # uses function name as layer; if 'default', sets default
    #         @data.set_dtype("name")  # explicit layer name
    #         or my_data.set_dtype('layer', int) for primatives
    #     """

    #     def register(t, layer_name):
    #         if not isinstance(t, type):
    #             raise TypeError(
    #                 f"dtype must be a type, not a value: {t!r}. "
    #                 f"Did you mean {type(t).__name__!r}?"
    #             )
    #         if layer_name not in self._layers:
    #             self._validate_layer_name(layer_name)
    #         layer = self._layers.setdefault(layer_name, DataLayer(layer_name))
    #         layer.dtype = t
    #         return t

    #     # my_data.set_dtype(int) — bare call with just a type
    #     if isinstance(layer_or_type, type):
    #         return register(layer_or_type, self._default)

    #     # my_data.set_dtype("output1", int) — direct call with layer + type
    #     if type_ is not None:
    #         return register(type_, layer_or_type)

    #     # @my_data.set_dtype("raw") — decorator form
    #     def decorator(t):
    #         return register(t, layer_or_type)
    #     return decorator

    def getter(self, layer_or_func: str | Callable = None):
        """Decorator to register a getter function for a layer.

        Supports two usage patterns:
            @data.getter          # uses function name as layer; if 'default', sets default
            @data.getter("name")  # explicit layer name
        """
        def register(getter_func: Callable, layer_name: str) -> Callable:
            if layer_name not in self._layers:
                self._validate_layer_name(layer_name)
            layer = self._layers.setdefault(layer_name, DataLayer(layer_name))
            layer.getter = getter_func
            return getter_func

        # @data.getter -- bare decorator, no layer name / argument
        if callable(layer_or_func):
            return register(layer_or_func, self._default)

        # @data.getter("name") -- called with an explicit layer name
        def decorator(getter_func: Callable) -> Callable:
            layer_name = layer_or_func or getter_func.__name__
            return register(getter_func, layer_name)

        return decorator

    def get(self, layer: Optional[str] = None, validate=False, *args, **kwargs) -> Any:
        """Retrieve data from a specific layer or the default layer."""
        if layer == "cache" and not self.cache:
            raise AttributeError(_NO_CACHE_ERR)
        if layer is None:
            layer = self._default
            if len([i for i in self._layers.keys() if i != "cache"]) == 1:
                layer = list(self._layers.keys())[0]

        lyr = self._layers[layer]
        data = lyr.getter(self)
        if validate is True and lyr.validator is not None:
            data = lyr.validator(data)

        return data

    @cache
    def get_from_cache(self, *args, **kwargs):
        """Return data from cache."""
        if self.cache is None:
            raise AttributeError(_NO_CACHE_ERR)
        return self.cache.get(self.name)[0]  # TODO: IndexError isn't helpful

    def fulfill(self, value: Any = None, layer: Optional[str] = None, validate=True) -> Result:
        """Fulfills the data layer value and creates a Result object."""
        if layer is None:
            layer = self._default
        lyr = self.layers[layer]

        if lyr.validator is not None and validate is True:
            value: Any = lyr.validator(value)

        # TODO: call self.write()???
        return Result(self.name, task_name=None, data=value)  # NOTE: process or main decorators will fill-in task_name

    # def validator(self, layer: Optional[str], value: Any):
    #     """Validates a layer if a corresponding validator exists."""
    #     validator: Callable|None = self._layers[layer].validator
    #     if not validator:
    #         return value
    #     return validator(value)

    def validator(self, layer_or_func: str | Callable = None):
        """Decorator to register a validation function for a layer.

        Supports three usage patterns:
            @data.validator          # uses function name as layer; if 'default', sets default
            @data.validator("name")  # explicit layer name
        """

        def register(validator_func: Callable, layer_name: str) -> Callable:
            if layer_name not in self._layers:
                self._validate_layer_name(layer_name)
            layer = self._layers.setdefault(layer_name, DataLayer(layer_name))
            layer.validator = validator_func
            return validator_func

        # @data.getter -- bare decorator, no layer name / argument
        if callable(layer_or_func):
            return register(layer_or_func, self._default)

        # @data.getter("name") -- called with an explicit layer name
        def decorator(validator_func: Callable) -> Callable:
            layer_name = layer_or_func or validator_func.__name__
            return register(validator_func, layer_name)

        return decorator

    def writer(self, layer_name: str):
        """."""
        # Think about how a Result might subclass Data to add a write() method to Data.
        # Consider renaming Data -> DataSource; and have a Result with 'write()'
        # Should dependencies have access to 'write()', how could they not?
        # Probably returns a function
        """Decorator to register a writer function for a layer.

        Use
            @data.writer("layer_name", data: Any)
        """
        def register(writer_func: Callable, layer_name: str) -> Callable:
            if layer_name not in self._layers:
                self._validate_layer_name(layer_name)
            layer = self._layers.setdefault(layer_name, DataLayer(layer_name))
            layer.writer = writer_func
            return writer_func

        # @data.getter -- bare decorator, no layer name / argument
        # if callable(layer_or_func):
        #     return register(layer_or_func, self._default)

        # @data.getter("name") -- called with an explicit layer name
        def decorator(writer_func: Callable) -> Callable:
            layer_name = layer_or_func or writer_func.__name__
            return register(writer_func, layer_name)

        return decorator

    def doc_getter(self, layer_or_func=None):  # TODO:
        """Decorator: register a user-defined documentation function."""
        self._doc_getter = func
        return func

    def _validate_layer_name(self, layer: str) -> None:
        """Raise if layer_name is reserved or already registered."""
        reserved = tuple(self.__dict__.keys()) + tuple(self._DEFAULT_SENTINELS)
        if layer in reserved:
            raise ValueError(
                f"Layer name '{layer}' is reserved and cannot be used."
            )
        if layer in self._layers.keys():
            raise ValueError(f"Layer '{layer}' is already registered.")

    def __getitem__(self, layer: str) -> Any:
        """Dict-like data accessor."""
        return self.get(layer)

    def __getattr__(self, prop: str) -> Any:
        """Property accessor."""
        # Handle data getter as property
        if prop in self._layers.keys():
            return self._layers[prop].getter(self)
        # Handle user-defined properties from kwargs
        elif prop in self._kwargs:
            return self._kwargs[prop]
        # Handle all other property accessors
        else:
            return self.__getattribute__(prop)

    # def __repr__(self) -> str:
    #     layers_str = ", ".join(layers) if self.layers else "no layers"
    #     return f"Data(name='{self._name}, layers={self.n_layers}'"


#__all__ = ["Data"]  # NOTE: I see no reason to export the DataLayer object
