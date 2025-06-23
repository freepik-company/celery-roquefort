from typing import Union
import prometheus_client as prom

class MetricService:

    def __init__(
        self, 
        metric_prefix: str,
        custom_labels: dict = None,
    ) -> None:
        self.metric_prefix = metric_prefix or "roquefort_"
        self.counters = {}
        self.gauges = {}
        self.histograms = {}
        self._registry = None
        self._custom_labels = custom_labels or {}
        self._custom_labels_keys = list(self._custom_labels.keys())
        self._configure_registry()
        
    def _configure_registry(self):
        # Disable the default metrics
        prom.disable_created_metrics()
        # Disable python metrics
        prom.REGISTRY.unregister(prom.GC_COLLECTOR)
        prom.REGISTRY.unregister(prom.PLATFORM_COLLECTOR)
        prom.REGISTRY.unregister(prom.PROCESS_COLLECTOR)
        self._registry = prom.CollectorRegistry(auto_describe=True)
        prom.REGISTRY.register(self._registry)
        
    def increment_counter(self, name: str, value: int = 1, labels: dict = None):
        labels = labels or {}
        self.counters[name].labels(**labels, **self._custom_labels).inc(value)
        
    def set_gauge(self, name: str, value: Union[int, float, bool], labels: dict = None):
        labels = labels or {}
        self.gauges[name].labels(**labels, **self._custom_labels).set(value)
        
    def create_counter(self, name: str, description: str, labels: list[str] = []):
        labels = labels + self._custom_labels_keys
        self.counters[name] = prom.Counter(name=f"{self.metric_prefix}{name}", documentation=description, labelnames=labels, registry=self._registry)
        
    def create_gauge(self, name: str, description: str, labels: list[str] = []):
        labels = labels + self._custom_labels_keys
        self.gauges[name] = prom.Gauge(name=f"{self.metric_prefix}{name}", documentation=description, labelnames=labels, registry=self._registry)
        
    def create_histogram(self, name: str, description: str, labels: list[str] = [], buckets: tuple = None):
        labels = labels + self._custom_labels_keys
        self.histograms[name] = prom.Histogram(name=f"{self.metric_prefix}{name}", documentation=description, buckets=buckets or prom.Histogram.DEFAULT_BUCKETS, labelnames=labels, registry=self._registry)