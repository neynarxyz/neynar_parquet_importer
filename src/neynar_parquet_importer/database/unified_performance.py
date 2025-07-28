"""
Unified performance and memory management for all database backends.

This module consolidates performance monitoring to eliminate redundant 
system calls and provide consistent memory management across backends.
"""

import logging
import time
import psutil
from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Union
from contextlib import contextmanager
from enum import Enum

# Import for type annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from ..settings import Settings

logger = logging.getLogger(__name__)


class MonitoringLevel(Enum):
    """Performance monitoring levels"""
    DISABLED = "disabled"      # No monitoring (PostgreSQL default)
    MINIMAL = "minimal"        # Basic metrics only  
    STANDARD = "standard"      # Standard monitoring (Neo4j default)
    DETAILED = "detailed"      # Full detailed monitoring


@dataclass
class UnifiedMetrics:
    """Consolidated metrics for all backends"""
    operations_count: int = 0
    total_time: float = 0.0
    memory_used_mb: float = 0.0
    peak_memory_mb: float = 0.0
    batch_times: List[float] = None
    errors_count: int = 0
    backend_type: str = "unknown"
    monitoring_level: MonitoringLevel = MonitoringLevel.DISABLED
    
    def __post_init__(self):
        if self.batch_times is None:
            self.batch_times = []
    
    @property
    def operations_per_second(self) -> float:
        """Calculate operations per second"""
        if self.total_time == 0:
            return 0.0
        return self.operations_count / self.total_time


class UnifiedPerformanceManager:
    """
    Single performance manager for all database backends.
    
    Eliminates redundant psutil calls and provides configurable monitoring levels.
    """
    
    def __init__(self, backend_type: str, monitoring_level: MonitoringLevel = MonitoringLevel.STANDARD):
        self.backend_type = backend_type
        self.monitoring_level = monitoring_level
        self.metrics = UnifiedMetrics(backend_type=backend_type, monitoring_level=monitoring_level)
        
        # Single psutil.Process instance (eliminates redundancy)
        self._process = psutil.Process() if monitoring_level != MonitoringLevel.DISABLED else None
        
        # Cached memory readings to reduce system calls
        self._cached_memory: Optional[Dict[str, float]] = None
        self._last_memory_check: float = 0
        self._memory_check_interval: float = 5.0  # Cache for 5 seconds
        
        # Batch monitoring configuration
        self._batch_count = 0
        self._memory_check_frequency = 10  # Check memory every N batches
        
        # Performance thresholds
        self.max_memory_mb = 2048
        self.batch_size_limits = (100, 10000)
        self.current_batch_size = 1000
        
        self.start_time: Optional[float] = None
        
    def start_monitoring(self):
        """Start performance monitoring"""
        if self.monitoring_level == MonitoringLevel.DISABLED:
            return
            
        self.start_time = time.time()
        if self._process:
            self.metrics.memory_used_mb = self._get_memory_usage()['rss_mb']
        logger.info(f"Unified performance monitoring started for {self.backend_type}")
    
    def _get_memory_usage(self) -> Dict[str, float]:
        """Get cached memory usage to reduce system calls"""
        if self.monitoring_level == MonitoringLevel.DISABLED or not self._process:
            return {'rss_mb': 0, 'vms_mb': 0, 'percent': 0}
        
        current_time = time.time()
        
        # Use cached reading if recent enough
        if (self._cached_memory is not None and 
            current_time - self._last_memory_check < self._memory_check_interval):
            return self._cached_memory
        
        # Refresh memory reading
        try:
            memory_info = self._process.memory_info()
            self._cached_memory = {
                'rss_mb': memory_info.rss / 1024 / 1024,
                'vms_mb': memory_info.vms / 1024 / 1024,
                'percent': self._process.memory_percent()
            }
            self._last_memory_check = current_time
        except Exception as e:
            logger.warning(f"Error getting memory usage: {e}")
            self._cached_memory = {'rss_mb': 0, 'vms_mb': 0, 'percent': 0}
        
        return self._cached_memory
    
    def should_check_memory_this_batch(self) -> bool:
        """Determine if memory should be checked this batch (reduces frequency)"""
        self._batch_count += 1
        return (self.monitoring_level != MonitoringLevel.DISABLED and 
                self._batch_count % self._memory_check_frequency == 0)
    
    def check_memory_pressure(self) -> Dict[str, Any]:
        """Check memory pressure with configurable frequency"""
        if not self.should_check_memory_this_batch():
            return {'should_pause': False, 'usage': {}}
        
        memory_usage = self._get_memory_usage()
        should_pause = memory_usage['rss_mb'] > self.max_memory_mb
        
        return {
            'should_pause': should_pause,
            'usage': memory_usage,
            'suggest_gc': memory_usage['rss_mb'] > self.max_memory_mb * 0.8
        }
    
    @contextmanager
    def batch_timer(self):
        """Context manager for timing batch operations with minimal overhead"""
        if self.monitoring_level == MonitoringLevel.DISABLED:
            yield  # No monitoring overhead
            return
        
        start_time = time.time()
        start_memory = None
        
        # Only get memory on detailed monitoring or periodic checks
        if (self.monitoring_level == MonitoringLevel.DETAILED or 
            self.should_check_memory_this_batch()):
            start_memory = self._get_memory_usage()
        
        try:
            yield
        finally:
            batch_time = time.time() - start_time
            self.metrics.batch_times.append(batch_time)
            
            # Update memory tracking if we measured it
            if start_memory:
                current_memory = self._get_memory_usage()
                self.metrics.memory_used_mb = current_memory['rss_mb']
                if current_memory['rss_mb'] > self.metrics.peak_memory_mb:
                    self.metrics.peak_memory_mb = current_memory['rss_mb']
    
    def record_operations(self, count: int):
        """Record the number of operations processed"""
        self.metrics.operations_count += count
    
    def record_error(self):
        """Record an error occurrence"""
        self.metrics.errors_count += 1
    
    def adjust_batch_size(self, batch_time: float) -> int:
        """Simple, conservative batch size adjustment"""
        if self.monitoring_level == MonitoringLevel.DISABLED:
            return self.current_batch_size
        
        # Only adjust if consistently slow (> 3 seconds per batch)
        if len(self.metrics.batch_times) >= 5:
            recent_avg = sum(self.metrics.batch_times[-5:]) / 5
            memory_check = self.check_memory_pressure()
            
            if recent_avg > 3.0 and memory_check['should_pause']:
                self.current_batch_size = max(
                    self.batch_size_limits[0],
                    int(self.current_batch_size * 0.8)  # 20% reduction
                )
                logger.info(f"Reduced batch size to {self.current_batch_size} due to performance")
        
        return self.current_batch_size
    
    def get_current_metrics(self) -> UnifiedMetrics:
        """Get current performance metrics"""
        return self.metrics
    
    def stop_monitoring(self):
        """Stop monitoring and log final metrics"""
        if self.monitoring_level == MonitoringLevel.DISABLED:
            return
            
        if self.start_time:
            self.metrics.total_time = time.time() - self.start_time
        
        if self.monitoring_level in [MonitoringLevel.STANDARD, MonitoringLevel.DETAILED]:
            logger.info(f"Performance Summary for {self.backend_type}:")
            logger.info(f"  Operations: {self.metrics.operations_count}")
            logger.info(f"  Total Time: {self.metrics.total_time:.2f}s")
            logger.info(f"  Operations/sec: {self.metrics.operations_per_second:.2f}")
            logger.info(f"  Peak Memory: {self.metrics.peak_memory_mb:.2f} MB")
            logger.info(f"  Errors: {self.metrics.errors_count}")


def create_performance_manager(backend_type: str, settings: Optional['Settings'] = None) -> UnifiedPerformanceManager:
    """Factory function to create appropriate performance manager"""
    
    # Determine monitoring level based on backend and settings
    if backend_type == "postgresql":
        # PostgreSQL: Minimal monitoring to maintain zero-overhead promise
        monitoring_level = MonitoringLevel.MINIMAL
    elif backend_type == "neo4j":
        # Neo4j: Standard monitoring for optimization
        monitoring_level = MonitoringLevel.STANDARD
    else:
        monitoring_level = MonitoringLevel.DISABLED
    
    # Allow override via settings
    if settings and hasattr(settings, 'performance_monitoring_level'):
        try:
            monitoring_level = MonitoringLevel(settings.performance_monitoring_level)
        except ValueError:
            pass  # Keep default
    
    return UnifiedPerformanceManager(backend_type, monitoring_level)
