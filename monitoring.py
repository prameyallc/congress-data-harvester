import time
import psutil
import logging
import boto3
from datetime import datetime, timezone
from functools import wraps
from typing import Dict, Any, Callable

class MetricsCollector:
    def __init__(self, service_name: str, region: str = 'us-west-2'):
        self.service_name = service_name
        self.logger = logging.getLogger('congress_downloader')
        self.cloudwatch_enabled = False
        self.request_start_times: Dict[str, float] = {}
        try:
            self.cloudwatch = boto3.client('cloudwatch', region_name=region)
            # Test CloudWatch permissions
            self.cloudwatch.put_metric_data(
                Namespace=f'CongressDownloader/{self.service_name}',
                MetricData=[{
                    'MetricName': 'startup_test',
                    'Value': 1.0,
                    'Unit': 'Count'
                }]
            )
            self.cloudwatch_enabled = True
            self.metrics_buffer = []
            self.buffer_size = 20  # Batch size for CloudWatch metrics
        except Exception as e:
            self.logger.info(f"CloudWatch metrics disabled: {str(e)}")
            self.cloudwatch = None
            self.metrics_buffer = []

    def _put_metric(self, name: str, value: float, unit: str, dimensions: Dict[str, str] = None):
        """Send metric to CloudWatch with buffering"""
        if not self.cloudwatch_enabled:
            return

        try:
            timestamp = datetime.now(timezone.utc)
            metric_data = {
                'MetricName': name,
                'Value': value,
                'Unit': unit,
                'Timestamp': timestamp
            }

            if dimensions:
                metric_data['Dimensions'] = [
                    {'Name': k, 'Value': v} for k, v in dimensions.items()
                ]

            self.metrics_buffer.append(metric_data)

            # Flush buffer if it reaches the threshold
            if len(self.metrics_buffer) >= self.buffer_size:
                self.flush_metrics()
        except Exception as e:
            if not str(e).startswith('CloudWatch metrics disabled'):
                self.logger.debug(f"Failed to buffer metric {name}: {str(e)}")

    def flush_metrics(self):
        """Flush buffered metrics to CloudWatch"""
        if not self.cloudwatch_enabled or not self.metrics_buffer:
            return

        try:
            self.cloudwatch.put_metric_data(
                Namespace=f'CongressDownloader/{self.service_name}',
                MetricData=self.metrics_buffer
            )
            self.metrics_buffer.clear()
        except Exception as e:
            if not str(e).startswith('CloudWatch metrics disabled'):
                self.logger.debug(f"Failed to send metrics to CloudWatch: {str(e)}")

    def track_duration(self, operation: str):
        """Decorator to track operation duration"""
        def decorator(func: Callable):
            @wraps(func)
            def wrapper(*args, **kwargs):
                start_time = time.time()
                try:
                    result = func(*args, **kwargs)
                    duration = time.time() - start_time
                    self._put_metric(
                        f"{operation}_duration",
                        duration,
                        'Seconds',
                        {'Operation': operation}
                    )
                    return result
                except Exception as e:
                    self._put_metric(
                        f"{operation}_errors",
                        1,
                        'Count',
                        {'Operation': operation, 'ErrorType': type(e).__name__}
                    )
                    raise
            return wrapper
        return decorator

    def track_api_request_start(self, endpoint: str):
        """Track the start of an API request"""
        self.request_start_times[endpoint] = time.time()
        self._put_metric(
            'api_requests_initiated',
            1,
            'Count',
            {'Endpoint': endpoint}
        )

    def track_api_request(self, endpoint: str, status_code: int, duration: float):
        """Track API request metrics with enhanced monitoring"""
        dimensions = {
            'Endpoint': endpoint,
            'StatusCode': str(status_code)
        }

        # Track request duration
        self._put_metric('api_request_duration', duration, 'Seconds', dimensions)

        # Track request count
        self._put_metric('api_requests', 1, 'Count', dimensions)

        # Track success/failure
        if 200 <= status_code < 300:
            self._put_metric('api_request_success', 1, 'Count', dimensions)
        else:
            self._put_metric('api_request_failure', 1, 'Count', dimensions)
            error_type = 'rate_limit' if status_code == 429 else 'other'
            self._put_metric(
                'api_request_errors',
                1,
                'Count',
                {**dimensions, 'ErrorType': error_type}
            )

    def track_rate_limit_wait(self, endpoint: str, wait_time: float):
        """Track rate limit wait times"""
        dimensions = {'Endpoint': endpoint}
        self._put_metric('rate_limit_wait_time', wait_time, 'Seconds', dimensions)
        self._put_metric('rate_limit_waits', 1, 'Count', dimensions)

    def track_dynamo_operation(self, operation: str, table: str, success: bool, duration: float):
        """Track DynamoDB operation metrics"""
        dimensions = {
            'Operation': operation,
            'Table': table,
            'Status': 'Success' if success else 'Failure'
        }

        self._put_metric('dynamo_operation_duration', duration, 'Seconds', dimensions)
        self._put_metric('dynamo_operations', 1, 'Count', dimensions)

    def track_bills_processed(self, count: int, success: bool = True):
        """Track number of bills processed"""
        status = 'Successful' if success else 'Failed'
        self._put_metric(
            'bills_processed',
            count,
            'Count',
            {'Status': status}
        )

    def track_resource_usage(self):
        """Track system resource usage"""
        try:
            # Memory usage
            memory = psutil.Process().memory_info()
            self._put_metric('memory_usage', memory.rss / 1024 / 1024, 'Megabytes')

            # CPU usage
            cpu_percent = psutil.Process().cpu_percent()
            self._put_metric('cpu_usage', cpu_percent, 'Percent')

            # Disk IO
            disk_io = psutil.disk_io_counters()
            if disk_io:  # Check if disk IO stats are available
                self._put_metric('disk_read_bytes', disk_io.read_bytes, 'Bytes')
                self._put_metric('disk_write_bytes', disk_io.write_bytes, 'Bytes')
        except Exception as e:
            self.logger.debug(f"Failed to collect resource metrics: {str(e)}")

# Global metrics collector instance
metrics = MetricsCollector('Development')