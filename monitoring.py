import time
import psutil
import logging
import boto3
from datetime import datetime, timezone
from functools import wraps
from typing import Dict, Any, Callable, List, Optional

class MetricsCollector:
    def __init__(self, service_name: str, region: str = 'us-west-2'):
        self.service_name = service_name
        self.logger = logging.getLogger('congress_downloader')
        self.cloudwatch_enabled = False
        self.request_start_times: Dict[str, float] = {}

        # Enhanced statistics tracking
        self.endpoint_stats: Dict[str, Dict[str, Any]] = {}
        self.ingestion_stats: Dict[str, Dict[str, Any]] = {}
        self.session_start_time = time.time()

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

    def _put_metric(self, name: str, value: float, unit: str, dimensions: Optional[Dict[str, str]] = None):
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

        # Initialize endpoint stats if not exists
        if endpoint not in self.endpoint_stats:
            self.endpoint_stats[endpoint] = {
                'requests': 0,
                'success': 0,
                'failures': 0,
                'timeouts': 0,
                'rate_limit_hits': 0,
                'total_duration': 0,
                'wait_time': 0,
                'first_request': time.time(),
                'last_request': time.time()
            }

        self.endpoint_stats[endpoint]['requests'] += 1
        self.endpoint_stats[endpoint]['last_request'] = time.time()

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

        # Track success/failure and update endpoint stats
        if 200 <= status_code < 300:
            self._put_metric('api_request_success', 1, 'Count', dimensions)
            if endpoint in self.endpoint_stats:
                self.endpoint_stats[endpoint]['success'] += 1
                self.endpoint_stats[endpoint]['total_duration'] += duration
        else:
            self._put_metric('api_request_failure', 1, 'Count', dimensions)
            error_type = 'rate_limit' if status_code == 429 else 'other'
            self._put_metric(
                'api_request_errors',
                1,
                'Count',
                {**dimensions, 'ErrorType': error_type}
            )

            if endpoint in self.endpoint_stats:
                self.endpoint_stats[endpoint]['failures'] += 1
                if status_code == 429:
                    self.endpoint_stats[endpoint]['rate_limit_hits'] += 1
                elif status_code == 408:
                    self.endpoint_stats[endpoint]['timeouts'] += 1

    def track_rate_limit_wait(self, endpoint: str, wait_time: float):
        """Track rate limit wait times"""
        dimensions = {'Endpoint': endpoint}
        self._put_metric('rate_limit_wait_time', wait_time, 'Seconds', dimensions)
        self._put_metric('rate_limit_waits', 1, 'Count', dimensions)

        # Update endpoint stats
        if endpoint in self.endpoint_stats:
            self.endpoint_stats[endpoint]['wait_time'] += wait_time

    def track_dynamo_operation(self, operation: str, table: str, success: bool, duration: float):
        """Track DynamoDB operation metrics"""
        dimensions = {
            'Operation': operation,
            'Table': table,
            'Status': 'Success' if success else 'Failure'
        }

        self._put_metric('dynamo_operation_duration', duration, 'Seconds', dimensions)
        self._put_metric('dynamo_operations', 1, 'Count', dimensions)

    def track_items_processed(self, endpoint: str, total: int, success: int = 0, failed: int = 0, duplicates: int = 0):
        """Track number of items processed per endpoint"""
        # Initialize ingestion stats for endpoint if not exists
        if endpoint not in self.ingestion_stats:
            self.ingestion_stats[endpoint] = {
                'total_processed': 0,
                'successful': 0,
                'failed': 0,
                'duplicates': 0,
                'last_updated': time.time()
            }

        # Update stats
        stats = self.ingestion_stats[endpoint]
        stats['total_processed'] += total
        stats['successful'] += success
        stats['failed'] += failed
        stats['duplicates'] += duplicates
        stats['last_updated'] = time.time()

        # Send metrics
        status = 'Successful' if success else 'Failed'
        self._put_metric(
            f'{endpoint}_items_processed',
            total,
            'Count',
            {'Endpoint': endpoint, 'Status': status}
        )

        if duplicates > 0:
            self._put_metric(
                f'{endpoint}_duplicate_items',
                duplicates,
                'Count',
                {'Endpoint': endpoint}
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

    def generate_api_metrics_report(self) -> str:
        """Generate a detailed report on API usage metrics"""
        report_lines = ["API METRICS REPORT"]
        report_lines.append("=" * 80)
        report_lines.append(f"Session duration: {self._format_duration(time.time() - self.session_start_time)}")
        report_lines.append("")

        # Add endpoint summary
        report_lines.append("ENDPOINT STATISTICS")
        report_lines.append("-" * 80)
        report_lines.append(f"{'Endpoint':<25} {'Requests':<10} {'Success':<10} {'Failures':<10} {'Rate Limits':<12} {'Avg Duration':<15}")
        report_lines.append("-" * 80)

        for endpoint, stats in sorted(self.endpoint_stats.items()):
            requests = stats['requests']
            successes = stats['success']
            failures = stats['failures']
            rate_limits = stats['rate_limit_hits']

            avg_duration = 0
            if successes > 0:
                avg_duration = stats['total_duration'] / successes

            success_rate = (successes / requests * 100) if requests > 0 else 0

            report_lines.append(f"{endpoint:<25} {requests:<10} {successes:<10} {failures:<10} {rate_limits:<12} {avg_duration:.3f}s")

        report_lines.append("")
        report_lines.append("SUCCESS RATES")
        report_lines.append("-" * 80)

        for endpoint, stats in sorted(self.endpoint_stats.items()):
            requests = stats['requests']
            successes = stats['success']
            success_rate = (successes / requests * 100) if requests > 0 else 0
            report_lines.append(f"{endpoint:<25} {success_rate:.1f}% success rate")

        return "\n".join(report_lines)

    def generate_ingestion_report(self) -> str:
        """Generate a detailed report on data ingestion metrics"""
        report_lines = ["DATA INGESTION REPORT"]
        report_lines.append("=" * 80)
        report_lines.append(f"Session duration: {self._format_duration(time.time() - self.session_start_time)}")
        report_lines.append("")

        # Calculate totals
        total_processed = sum(stats['total_processed'] for stats in self.ingestion_stats.values())
        total_successful = sum(stats['successful'] for stats in self.ingestion_stats.values())
        total_failed = sum(stats['failed'] for stats in self.ingestion_stats.values())
        total_duplicates = sum(stats['duplicates'] for stats in self.ingestion_stats.values())

        # Add summary
        report_lines.append(f"Total items processed: {total_processed}")
        report_lines.append(f"Successfully stored: {total_successful}")
        report_lines.append(f"Failed items: {total_failed}")
        report_lines.append(f"Duplicate items skipped: {total_duplicates}")
        report_lines.append("")

        # Add per-endpoint breakdown
        report_lines.append("PER-ENDPOINT BREAKDOWN")
        report_lines.append("-" * 80)
        report_lines.append(f"{'Endpoint':<25} {'Processed':<10} {'Success':<10} {'Failed':<10} {'Duplicates':<12} {'Success Rate':<15}")
        report_lines.append("-" * 80)

        for endpoint, stats in sorted(self.ingestion_stats.items()):
            processed = stats['total_processed']
            successful = stats['successful']
            failed = stats['failed']
            duplicates = stats['duplicates']

            success_rate = (successful / (processed - duplicates) * 100) if (processed - duplicates) > 0 else 0

            report_lines.append(f"{endpoint:<25} {processed:<10} {successful:<10} {failed:<10} {duplicates:<12} {success_rate:.1f}%")

        return "\n".join(report_lines)

    def _format_duration(self, seconds: float) -> str:
        """Format seconds into human-readable duration"""
        if seconds < 60:
            return f"{seconds:.1f} seconds"

        minutes = seconds // 60
        remaining_seconds = seconds % 60

        if minutes < 60:
            return f"{int(minutes)} minutes {int(remaining_seconds)} seconds"

        hours = minutes // 60
        remaining_minutes = minutes % 60

        return f"{int(hours)} hours {int(remaining_minutes)} minutes"

    def reset_stats(self):
        """Reset all statistics for a new session"""
        self.endpoint_stats.clear()
        self.ingestion_stats.clear()
        self.session_start_time = time.time()
        self.logger.info("Metrics statistics have been reset for new session")

# Global metrics collector instance
metrics = MetricsCollector('Development')