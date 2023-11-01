"""Instrument Pub task functions.

Usage:
    @instrument_func()
      def func():
          pass

"""
import os
import functools
import logging

from opentelemetry import trace, context
from opentelemetry.trace import Status, StatusCode
from opentelemetry.sdk.resources import Resource, SERVICE_NAME
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
)
from opentelemetry.propagate import set_global_textmap, extract
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry import baggage


propagator = TraceContextTextMapPropagator()
log = logging.getLogger(__name__)


class TracingWrapper:
    """Wrapper class that will wrap all methods of calls with the instrument_tracing decorator."""

    __instance = None

    def __new__(cls):
        """Create a new instance if one does not exist."""
        if not os.getenv("PUB_OTEL_TRACING", "").lower() == "true":
            return None

        if TracingWrapper.__instance is None:
            log.info("Creating TracingWrapper instance")
            cls.__instance = super().__new__(cls)
            otlp_exporter = OTLPSpanExporter(
                endpoint=f"{os.getenv('OTEL_EXPORTER_OTLP_ENDPOINT')}/v1/traces",
            )
            cls.provider = TracerProvider(
                resource=Resource.create({SERVICE_NAME: os.getenv("OTEL_SERVICE_NAME")})
            )
            cls.processor = BatchSpanProcessor(otlp_exporter)
            cls.provider.add_span_processor(cls.processor)
            trace.set_tracer_provider(cls.provider)
            set_global_textmap(propagator)
            cls.tracer = trace.get_tracer(__name__)
        return cls.__instance


tracing_wrapper = TracingWrapper()


def instrument_func(span_name=None, task_info = None, args_to_attr=False):
    """Instrument tracing for a function.

    Args:
        span_name: str
            Span name.
        task_info: dict
            Pub task information, which contains trace context.
        args_to_attr: boolean
            Add function parameters into span attributes

    Returns:
        The decorated function or class
    """
    tracer = trace.get_tracer(__name__)

    def _instrument_func(func):
        @functools.wraps(func)
        def wrap(*args, **kwargs):
            if not os.getenv("PUB_OTEL_TRACING", "").lower() == "true":
                return func(*args, **kwargs)

            links = []
            pub_task_info = task_info
            # Trace context and baggage are extracted from task_info.
            if not context.get_current():
                # If pubd/lib/openshift_task_manager.OpenshiftTaskManager.take_task is instrumented in Pub worker,
                # task_info is fetched from the function parameters.
                if args and len(args) >= 2 and "args" in args[1]:
                    pub_task_info = args[1]

                # If tasks which inherit pubd/tasks/base.PubTaskBase.run are instrumented in Pub task job,
                # task_info is passed to the decorator function directly.
                if pub_task_info:
                    trace_ctx = extract(carrier=pub_task_info["args"])
                    trace_ctx = _add_data_to_trace_baggage(pub_task_info, trace_ctx)
                    context.attach(trace_ctx)
                    # Create a span link which link to push creation span in Pub hub.
                    links = [trace.Link(trace.get_current_span().get_span_context())]

            attributes = {
                "function_name": func.__qualname__,
            }
            if args_to_attr:
                attributes["args"] = ", ".join(map(str, args))
                attributes["kwargs"] = ", ".join("{}={}".format(k, v) for k, v in kwargs.items())

            with tracer.start_as_current_span(
                name=span_name or func.__qualname__,
                links=links,
                attributes=attributes,
            ) as span:
                try:
                    result = func(*args, **kwargs)
                except Exception as exc:
                    span.set_status(Status(StatusCode.ERROR))
                    span.record_exception(exc)
                    raise
                finally:
                    # Add baggage data into span attributes
                    span.set_attributes(baggage.get_all())
                return result

        return wrap

    return _instrument_func

def _add_data_to_trace_baggage(task, trace_ctx):
    """Extract data from Pub task and put into baggage.

    Args:
        span_name: dict
            A Pub task exported data
        trace_ctx: Context
            trace context
    Returns:
        Trace context
    """

    # Put task_id and advisory into baggage
    # TODO: anything else need to put into?
    data = {}
    if "id" in task:
        data["task_id"] = task["id"]
    if task.get("method", "") == "PushAdvisory":
        data["advisory"] = task["args"]["file_list"]

    baggage_data = dict(baggage.get_all(context=trace_ctx))
    baggage_data.update(data)

    return baggage.set_value(baggage._BAGGAGE_KEY, baggage_data, context=trace_ctx)
