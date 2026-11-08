from fastapi import Request

from coordinator.app.services.coordinator import Coordinator


def get_coordinator(request: Request) -> Coordinator:
    return request.app.state.coordinator
