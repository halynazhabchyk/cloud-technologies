from app.storage.models import Version


def is_newer(incoming: Version, stored: Version | None) -> bool:
    # DEFENSE: LWW – більший timestamp, при рівності більший offset; довіряє годинникам.
    if stored is None:
        return True
    return (incoming.timestamp, incoming.offset) > (stored.timestamp, stored.offset)
