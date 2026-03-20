def detect_device_type(user_agent: str | None) -> str:
    ua = (user_agent or "").lower()
    if "iphone" in ua:
        return "iphone"
    if "ipad" in ua:
        return "ipad"
    if "android" in ua and "mobile" in ua:
        return "android"
    if "android" in ua:
        return "tablet"
    if "windows" in ua or "macintosh" in ua or "linux" in ua:
        return "desktop"
    return "unknown"
