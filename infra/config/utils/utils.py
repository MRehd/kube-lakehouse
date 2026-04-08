def _deep_merge(base: dict, override: dict) -> dict:
    '''Recursively merge override into base, with override taking precedence.'''
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result
