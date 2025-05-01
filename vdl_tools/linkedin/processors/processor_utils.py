def safe_get_attr(element, field_name):
    if not element:
        return None
    
    return getattr(element, field_name, None)

def safe_strip(item: str):
    return item.strip() if item else None


def safe_get_elem(lst, idx):
    if not lst or len(lst) <= idx:
        return None
    return lst[idx]