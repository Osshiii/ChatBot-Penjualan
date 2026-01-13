def format_number(value, decimals=0, thousands_sep=True):
    """Format number with proper locale separators."""
    if value is None:
        return "0"
    
    try:
        val = float(value)
        if thousands_sep:
            # Format with thousands separator (use dot, then replace for output)
            formatted = f"{val:,.{decimals}f}"
            print(f"Input: {val}, decimals: {decimals}")
            print(f"Formatted with comma: '{formatted}'")
            # Replace . with , and , with . for Indonesian/European format
            parts = formatted.split(',')
            print(f"Parts after split by comma: {parts}")
            if len(parts) > 1:
                result = parts[0].replace(',', '.') + ',' + parts[-1]
                print(f"Result: '{result}'")
                return result
            return formatted
        else:
            return f"{val:,.{decimals}f}".replace(',', '.')
    except (ValueError, TypeError):
        return str(value)

# Test
result = format_number(22506, decimals=0, thousands_sep=True)
print(f"Final: {result}")
print(f"Expected: 22.506")
