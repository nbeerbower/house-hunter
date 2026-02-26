from urllib.parse import quote

from flask import Flask


def register_filters(app: Flask):
    """Register all custom Jinja template filters."""

    @app.template_filter('currency')
    def currency_filter(value):
        if value is None:
            return 'N/A'
        try:
            return f'${float(value):,.0f}'
        except (ValueError, TypeError):
            return 'N/A'

    @app.template_filter('score_pct')
    def score_pct_filter(value):
        """Convert 0-10 score to 0-100 percentage."""
        if value is None:
            return 0
        try:
            return int(float(value) * 10)
        except (ValueError, TypeError):
            return 0

    @app.template_filter('score_color')
    def score_color_filter(value):
        """Return a color class based on score (0-10)."""
        if value is None:
            return 'poor'
        try:
            s = float(value)
        except (ValueError, TypeError):
            return 'poor'
        if s >= 8:
            return 'excellent'
        if s >= 6:
            return 'good'
        if s >= 4:
            return 'fair'
        return 'poor'

    @app.template_filter('short_date')
    def short_date_filter(value):
        if not value:
            return ''
        return str(value)[:10]

    @app.template_filter('format_beds')
    def format_beds_filter(value):
        if value is None:
            return '?'
        try:
            return f'{int(float(value))}'
        except (ValueError, TypeError):
            return '?'

    @app.template_filter('format_baths')
    def format_baths_filter(value):
        if value is None:
            return '?'
        try:
            v = float(value)
            return f'{int(v)}' if v == int(v) else f'{v:.1f}'
        except (ValueError, TypeError):
            return '?'

    @app.template_filter('format_sqft')
    def format_sqft_filter(value):
        if value is None:
            return '?'
        try:
            return f'{int(float(value)):,}'
        except (ValueError, TypeError):
            return '?'

    @app.template_filter('format_lot')
    def format_lot_filter(value):
        """Format lot size: show acres if >= 1 acre (43560 sqft), else sqft."""
        if value is None:
            return 'N/A'
        try:
            sqft = float(value)
            if sqft >= 43560:
                acres = sqft / 43560
                return f'{acres:.2f} acres'
            return f'{int(sqft):,} sqft'
        except (ValueError, TypeError):
            return 'N/A'

    @app.template_filter('truncate_desc')
    def truncate_desc_filter(value, length=200):
        if not value:
            return ''
        if len(value) <= length:
            return value
        return value[:length].rsplit(' ', 1)[0] + '...'

    @app.template_filter('urlencode')
    def urlencode_filter(value):
        if not value:
            return ''
        return quote(str(value), safe='')
