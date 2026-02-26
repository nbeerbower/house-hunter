import os

from flask import Flask

from house_hunter.config import AppConfig, LLMConfig, SearchConfig
from house_hunter.db import Database


def get_db() -> Database:
    """Get the shared Database instance from the app."""
    from flask import current_app
    if not hasattr(current_app, '_db') or current_app._db is None:
        current_app._db = Database(current_app.config['DB_PATH'])
    return current_app._db


def get_agent():
    """Get the shared Agent instance (lazy, requires LLM)."""
    from flask import current_app
    if not hasattr(current_app, '_agent') or current_app._agent is None:
        from house_hunter.agent import Agent
        current_app._agent = Agent(current_app.config['APP_CONFIG'])
    return current_app._agent


def create_app(app_config: AppConfig | None = None) -> Flask:
    """Flask application factory."""
    app = Flask(__name__)
    app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'house-hunter-dev-key')

    if app_config is None:
        llm_config = LLMConfig.from_env()
        app_config = AppConfig(llm=llm_config)

    app.config['APP_CONFIG'] = app_config
    app.config['DB_PATH'] = app_config.db_path

    # Initialize as None — lazy creation
    app._db = None
    app._agent = None

    # Register Jinja filters
    from house_hunter.web.filters import register_filters
    register_filters(app)

    # Register blueprints
    from house_hunter.web.routes_browse import browse_bp
    from house_hunter.web.routes_agent import agent_bp
    app.register_blueprint(browse_bp)
    app.register_blueprint(agent_bp, url_prefix='/agent')

    @app.teardown_appcontext
    def close_db(exception):
        db = getattr(app, '_db', None)
        if db is not None:
            db.close()
            app._db = None

    return app
