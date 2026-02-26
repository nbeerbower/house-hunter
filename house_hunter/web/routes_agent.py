from dataclasses import asdict

from flask import Blueprint, flash, redirect, render_template, request, session, url_for

from house_hunter.web import get_agent, get_db
from house_hunter.config import SearchConfig

agent_bp = Blueprint('agent', __name__)

MAX_CHAT_HISTORY = 20


def _get_chat_history():
    return session.get('chat_history', [])


def _add_to_chat(role, content):
    history = session.get('chat_history', [])
    history.append({'role': role, 'content': content})
    # Keep only last N exchanges
    session['chat_history'] = history[-MAX_CHAT_HISTORY:]


@agent_bp.route('/')
def index():
    db = get_db()
    try:
        ag = get_agent()
        shortlist = ag.get_shortlist(20)
    except Exception:
        shortlist = []

    saved_searches = db.get_saved_searches()

    return render_template('chat.html',
                           shortlist=shortlist,
                           saved_searches=saved_searches,
                           chat_history=_get_chat_history())


@agent_bp.route('/chat', methods=['POST'])
def chat():
    message = request.form.get('message', '').strip()
    if not message:
        return redirect(url_for('agent.index'))

    ag = get_agent()
    _add_to_chat('user', message)

    try:
        response = ag.handle_feedback(message)
    except Exception as e:
        response = f'Error: {e}'

    _add_to_chat('assistant', response)
    return redirect(url_for('agent.index'))


@agent_bp.route('/search', methods=['POST'])
def search():
    location = request.form.get('location', '').strip()
    if not location:
        flash('Location is required.', 'warning')
        return redirect(url_for('agent.index'))

    ag = get_agent()

    # Build search config from agent's existing config + new location
    base = ag.config.search
    search_config = SearchConfig(
        location=location,
        listing_type=base.listing_type,
        price_min=base.price_min,
        price_max=base.price_max,
        beds_min=base.beds_min,
        beds_max=base.beds_max,
        baths_min=base.baths_min,
        baths_max=base.baths_max,
        sqft_min=base.sqft_min,
        sqft_max=base.sqft_max,
        limit=base.limit,
        extra_property_data=base.extra_property_data,
    )

    try:
        new_ids, changed_ids = ag.run_search(search_config)
        msg = f'Search complete for {location}.'
        if new_ids:
            msg += f' {len(new_ids)} new listings.'
        if changed_ids:
            msg += f' {len(changed_ids)} price changes.'
        flash(msg, 'success')
    except Exception as e:
        flash(f'Search failed: {e}', 'warning')

    return redirect(url_for('agent.index'))


@agent_bp.route('/search/save', methods=['POST'])
def save_search():
    name = request.form.get('name', '').strip()
    if not name:
        flash('Search name is required.', 'warning')
        return redirect(url_for('agent.index'))

    ag = get_agent()
    db = get_db()
    config_dict = asdict(ag.config.search)
    db.save_search(name, config_dict)
    flash(f'Saved search "{name}".', 'success')
    return redirect(url_for('agent.index'))


@agent_bp.route('/search/load/<int:search_id>', methods=['POST'])
def load_search(search_id):
    import json
    db = get_db()
    ag = get_agent()

    row = db.load_search(search_id)
    if not row:
        flash('Saved search not found.', 'warning')
        return redirect(url_for('agent.index'))

    config_dict = json.loads(row['config_json'])
    search_config = SearchConfig(**{k: v for k, v in config_dict.items() if k in SearchConfig.__dataclass_fields__})
    ag.config.search = search_config

    try:
        ag.run_search(search_config)
        flash(f'Loaded search "{row["name"]}" ({search_config.location}).', 'success')
    except Exception as e:
        flash(f'Search failed: {e}', 'warning')

    return redirect(url_for('agent.index'))


@agent_bp.route('/search/delete/<int:search_id>', methods=['POST'])
def delete_search(search_id):
    db = get_db()
    if db.delete_search(search_id):
        flash('Saved search deleted.', 'success')
    else:
        flash('Saved search not found.', 'warning')
    return redirect(url_for('agent.index'))


@agent_bp.route('/refresh', methods=['POST'])
def refresh():
    ag = get_agent()
    try:
        ag.handle_feedback('refresh')
        flash('Scores refreshed.', 'success')
    except Exception as e:
        flash(f'Refresh failed: {e}', 'warning')
    return redirect(url_for('agent.index'))
