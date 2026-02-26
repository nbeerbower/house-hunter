import math

from flask import Blueprint, flash, redirect, render_template, request, url_for

from house_hunter.web import get_db

browse_bp = Blueprint('browse', __name__)

PER_PAGE = 24


@browse_bp.route('/')
def index():
    return redirect(url_for('browse.listings'))


@browse_bp.route('/listings')
def listings():
    db = get_db()
    page = request.args.get('page', 1, type=int)
    sort = request.args.get('sort', 'score')
    q = request.args.get('q', '').strip()

    all_listings = db.get_all_listings()

    # Text search filter
    if q:
        q_lower = q.lower()
        all_listings = [
            l for l in all_listings
            if q_lower in (l.get('address') or '').lower()
            or q_lower in (l.get('city') or '').lower()
            or q_lower in (l.get('zip_code') or '').lower()
        ]

    # Join with cached scores
    pids = [l['property_id'] for l in all_listings]
    pref_hash = db.get_preferences_hash()
    scores = db.get_cached_scores(pids, pref_hash) if pids else {}

    # Attach scores to listings
    for l in all_listings:
        sc = scores.get(l['property_id'])
        l['score'] = sc['score'] if sc else None
        l['reasoning'] = sc['reasoning'] if sc else None

    # Sort
    if sort == 'score':
        all_listings.sort(key=lambda l: (l['score'] is not None, l['score'] or 0), reverse=True)
    elif sort == 'price':
        all_listings.sort(key=lambda l: l.get('price') or float('inf'))
    elif sort == 'date':
        all_listings.sort(key=lambda l: l.get('list_date') or '', reverse=True)

    total = len(all_listings)
    total_pages = max(1, math.ceil(total / PER_PAGE))
    page = max(1, min(page, total_pages))
    start = (page - 1) * PER_PAGE
    page_listings = all_listings[start:start + PER_PAGE]

    return render_template('listings.html',
                           listings=page_listings,
                           page=page,
                           total_pages=total_pages,
                           total=total,
                           sort=sort,
                           q=q)


@browse_bp.route('/listings/detail')
def detail():
    db = get_db()
    pid = request.args.get('pid', '')
    if not pid:
        flash('No property specified.', 'warning')
        return redirect(url_for('browse.listings'))

    listing = db.get_listing(pid)
    if not listing:
        flash('Listing not found.', 'warning')
        return redirect(url_for('browse.listings'))

    pref_hash = db.get_preferences_hash()
    scores = db.get_cached_scores([pid], pref_hash)
    score = scores.get(pid)

    price_history = db.get_price_history(pid)
    notes = db.get_notes(pid)

    return render_template('detail.html',
                           listing=listing,
                           score=score,
                           price_history=price_history,
                           notes=notes)


@browse_bp.route('/listings/action', methods=['POST'])
def listing_action():
    db = get_db()
    pid = request.form.get('pid', '')
    action = request.form.get('action', '')
    note = request.form.get('note', '').strip() or None

    if not pid or action not in ('favorite', 'reject', 'note'):
        flash('Invalid action.', 'warning')
        return redirect(url_for('browse.listings'))

    if action == 'note' and not note:
        flash('Note text is required.', 'warning')
        return redirect(url_for('browse.detail', pid=pid))

    db.add_action(pid, action, note)

    labels = {'favorite': 'Favorited', 'reject': 'Rejected', 'note': 'Note added to'}
    flash(f'{labels[action]} listing.', 'success')
    return redirect(url_for('browse.detail', pid=pid))


@browse_bp.route('/favorites')
def favorites():
    db = get_db()
    favs = db.get_favorites()

    # Attach scores
    pids = [f['property_id'] for f in favs]
    pref_hash = db.get_preferences_hash()
    scores = db.get_cached_scores(pids, pref_hash) if pids else {}
    for f in favs:
        sc = scores.get(f['property_id'])
        f['score'] = sc['score'] if sc else None

    return render_template('favorites.html', listings=favs)


@browse_bp.route('/preferences', methods=['GET', 'POST'])
def preferences():
    db = get_db()

    if request.method == 'POST':
        text = request.form.get('text', '').strip()
        if text:
            db.add_preference(text)
            flash('Preference added.', 'success')
        return redirect(url_for('browse.preferences'))

    prefs = db.get_active_preferences()
    return render_template('preferences.html', prefs=prefs)


@browse_bp.route('/preferences/<int:pref_id>/delete', methods=['POST'])
def delete_preference(pref_id):
    db = get_db()
    db.deactivate_preference(pref_id)
    flash('Preference removed.', 'success')
    return redirect(url_for('browse.preferences'))
