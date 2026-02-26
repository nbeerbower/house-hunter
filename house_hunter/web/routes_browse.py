import math

from flask import Blueprint, flash, redirect, render_template, request, url_for

from house_hunter.distance import compute_distances, geocode
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

    # Compute distances
    distances = None
    locations = db.get_locations()
    if locations and listing.get('latitude') and listing.get('longitude'):
        distances = compute_distances(listing['latitude'], listing['longitude'], locations)
        distances.sort(key=lambda d: d['priority'], reverse=True)

    # Look up school district (prefer direct FK, fallback to zip)
    district = db.get_district_for_listing(pid)

    return render_template('detail.html',
                           listing=listing,
                           score=score,
                           price_history=price_history,
                           notes=notes,
                           distances=distances,
                           district=district)


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


@browse_bp.route('/locations', methods=['GET', 'POST'])
def locations():
    db = get_db()

    if request.method == 'POST':
        name = request.form.get('name', '').strip()
        priority = request.form.get('priority', '1', type=int)
        if not name:
            flash('Location name is required.', 'warning')
            return redirect(url_for('browse.locations'))

        existing = db.get_locations()
        if len(existing) >= 3:
            flash('Maximum 3 locations allowed. Remove one first.', 'warning')
            return redirect(url_for('browse.locations'))

        # Check geocode cache
        cached = db.get_cached_geocode(name)
        if cached:
            lat, lon = cached
        else:
            result = geocode(name)
            if not result:
                flash(f'Could not geocode "{name}". Try a more specific place name.', 'warning')
                return redirect(url_for('browse.locations'))
            lat, lon = result
            db.cache_geocode(name, lat, lon)

        priority = max(1, min(priority, 3))
        db.add_location(name, lat, lon, priority)
        flash(f'Added location "{name}".', 'success')
        return redirect(url_for('browse.locations'))

    locs = db.get_locations()
    return render_template('locations.html', locations=locs)


@browse_bp.route('/locations/<int:loc_id>/delete', methods=['POST'])
def delete_location(loc_id):
    db = get_db()
    if db.remove_location(loc_id):
        flash('Location removed.', 'success')
    else:
        flash('Location not found.', 'warning')
    return redirect(url_for('browse.locations'))


@browse_bp.route('/rescore', methods=['POST'])
def rescore():
    from house_hunter.web import get_agent
    try:
        ag = get_agent()
        # Load all listings into agent if not already loaded
        if not ag.current_listings:
            db = get_db()
            listings = db.get_all_listings()
            ag.current_listings = {l['property_id']: l for l in listings}
        ag._score_listings(list(ag.current_listings.values()))
        flash(f'Rescored {len(ag.current_listings)} listings.', 'success')
    except Exception as e:
        flash(f'Rescore failed: {e}', 'warning')
    return redirect(url_for('browse.listings'))


@browse_bp.route('/districts', methods=['GET', 'POST'])
def districts():
    db = get_db()

    if request.method == 'POST':
        name = request.form.get('name', '').strip()
        zip_code = request.form.get('zip_code', '').strip()
        state = request.form.get('state', '').strip() or None
        if not name or not zip_code:
            flash('District name and zip code are required.', 'warning')
            return redirect(url_for('browse.districts'))

        db.assign_district_to_zip(name, zip_code, state)
        flash(f'Assigned zip {zip_code} to district "{name}".', 'success')
        return redirect(url_for('browse.districts'))

    all_districts = db.get_all_districts()
    return render_template('districts.html', districts=all_districts)


@browse_bp.route('/districts/<int:district_id>/exclude', methods=['POST'])
def toggle_district_exclusion(district_id):
    db = get_db()
    exclude = request.form.get('exclude', '1') == '1'
    db.exclude_district(district_id, exclude)
    action = 'Excluded' if exclude else 'Included'
    flash(f'{action} district.', 'success')
    return redirect(url_for('browse.districts'))


@browse_bp.route('/districts/<int:district_id>/rate', methods=['POST'])
def rate_district(district_id):
    db = get_db()
    rating = request.form.get('rating', type=int)
    if rating is None or rating < 1 or rating > 10:
        flash('Rating must be between 1 and 10.', 'warning')
        return redirect(url_for('browse.districts'))
    db.set_district_rating(district_id, rating)
    flash(f'District rated {rating}/10.', 'success')
    return redirect(url_for('browse.districts'))


@browse_bp.route('/districts/populate', methods=['POST'])
def populate_districts():
    db = get_db()
    from house_hunter.schools import populate_districts as _populate
    listings = db.get_listings_without_district()
    if not listings:
        flash('All listings already have districts assigned.', 'success')
        return redirect(url_for('browse.districts'))
    result = _populate(db, listings)
    flash(result, 'success')
    return redirect(url_for('browse.districts'))


@browse_bp.route('/districts/fetch-ratings', methods=['POST'])
def fetch_district_ratings():
    db = get_db()
    from house_hunter.schools import fetch_all_ratings
    result = fetch_all_ratings(db)
    flash(result, 'success')
    return redirect(url_for('browse.districts'))


# --- Score Snapshots ---

@browse_bp.route('/snapshots', methods=['GET', 'POST'])
def snapshots():
    db = get_db()

    if request.method == 'POST':
        name = request.form.get('name', '').strip()
        if not name:
            flash('Snapshot name is required.', 'warning')
            return redirect(url_for('browse.snapshots'))
        db.save_snapshot(name)
        flash(f'Saved snapshot "{name}".', 'success')
        return redirect(url_for('browse.snapshots'))

    all_snapshots = db.get_snapshots()
    current_hash = db.get_preferences_hash()

    # Comparison data
    compare_name = request.args.get('compare', '').strip()
    comparison = None
    compare_snap = None
    if compare_name:
        compare_snap = db.get_snapshot(compare_name)
        if compare_snap:
            # Get all listings
            all_listings = db.get_all_listings()
            pids = [l['property_id'] for l in all_listings]
            old_scores = db.get_snapshot_scores(compare_snap['preferences_hash'], pids)
            new_scores = db.get_cached_scores(pids, current_hash)

            comparison = []
            for l in all_listings:
                pid = l['property_id']
                old = old_scores.get(pid, {}).get('score')
                new = new_scores.get(pid, {}).get('score')
                if old is None and new is None:
                    continue
                delta = None
                if old is not None and new is not None:
                    delta = round(new - old, 1)
                comparison.append({
                    'address': l.get('address', pid[:30]),
                    'city': l.get('city', ''),
                    'old_score': old,
                    'new_score': new,
                    'delta': delta,
                })
            # Sort by absolute delta
            comparison.sort(key=lambda r: abs(r['delta']) if r['delta'] is not None else 0, reverse=True)

    return render_template('snapshots.html',
                           snapshots=all_snapshots,
                           current_hash=current_hash,
                           comparison=comparison,
                           compare_snap=compare_snap,
                           compare_name=compare_name)


@browse_bp.route('/snapshots/<int:snapshot_id>/delete', methods=['POST'])
def delete_snapshot(snapshot_id):
    db = get_db()
    if db.delete_snapshot(snapshot_id):
        flash('Snapshot deleted.', 'success')
    else:
        flash('Snapshot not found.', 'warning')
    return redirect(url_for('browse.snapshots'))


@browse_bp.route('/snapshots/<int:snapshot_id>/restore', methods=['POST'])
def restore_snapshot(snapshot_id):
    db = get_db()
    name = db.restore_snapshot(snapshot_id)
    if name:
        flash(f'Restored snapshot "{name}". Preferences, locations, and districts reverted.', 'success')
    else:
        flash('Snapshot not found.', 'warning')
    return redirect(url_for('browse.snapshots'))
