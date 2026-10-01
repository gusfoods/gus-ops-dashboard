#!/usr/bin/env python3
"""
Odoo -> Supabase sync for the Gus Foods Ops Dashboard (project evwfqounmubytvxmlinj).

Fills the staging tables the ops_rollup materialized view reads from:
  products, warehouses, sales_order_lines, stock_moves
then calls refresh_ops_rollup().

operation_type derivation (stock.move, state=done):
  Scrap                     dest location is the scrap location (usage=inventory, scrap_location=true)
  Negative Stock Adjustment dest location usage=inventory, not scrap
  Positive Stock Adjustment source location usage=inventory, not scrap
  Delivered to              dest is a warehouse's stock location, picking type internal/incoming

Warehouse attribution: fridge stock location on the move (source for outgoing
scrap/neg-adj, destination for pos-adj/receipts), falling back to move.warehouse_id.

Usage:
  python3 sync_ops_dashboard.py --since 2026-01-01          # backfill
  python3 sync_ops_dashboard.py --days 3                    # daily incremental (overlap-safe: upserts)
  python3 sync_ops_dashboard.py --since 2026-01-01 --dry-run
"""

import argparse
import json
import os
import sys
import time
import urllib.request
import urllib.error
import xmlrpc.client
from datetime import datetime, timedelta, timezone

BATCH_ODOO = 5000
BATCH_SUPABASE = 500


def load_env():
    candidates = [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env.local'),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', '.env.local'),
        '.env.local',
    ]
    for path in candidates:
        if os.path.exists(path):
            for line in open(path):
                line = line.strip()
                if line and '=' in line and not line.startswith('#'):
                    k, v = line.split('=', 1)
                    # .env.local wins over inherited shell env - the shell may export
                    # SUPABASE_* for a different project.
                    os.environ[k] = v
            break
    required = ['ODOO_BASE_URL', 'ODOO_DB_NAME', 'ODOO_USERNAME', 'ODOO_API_KEY',
                'SUPABASE_URL', 'SUPABASE_KEY']
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        sys.exit(f'Missing env vars: {", ".join(missing)}')


class Odoo:
    def __init__(self):
        self.url = os.environ['ODOO_BASE_URL']
        self.db = os.environ['ODOO_DB_NAME']
        self.user = os.environ['ODOO_USERNAME']
        self.key = os.environ['ODOO_API_KEY']
        self.uid = xmlrpc.client.ServerProxy(f'{self.url}/xmlrpc/2/common').authenticate(
            self.db, self.user, self.key, {})
        if not self.uid:
            sys.exit('Odoo authentication failed')
        self.models = xmlrpc.client.ServerProxy(f'{self.url}/xmlrpc/2/object')

    def call(self, model, method, args, **kwargs):
        for attempt in range(4):
            try:
                return self.models.execute_kw(self.db, self.uid, self.key, model, method, args, kwargs)
            except Exception as e:
                if attempt == 3:
                    raise
                wait = 5 * (attempt + 1)
                log(f'  Odoo call failed ({e.__class__.__name__}), retrying in {wait}s...')
                time.sleep(wait)

    def read_all(self, model, domain, fields, order='id', context=None):
        """Paginate search_read until exhausted."""
        out, offset = [], 0
        while True:
            rows = self.call(model, 'search_read', [domain],
                             fields=fields, limit=BATCH_ODOO, offset=offset, order=order,
                             **({'context': context} if context else {}))
            out.extend(rows)
            log(f'  {model}: fetched {len(out)} rows')
            if len(rows) < BATCH_ODOO:
                return out
            offset += BATCH_ODOO


class Supabase:
    def __init__(self, dry_run=False):
        self.base = os.environ['SUPABASE_URL'].rstrip('/')
        # Writes require the service_role key (RLS blocks anon, by design).
        self.key = os.environ.get('SUPABASE_SERVICE_KEY') or os.environ['SUPABASE_KEY']
        if not os.environ.get('SUPABASE_SERVICE_KEY'):
            log('WARNING: SUPABASE_SERVICE_KEY not set - writes will fail with the anon key')
        self.dry_run = dry_run

    def _request(self, method, path, payload=None, headers=None):
        req = urllib.request.Request(self.base + path, method=method)
        req.add_header('apikey', self.key)
        req.add_header('Authorization', f'Bearer {self.key}')
        req.add_header('Content-Type', 'application/json')
        for k, v in (headers or {}).items():
            req.add_header(k, v)
        body = json.dumps(payload).encode() if payload is not None else None
        for attempt in range(4):
            try:
                with urllib.request.urlopen(req, body, timeout=120) as res:
                    return res.read().decode()
            except urllib.error.HTTPError as e:
                detail = e.read().decode()[:300]
                raise RuntimeError(f'{method} {path} -> HTTP {e.code}: {detail}') from None
            except Exception:
                if attempt == 3:
                    raise
                time.sleep(5 * (attempt + 1))

    def upsert(self, table, rows, on_conflict):
        if not rows:
            return 0
        if self.dry_run:
            log(f'  [dry-run] would upsert {len(rows)} rows into {table}')
            return len(rows)
        total = 0
        for i in range(0, len(rows), BATCH_SUPABASE):
            chunk = rows[i:i + BATCH_SUPABASE]
            self._request('POST', f'/rest/v1/{table}?on_conflict={on_conflict}', chunk,
                          {'Prefer': 'resolution=merge-duplicates,return=minimal'})
            total += len(chunk)
            if total % 5000 < BATCH_SUPABASE:
                log(f'  {table}: upserted {total}/{len(rows)}')
        log(f'  {table}: upserted {total} rows')
        return total

    def insert_ignore(self, table, rows, on_conflict):
        """Insert rows, silently skipping ones whose key already exists (never overwrites)."""
        if not rows:
            return 0
        if self.dry_run:
            log(f'  [dry-run] would insert-ignore {len(rows)} rows into {table}')
            return len(rows)
        for i in range(0, len(rows), BATCH_SUPABASE):
            self._request('POST', f'/rest/v1/{table}?on_conflict={on_conflict}',
                          rows[i:i + BATCH_SUPABASE],
                          {'Prefer': 'resolution=ignore-duplicates,return=minimal'})
        log(f'  {table}: insert-ignored {len(rows)} rows')
        return len(rows)

    def rpc(self, fn):
        if self.dry_run:
            log(f'  [dry-run] would call rpc/{fn}')
            return
        self._request('POST', f'/rest/v1/rpc/{fn}', {})


def log(msg):
    print(f'[{datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")}Z] {msg}', flush=True)


def clean(v):
    return None if v is False else v


def m2o_name(v):
    return v[1] if isinstance(v, (list, tuple)) and len(v) > 1 else None


def sync_products(odoo, sb, now_iso):
    log('Syncing products...')
    # active_test False: include archived products so historical sale lines keep their
    # reference and category (otherwise ~5% of lines fall outside the catalog).
    rows = odoo.read_all('product.product', [],
                         ['default_code', 'name', 'barcode', 'standard_price', 'list_price',
                          'categ_id', 'type'],
                         context={'active_test': False})
    out = []
    for p in rows:
        if not p.get('default_code'):
            continue
        out.append({
            'internal_reference': p['default_code'],
            'name': clean(p.get('name')),
            'barcode': clean(p.get('barcode')),
            'cost': p.get('standard_price') or 0,
            'sales_price': p.get('list_price') or 0,
            'product_category': m2o_name(p.get('categ_id')),
            'stock_non_stock': p.get('type') == 'product' if 'type' in p else None,
            'synced_at': now_iso,
        })
    sb.upsert('products', out, 'internal_reference')
    return len(out)


def commercial_partners(odoo, partner_ids):
    """Map partner id -> (commercial_partner_id, commercial name) in one batched read."""
    ids = sorted({p for p in partner_ids if p})
    if not ids:
        return {}
    rows = odoo.call('res.partner', 'read', [ids], fields=['commercial_partner_id'])
    return {r['id']: (r['commercial_partner_id'][0], r['commercial_partner_id'][1])
            for r in rows if isinstance(r.get('commercial_partner_id'), (list, tuple))}


def sync_warehouses(odoo, sb, now_iso):
    log('Syncing warehouses...')
    rows = odoo.read_all('stock.warehouse', [], ['name', 'code', 'lot_stock_id', 'partner_id'])
    # Fridge -> B2B company: warehouse partner normalized to its commercial (company) partner.
    comm = commercial_partners(odoo, [w['partner_id'][0] for w in rows
                                      if isinstance(w.get('partner_id'), (list, tuple))])
    out, stock_loc_to_wh, companies = [], {}, {}
    for w in rows:
        loc_name = m2o_name(w.get('lot_stock_id'))
        pid = w['partner_id'][0] if isinstance(w.get('partner_id'), (list, tuple)) else None
        cpid, cname = comm.get(pid, (None, None))
        out.append({
            'complete_name': w['name'],
            'warehouse_name': w['name'],
            'warehouse_code': clean(w.get('code')),
            'location_name': loc_name,
            'partner_id': cpid,
            'customer_name': cname,
            'synced_at': now_iso,
        })
        if cpid:
            companies[cpid] = cname
        if isinstance(w.get('lot_stock_id'), (list, tuple)):
            stock_loc_to_wh[w['lot_stock_id'][0]] = w['name']
    sb.upsert('warehouses', out, 'complete_name')
    sb.upsert('b2b_customers', [{'partner_id': k, 'name': v} for k, v in companies.items()],
              'partner_id')
    return stock_loc_to_wh


INVOICE_JOURNALS = ['FEES', 'ROOM']


def sync_invoice_lines(odoo, sb, since, now_iso, until=None):
    """Customer invoice lines on the subscription-fee and room-service journals.
    Credit notes are stored with negative amounts."""
    log(f'Syncing invoice lines ({"/".join(INVOICE_JOURNALS)}) since {since}'
        + (f' until {until}' if until else '') + '...')
    domain = [('journal_id.code', 'in', INVOICE_JOURNALS),
              ('display_type', '=', 'product'),
              ('parent_state', '=', 'posted'),
              ('move_id.move_type', 'in', ['out_invoice', 'out_refund']),
              ('date', '>=', since)]
    if until:
        domain.append(('date', '<', until))
    lines = odoo.read_all('account.move.line', domain,
                          ['move_id', 'journal_id', 'partner_id', 'date', 'name',
                           'product_id', 'quantity', 'price_subtotal', 'analytic_distribution'])
    if not lines:
        log('  no invoice lines in range')
        return 0
    move_ids = sorted({l['move_id'][0] for l in lines if isinstance(l.get('move_id'), (list, tuple))})
    moves = {}
    for i in range(0, len(move_ids), BATCH_ODOO):
        for m in odoo.call('account.move', 'read', [move_ids[i:i + BATCH_ODOO]],
                           fields=['name', 'move_type', 'commercial_partner_id', 'partner_shipping_id',
                                   'invoice_date', 'date']):
            moves[m['id']] = m
    # Analytic accounts: distribution keys are analytic account ids -> resolve names once.
    ana_ids = set()
    for l in lines:
        for k in (l.get('analytic_distribution') or {}):
            ana_ids.add(int(k))
    ana_names = {}
    if ana_ids:
        for a in odoo.call('account.analytic.account', 'read', [sorted(ana_ids)], fields=['name']):
            ana_names[a['id']] = a['name']
    out = []
    for l in lines:
        mv = moves.get(l['move_id'][0] if isinstance(l.get('move_id'), (list, tuple)) else None, {})
        move_type = mv.get('move_type') or 'out_invoice'
        sign = -1 if move_type == 'out_refund' else 1
        jname = m2o_name(l.get('journal_id')) or ''
        code = 'FEES' if 'Subscription' in jname else ('ROOM' if 'Room' in jname else jname[:8])
        dist = l.get('analytic_distribution') or {}
        top_ana = max(dist, key=dist.get) if dist else None
        cp = mv.get('commercial_partner_id')
        ship = mv.get('partner_shipping_id')
        out.append({
            'ship_partner_id': ship[0] if isinstance(ship, (list, tuple)) else None,
            'ship_name': ship[1] if isinstance(ship, (list, tuple)) else None,
            'id': l['id'],
            'move_id': mv.get('id') or (l['move_id'][0] if isinstance(l.get('move_id'), (list, tuple)) else None),
            'move_name': clean(mv.get('name')),
            'journal_code': code,
            'move_type': move_type,
            'partner_id': cp[0] if isinstance(cp, (list, tuple)) else None,
            'invoice_date': mv.get('invoice_date') or l.get('date'),
            'line_name': clean(l.get('name')),
            'product_name': m2o_name(l.get('product_id')),
            'quantity': l.get('quantity') or 0,
            'amount': sign * (l.get('price_subtotal') or 0),
            'analytic': ana_names.get(int(top_ana)) if top_ana else None,
            'synced_at': now_iso,
        })
    # Companies seen on invoices may not have a fridge yet - keep b2b_customers complete.
    comps = {}
    for m in moves.values():
        cp = m.get('commercial_partner_id')
        if isinstance(cp, (list, tuple)):
            comps[cp[0]] = cp[1]
    sb.upsert('b2b_customers', [{'partner_id': k, 'name': v} for k, v in comps.items()], 'partner_id')
    # New ship-to addresses land unmapped in location_map (site=null) so they surface in the
    # dashboard's "(Unmapped)" bucket; existing mappings are never overwritten.
    ship_partners = {}
    for m in moves.values():
        sp = m.get('partner_shipping_id')
        if isinstance(sp, (list, tuple)):
            ship_partners[sp[0]] = sp[1]
    sb.insert_ignore('location_map',
                     [{'ship_partner_id': k, 'ship_name': v} for k, v in ship_partners.items()],
                     'ship_partner_id')
    return sb.upsert('invoice_lines', out, 'id')


def sync_sales(odoo, sb, since, now_iso, until=None):
    log(f'Syncing sales order lines since {since}' + (f' until {until}' if until else '') + '...')
    domain = [('order_id.date_order', '>=', since),
              ('order_id.state', 'in', ['sale', 'done'])]
    if until:
        domain.append(('order_id.date_order', '<', until))
    lines = odoo.read_all('sale.order.line', domain,
                          ['order_id', 'product_id', 'qty_delivered', 'price_unit',
                           'discount', 'price_subtotal', 'price_total'])
    order_ids = sorted({l['order_id'][0] for l in lines if l.get('order_id')})
    log(f'  reading {len(order_ids)} parent orders...')
    orders = {}
    for i in range(0, len(order_ids), BATCH_ODOO):
        for o in odoo.call('sale.order', 'read', [order_ids[i:i + BATCH_ODOO]],
                           fields=['date_order', 'warehouse_id', 'partner_id']):
            orders[o['id']] = o
    out = []
    for l in lines:
        o = orders.get(l['order_id'][0]) if l.get('order_id') else None
        if not o:
            continue
        product_disp = m2o_name(l.get('product_id')) or ''
        out.append({
            'id': l['id'],
            'order_id': l['order_id'][0],
            # Pseudonymous: only the numeric partner id is synced, never names/emails.
            'buyer_id': o['partner_id'][0] if isinstance(o.get('partner_id'), (list, tuple)) else None,
            'order_at': (o['date_order'] or '').replace(' ', 'T') + 'Z' if o.get('date_order') else None,
            'warehouse_name': m2o_name(o.get('warehouse_id')),
            'product_name': product_disp,
            'qty_delivered': l.get('qty_delivered') or 0,
            'price_unit': l.get('price_unit') or 0,
            'discount': l.get('discount') or 0,
            'price_subtotal': l.get('price_subtotal') or 0,
            'price_total': l.get('price_total') or 0,
            'synced_at': now_iso,
        })
    sb.upsert('sales_order_lines', out, 'id')
    return len(out)


def sync_stock_moves(odoo, sb, since, now_iso, stock_loc_to_wh, until=None):
    log(f'Syncing stock moves since {since}' + (f' until {until}' if until else '') + '...')
    inv_locs = odoo.call('stock.location', 'search_read', [[('usage', '=', 'inventory')]],
                         fields=['scrap_location'])
    scrap_ids = [l['id'] for l in inv_locs if l.get('scrap_location')]
    adj_ids = [l['id'] for l in inv_locs if not l.get('scrap_location')]
    fridge_stock_ids = list(stock_loc_to_wh.keys())

    fields = ['date', 'product_id', 'quantity', 'location_id', 'location_dest_id',
              'reference', 'warehouse_id']
    base = [('state', '=', 'done'), ('date', '>=', since)]
    if until:
        base = base + [('date', '<', until)]
    categories = [
        ('Scrap',                     base + [('location_dest_id', 'in', scrap_ids)],  'src'),
        ('Negative Stock Adjustment', base + [('location_dest_id', 'in', adj_ids)],    'src'),
        ('Positive Stock Adjustment', base + [('location_id', 'in', adj_ids)],         'dest'),
                ('Delivered to',              base + [('location_dest_id', 'in', fridge_stock_ids),
                                              ('location_id.usage', 'in', ['supplier', 'internal', 'transit'])], 'dest'),
    ]

    total = 0
    for op_type, domain, wh_side in categories:
        log(f'  category: {op_type}')
        moves = odoo.read_all('stock.move', domain, fields)
        out = []
        for mv in moves:
            product_disp = m2o_name(mv.get('product_id')) or ''
            side = mv.get('location_id') if wh_side == 'src' else mv.get('location_dest_id')
            wh = None
            if isinstance(side, (list, tuple)):
                wh = stock_loc_to_wh.get(side[0])
            if not wh:
                wh = m2o_name(mv.get('warehouse_id'))
            out.append({
                'id': mv['id'],
                'product_name': product_disp,
                'qty': mv.get('quantity') or 0,
                'moved_at': (mv['date'] or '').replace(' ', 'T') + 'Z' if mv.get('date') else None,
                'location_name': m2o_name(mv.get('location_id')),
                'location_dest_name': m2o_name(mv.get('location_dest_id')),
                'reference': clean(mv.get('reference')),
                'operation_type': op_type,
                'warehouse': wh,
                'synced_at': now_iso,
            })
        total += sb.upsert('stock_moves', out, 'id')
    return total


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--since', help='sync data from this date (YYYY-MM-DD)')
    ap.add_argument('--until', help='sync data before this date (YYYY-MM-DD, exclusive) - for chunked backfills')
    ap.add_argument('--days', type=int, help='sync the last N days (for scheduled runs)')
    ap.add_argument('--no-refresh', action='store_true', help='skip the rollup refresh (refresh once after the last chunk)')
    ap.add_argument('--invoices-only', action='store_true', help='only sync invoice lines (skip products/warehouses/sales/moves)')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()
    if not args.since and not args.days:
        ap.error('pass --since YYYY-MM-DD or --days N')
    since = args.since or (datetime.now(timezone.utc) - timedelta(days=args.days)).strftime('%Y-%m-%d')

    load_env()
    now_iso = datetime.now(timezone.utc).isoformat()
    odoo = Odoo()
    sb = Supabase(dry_run=args.dry_run)
    log(f'Connected to Odoo (uid {odoo.uid}); syncing since {since}')

    if args.invoices_only:
        n_products = n_sales = n_moves = 0
    else:
        n_products = sync_products(odoo, sb, now_iso)
        stock_loc_to_wh = sync_warehouses(odoo, sb, now_iso)
        n_sales = sync_sales(odoo, sb, since, now_iso, args.until)
        n_moves = sync_stock_moves(odoo, sb, since, now_iso, stock_loc_to_wh, args.until)
    n_inv = sync_invoice_lines(odoo, sb, since, now_iso, args.until)

    if args.no_refresh:
        log('Skipping rollup refresh (--no-refresh)')
    else:
        # The refresh can lose a lock race (e.g. autovacuum) - retry before failing the run.
        for attempt in range(3):
            try:
                log('Refreshing materialized views' + (f' (retry {attempt})' if attempt else '') + '...')
                sb.rpc('refresh_ops_rollup')
                break
            except Exception as e:
                if attempt == 2:
                    raise
                log(f'Refresh failed ({e}); retrying in 90s')
                time.sleep(90)
    log(f'Done. products={n_products} sales_lines={n_sales} stock_moves={n_moves} invoice_lines={n_inv}')


if __name__ == '__main__':
    main()
