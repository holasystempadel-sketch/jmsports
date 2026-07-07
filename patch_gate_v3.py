#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
patch_gate_v3.py  (System Padel - Komunika)

Actualitza el gate del Catalogo profesional a layout/theme.liquid (tema MAIN):

  1) VALIDACIO DINAMICA PER CODI DE DESCOMPTE
     - spCheck() valida el codi escrit contra els descomptes VIUS de Shopify
       via Storefront API tokenless (cartCreate + discountCodes). Si
       applicable == true -> obre + aplica el descompte al carret real
       (/discount/CODE) i aterra desbloquejat.
     - Qualsevol codi de descompte nou creat a l'admin funciona sol.
     - El codi mestre 'systempadel2026' segueix funcionant com a alternativa.

  2) SEED DINAMIC (sense manteniment)
     - La validacio necessita un producte al carret. En lloc d'un producte
       fix, el Liquid tria en el moment de renderitzar el primer producte
       DISPONIBLE de la col-leccio 'padel' (fallback 'equipamiento'). Aixi
       mai depen de l'estoc d'un producte concret.

  3) LOGO CENTRAT
     - L'<img> del logo es block (reset del tema) dins d'un contenidor
       text-align:center -> no es centra. S'afegeix display:block;margin:0 auto.

Idempotent: si ja hi ha 'cartCreate' al fitxer, no fa res.
Requereix env: SHOPIFY_STORE (default xqksc3-ua) i SHOPIFY_TOKEN (write_themes).
"""

import os
import json
import urllib.request

STORE = os.environ.get("SHOPIFY_STORE", "xqksc3-ua")
TOKEN = os.environ["SHOPIFY_TOKEN"]
API = "2024-10"
THEME_ID = "gid://shopify/OnlineStoreTheme/193671102846"  # Copia de systempadel-oficial (MAIN)
FILENAME = "layout/theme.liquid"

ENDPOINT = "https://%s.myshopify.com/admin/api/%s/graphql.json" % (STORE, API)


def gql(query, variables=None):
    payload = json.dumps({"query": query, "variables": variables or {}}).encode("utf-8")
    req = urllib.request.Request(
        ENDPOINT,
        data=payload,
        headers={"Content-Type": "application/json", "X-Shopify-Access-Token": TOKEN},
        method="POST",
    )
    with urllib.request.urlopen(req) as r:
        data = json.loads(r.read().decode("utf-8"))
    if data.get("errors"):
        raise SystemExit("GraphQL errors: %s" % json.dumps(data["errors"], ensure_ascii=False))
    return data["data"]


READ_Q = """
query($id: ID!, $names: [String!]!) {
  theme(id: $id) {
    files(filenames: $names, first: 1) {
      nodes { filename body { ... on OnlineStoreThemeFileBodyText { content } } }
    }
  }
}
"""

WRITE_Q = """
mutation($themeId: ID!, $files: [OnlineStoreThemeFilesUpsertFileInput!]!) {
  themeFilesUpsert(themeId: $themeId, files: $files) {
    upsertedThemeFiles { filename }
    userErrors { filename code message }
  }
}
"""

# ---- Anchors (exact match against current theme.liquid) --------------------

OLD_LOGO = (
    '<img src="https://cdn.shopify.com/s/files/1/0983/7209/2286/files/'
    'logotip_systempadel.png?v=1774447505" alt="System Padel" '
    'style="max-height:60px;width:auto;">'
)
NEW_LOGO = (
    '<img src="https://cdn.shopify.com/s/files/1/0983/7209/2286/files/'
    'logotip_systempadel.png?v=1774447505" alt="System Padel" '
    'style="max-height:60px;width:auto;display:block;margin:0 auto;">'
)

# Anchor starts at the <script> line and covers ONLY the spCheck() function
# (spToggle + keypress listener are left untouched after it).
OLD_ANCHOR = """      <script>
        function spCheck() {
          var code = document.getElementById('sp-input').value;
          if (code === 'systempadel2026') {
            fetch('/cart/update.js', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ attributes: { sp_access: 'systempadel2026', sp_access_ts: String(Math.floor(Date.now()/1000)) } })
            }).then(function(){ window.location.reload(); });
          } else {
            document.getElementById('sp-error').style.display = 'block';
          }
        }"""

NEW_ANCHOR = """      {%- liquid
        assign sp_seed_id = ''
        for p in collections['padel'].products
          if p.available
            assign sp_seed_id = p.selected_or_first_available_variant.id
            break
          endif
        endfor
        if sp_seed_id == blank
          for p in collections['equipamiento'].products
            if p.available
              assign sp_seed_id = p.selected_or_first_available_variant.id
              break
            endif
          endfor
        endif
      -%}
      <script>
        var SP_SEED = 'gid://shopify/ProductVariant/{{ sp_seed_id }}';
        function spUnlock() {
          return fetch('/cart/update.js', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ attributes: { sp_access: 'systempadel2026', sp_access_ts: String(Math.floor(Date.now()/1000)) } })
          });
        }
        async function spCheck() {
          var input = document.getElementById('sp-input');
          var err = document.getElementById('sp-error');
          var code = (input.value || '').trim();
          err.style.display = 'none';
          if (!code) { return; }
          // Master code keeps working as a fallback.
          if (code === 'systempadel2026') {
            try { await spUnlock(); } catch (e) {}
            window.location.reload();
            return;
          }
          // Need a seed product to validate a discount against; if none, only master works.
          if (!SP_SEED || SP_SEED.charAt(SP_SEED.length - 1) === '/') {
            err.style.display = 'block';
            return;
          }
          // Validate the typed code against live Shopify discounts (tokenless Storefront API).
          var okCode = false;
          try {
            var q = 'mutation{cartCreate(input:{lines:[{merchandiseId:"' + SP_SEED + '",quantity:1}],discountCodes:[' + JSON.stringify(code) + ']}){cart{discountCodes{code applicable}}}}';
            var r = await fetch('/api/2025-07/graphql.json', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ query: q })
            });
            if (r.ok) {
              var j = await r.json();
              if (!j.errors) {
                var dcs = (((j.data || {}).cartCreate || {}).cart || {}).discountCodes || [];
                okCode = dcs.some(function (d) { return d.applicable; });
              }
            }
          } catch (e) {}
          if (!okCode) { err.style.display = 'block'; return; }
          // Valid discount code -> unlock (existing mechanism) then apply the discount
          // to the real cart and land unlocked.
          try { await spUnlock(); } catch (e) {}
          window.location.href = '/discount/' + encodeURIComponent(code) + '?redirect=/pages/tienda-online';
        }"""


def main():
    data = gql(READ_Q, {"id": THEME_ID, "names": [FILENAME]})
    nodes = data["theme"]["files"]["nodes"]
    if not nodes:
        raise SystemExit("ERROR: %s not found on theme" % FILENAME)
    content = nodes[0]["body"]["content"]

    if "cartCreate" in content:
        print("SKIP: gate v3 already present (cartCreate found). No changes.")
        return

    with open("backup_theme_liquid_v3.liquid", "w", encoding="utf-8") as f:
        f.write(content)
    print("Backup written: backup_theme_liquid_v3.liquid (%d bytes)" % len(content))

    n_logo = content.count(OLD_LOGO)
    n_anchor = content.count(OLD_ANCHOR)
    if n_logo != 1:
        raise SystemExit("ERROR: logo anchor found %d times (expected 1). Aborting." % n_logo)
    if n_anchor != 1:
        raise SystemExit("ERROR: spCheck anchor found %d times (expected 1). Aborting." % n_anchor)

    new_content = content.replace(OLD_LOGO, NEW_LOGO).replace(OLD_ANCHOR, NEW_ANCHOR)

    if new_content == content:
        raise SystemExit("ERROR: no changes applied. Aborting.")
    for token in ("cartCreate", "display:block;margin:0 auto", "sp_seed_id", "SP_SEED"):
        if token not in new_content:
            raise SystemExit("ERROR: sanity check failed (missing %r). Aborting." % token)

    res = gql(WRITE_Q, {
        "themeId": THEME_ID,
        "files": [{"filename": FILENAME, "body": {"type": "TEXT", "value": new_content}}],
    })
    errs = res["themeFilesUpsert"]["userErrors"]
    if errs:
        raise SystemExit("themeFilesUpsert userErrors: %s" % json.dumps(errs, ensure_ascii=False))
    print("OK: %s updated (%d -> %d bytes)." % (FILENAME, len(content), len(new_content)))
    print("Deployed: dynamic discount-code gate (dynamic seed) + centered logo.")


if __name__ == "__main__":
    main()
