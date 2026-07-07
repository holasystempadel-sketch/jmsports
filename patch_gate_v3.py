#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
patch_gate_v3.py  (System Padel - Komunika)  --  CORRECCIO multi-seed

Corregeix el gate desplegat: el seed dinamic per Liquid ('sp_seed_id') fallava
perque les col-leccions tenen molts productes AGOTADO al principi (el primer
producte disponible no apareixia dins del limit del bucle). Es substitueix per
una llista fixa de 3 productes amb estoc MASSIU (desenes de milers d'unitats);
es posen tots en un sol carret de validacio: els que tenen estoc s'afegeixen,
els esgotats s'ignoren -> applicable distingeix codi real de fals de forma fiable.
Zero manteniment realista (3 productes de 33k-97k u.; cauria nomes si els 3
s'esgotessin o s'esborressin alhora).

Transforma l'estat DESPLEGAT (v3 amb sp_seed_id/SP_SEED) a la versio multi-seed.
Idempotent: si ja hi ha 'SP_SEEDS' al fitxer, no fa res.
Requereix env: SHOPIFY_STORE (default xqksc3-ua) i SHOPIFY_TOKEN (write_themes).
"""

import os
import json
import urllib.request

STORE = os.environ.get("SHOPIFY_STORE", "xqksc3-ua")
TOKEN = os.environ["SHOPIFY_TOKEN"]
API = "2024-10"
THEME_ID = "gid://shopify/OnlineStoreTheme/193671102846"
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

# ---- Anchors (match the currently DEPLOYED v3 gate) -----------------------

# 1) Remove the dynamic Liquid seed + SP_SEED var; use a fixed massive-stock list.
OLD_1 = """      {%- liquid
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
        var SP_SEED = 'gid://shopify/ProductVariant/{{ sp_seed_id }}';"""

NEW_1 = """      <script>
        // Massive-stock seed products (33k-97k units) used only to build a validation cart.
        var SP_SEEDS = ['gid://shopify/ProductVariant/57478671663486','gid://shopify/ProductVariant/57478450348414','gid://shopify/ProductVariant/57478567428478'];"""

# 2) Replace the single-seed validation with a multi-line one.
OLD_2 = """          // Need a seed product to validate a discount against; if none, only master works.
          if (!SP_SEED || SP_SEED.charAt(SP_SEED.length - 1) === '/') {
            err.style.display = 'block';
            return;
          }
          // Validate the typed code against live Shopify discounts (tokenless Storefront API).
          var okCode = false;
          try {
            var q = 'mutation{cartCreate(input:{lines:[{merchandiseId:"' + SP_SEED + '",quantity:1}],discountCodes:[' + JSON.stringify(code) + ']}){cart{discountCodes{code applicable}}}}';"""

NEW_2 = """          // Validate the typed code against live Shopify discounts (tokenless Storefront API).
          // Add all seed products in one cart; in-stock ones are added, out-of-stock ignored.
          var okCode = false;
          try {
            var lines = SP_SEEDS.map(function (v) { return '{merchandiseId:"' + v + '",quantity:1}'; }).join(',');
            var q = 'mutation{cartCreate(input:{lines:[' + lines + '],discountCodes:[' + JSON.stringify(code) + ']}){cart{discountCodes{code applicable}}}}';"""


def main():
    data = gql(READ_Q, {"id": THEME_ID, "names": [FILENAME]})
    nodes = data["theme"]["files"]["nodes"]
    if not nodes:
        raise SystemExit("ERROR: %s not found on theme" % FILENAME)
    content = nodes[0]["body"]["content"]

    if "SP_SEEDS" in content:
        print("SKIP: multi-seed already present (SP_SEEDS found). No changes.")
        return

    with open("backup_theme_liquid_v3b.liquid", "w", encoding="utf-8") as f:
        f.write(content)
    print("Backup written: backup_theme_liquid_v3b.liquid (%d bytes)" % len(content))

    if content.count(OLD_1) != 1:
        raise SystemExit("ERROR: anchor OLD_1 found %d times (expected 1). Aborting." % content.count(OLD_1))
    if content.count(OLD_2) != 1:
        raise SystemExit("ERROR: anchor OLD_2 found %d times (expected 1). Aborting." % content.count(OLD_2))

    new_content = content.replace(OLD_1, NEW_1).replace(OLD_2, NEW_2)

    if new_content == content:
        raise SystemExit("ERROR: no changes applied. Aborting.")
    if "SP_SEEDS" not in new_content or "sp_seed_id" in new_content or "SP_SEED " in new_content:
        raise SystemExit("ERROR: sanity check failed after replace. Aborting.")

    res = gql(WRITE_Q, {
        "themeId": THEME_ID,
        "files": [{"filename": FILENAME, "body": {"type": "TEXT", "value": new_content}}],
    })
    errs = res["themeFilesUpsert"]["userErrors"]
    if errs:
        raise SystemExit("themeFilesUpsert userErrors: %s" % json.dumps(errs, ensure_ascii=False))
    print("OK: %s updated (%d -> %d bytes)." % (FILENAME, len(content), len(new_content)))
    print("Deployed: multi-seed discount-code validation (robust, no stock dependency).")


if __name__ == "__main__":
    main()
