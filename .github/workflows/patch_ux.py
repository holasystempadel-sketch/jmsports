#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
patch_ux.py  (System Padel - Komunika)

2 millores UX al tema (peticions client 20-jul):
 1) FITXA DE PRODUCTE: boto "Anadir al carrito" TAMBE al final de la fitxa
    + barra STICKY inferior en mobil que sempre mostra el boto de compra.
    Mecanisme: es localitza el boto real del formulari principal (selectors
    adaptatius, s'exclouen els quick-add de les graelles) i (a) se'n crea un
    duplicat visual al final de la seccio del producte, (b) es crea una barra
    fixa inferior nomes en mobil. Tots dos fan CLICK PROXY al boto original ->
    la variant seleccionada i la logica del tema queden intactes.
 2) CARRET: textos en NEGRE (#111) en lloc del gris actual. CSS limitat a la
    pagina del carret i al drawer; s'exclouen botons i inputs per no trencar
    el boto "Pagar".

Idempotent: si 'sp-ux' ja es al fitxer, no fa res.
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

SNIPPET = """
  <!-- sp-ux: boto compra duplicat+sticky a producte / textos carret en negre (Komunika) -->
  <style>
    /* 2) Carret: textos en negre (paginas i drawer); fora botons/inputs */
    .cart-page :not(button, .button, button *, .button *, input, select, textarea, svg, svg *),
    cart-items-component :not(button, .button, button *, .button *, input, select, textarea, svg, svg *),
    cart-drawer-component :not(button, .button, button *, .button *, input, select, textarea, svg, svg *) {
      color: #111 !important;
    }
    /* 1) Boto duplicat al final de la fitxa */
    .sp-buy-again { margin: 18px 0 6px; }
    .sp-buy-again button, .sp-sticky-bar button {
      width: 100%; padding: 14px 18px; border: 0; border-radius: 8px;
      background: #111; color: #fff; font-size: 15px; font-weight: 600;
      cursor: pointer;
    }
    /* 1b) Barra sticky inferior (nomes mobil) */
    .sp-sticky-bar {
      display: none; position: fixed; left: 0; right: 0; bottom: 0; z-index: 990;
      padding: 10px 14px calc(10px + env(safe-area-inset-bottom));
      background: rgba(255,255,255,.97); border-top: 1px solid #e5e5e5;
      box-shadow: 0 -4px 14px rgba(0,0,0,.08);
    }
    .sp-sticky-bar.sp-on { display: block; }
    @media (min-width: 750px) { .sp-sticky-bar { display: none !important; } }
  </style>
  <script>
  (function () {
    function mainAddBtn() {
      var forms = document.querySelectorAll('form[action*="/cart/add"]');
      for (var i = 0; i < forms.length; i++) {
        var f = forms[i];
        if (f.closest('.quick-add') || f.closest('[class*="quick-add"]')) continue;
        var b = f.querySelector('button[type="submit"], button[name="add"], input[type="submit"]');
        if (b) return b;
      }
      var alt = document.querySelector('add-to-cart-component button, product-form-component button[type="submit"]');
      if (alt && !alt.closest('[class*="quick-add"]')) return alt;
      return null;
    }

    function label(btn) {
      var t = (btn.textContent || '').replace(/\\s+/g, ' ').trim();
      return t && t.length > 2 ? t : 'Añadir al carrito';
    }

    function mk(cls, btn) {
      var wrap = document.createElement('div');
      wrap.className = cls;
      var b = document.createElement('button');
      b.type = 'button';
      b.textContent = label(btn);
      b.addEventListener('click', function () {
        btn.scrollIntoView({ block: 'center' });
        btn.click();
      });
      wrap.appendChild(b);
      return wrap;
    }

    function init() {
      var btn = mainAddBtn();
      if (!btn || document.querySelector('.sp-sticky-bar')) return;

      // (a) duplicat al final de la seccio del producte
      var section = btn.closest('section') || btn.closest('main') || btn.closest('form').parentElement;
      if (section && !section.querySelector('.sp-buy-again')) {
        section.appendChild(mk('sp-buy-again', btn));
      }

      // (b) barra sticky inferior (mobil): apareix quan el boto original surt de pantalla
      var bar = mk('sp-sticky-bar', btn);
      document.body.appendChild(bar);
      if ('IntersectionObserver' in window) {
        new IntersectionObserver(function (es) {
          bar.classList.toggle('sp-on', !es[0].isIntersecting);
        }, { threshold: 0 }).observe(btn);
      } else {
        bar.classList.add('sp-on');
      }
    }

    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', init);
    } else { init(); }
    setTimeout(init, 1500);
  })();
  </script>
"""

ANCHOR = "</body>"


def main():
    data = gql(READ_Q, {"id": THEME_ID, "names": [FILENAME]})
    nodes = data["theme"]["files"]["nodes"]
    if not nodes:
        raise SystemExit("ERROR: %s not found on theme" % FILENAME)
    content = nodes[0]["body"]["content"]

    if "sp-ux" in content:
        print("SKIP: sp-ux ja present. Cap canvi.")
        return

    with open("backup_theme_liquid_ux.liquid", "w", encoding="utf-8") as f:
        f.write(content)
    print("Backup written: backup_theme_liquid_ux.liquid (%d bytes)" % len(content))

    if content.count(ANCHOR) != 1:
        raise SystemExit("ERROR: anchor </body> found %d times (expected 1). Aborting." % content.count(ANCHOR))

    new_content = content.replace(ANCHOR, SNIPPET + "\n" + ANCHOR)
    if new_content == content or "sp-ux" not in new_content:
        raise SystemExit("ERROR: sanity check failed. Aborting.")

    res = gql(WRITE_Q, {
        "themeId": THEME_ID,
        "files": [{"filename": FILENAME, "body": {"type": "TEXT", "value": new_content}}],
    })
    errs = res["themeFilesUpsert"]["userErrors"]
    if errs:
        raise SystemExit("themeFilesUpsert userErrors: %s" % json.dumps(errs, ensure_ascii=False))
    print("OK: %s updated (%d -> %d bytes)." % (FILENAME, len(content), len(new_content)))
    print("Desplegat: boto compra duplicat+sticky i carret en negre.")


if __name__ == "__main__":
    main()
