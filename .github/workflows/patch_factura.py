#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
patch_factura.py  (System Padel - Komunika)

Afegeix al carret (pagina /cart i mini-carret/drawer) un bloc "Necesito factura":
un checkbox que desplega 4 camps (Razon social, NIF/DNI, Direccion, CP y poblacion).
Els valors es desen com a ATRIBUTS del carret (attributes[...]) -> viatgen amb la
comanda i es veuen a l'admin de Shopify (Additional details) per fer la factura.

- Els inputs s'insereixen DINS del mateix <form> del boto de checkout -> Shopify
  els persisteix de forma nativa en enviar. A mes, cada canvi fa POST /cart/update.js
  (cinturo i tirants per drawers que salten a /checkout via JS).
- Si el checkbox esta marcat i falten Razon social o NIF/DNI, es bloqueja el
  checkout i es marquen els camps en vermell.
- Sense validacio de format del NIF (decisio client): accepta qualsevol text.

Idempotent: si 'sp-factura' ja es al fitxer, no fa res.
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
  <!-- sp-factura: bloc "Necesito factura" al carret (Komunika) -->
  <style>
    .sp-fact{border:1px solid #d9d9d9;border-radius:8px;padding:14px 16px;margin:14px 0;background:#fafafa;font-size:14px;text-align:left}
    .sp-fact label.sp-fact-main{display:flex;align-items:center;gap:8px;font-weight:600;cursor:pointer;margin:0}
    .sp-fact-fields{display:none;margin-top:12px}
    .sp-fact-fields.sp-open{display:block}
    .sp-fact-fields input{display:block;width:100%;box-sizing:border-box;margin:0 0 8px;padding:9px 10px;border:1px solid #ccc;border-radius:6px;font-size:14px;background:#fff}
    .sp-fact-fields input.sp-err{border-color:#c00;background:#fff5f5}
    .sp-fact-note{font-size:12px;color:#777;margin:4px 0 0}
  </style>
  <script>
  (function () {
    var FIELDS = [
      ['Razon social', 'Nombre fiscal / Razon social *'],
      ['NIF/DNI', 'NIF / DNI *'],
      ['Direccion facturacion', 'Direccion de facturacion'],
      ['CP y poblacion', 'Codigo postal y poblacion']
    ];
    var CHK = 'Necesito factura';

    function currentAttrs(box) {
      var a = {};
      a[CHK] = box.querySelector('.sp-fact-chk').checked ? 'Si' : '';
      FIELDS.forEach(function (f) {
        var el = box.querySelector('[data-sp="' + f[0] + '"]');
        a[f[0]] = (box.querySelector('.sp-fact-chk').checked && el) ? el.value.trim() : '';
      });
      return a;
    }

    function push(box) {
      try {
        fetch('/cart/update.js', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ attributes: currentAttrs(box) })
        });
      } catch (e) {}
    }

    function build(prefill) {
      var box = document.createElement('div');
      box.className = 'sp-fact';
      var checked = prefill[CHK] === 'Si';
      var h = '<label class="sp-fact-main"><input type="checkbox" class="sp-fact-chk" name="attributes[' + CHK + ']" value="Si"' + (checked ? ' checked' : '') + '> Necesito factura</label>';
      h += '<div class="sp-fact-fields' + (checked ? ' sp-open' : '') + '">';
      FIELDS.forEach(function (f) {
        var v = prefill[f[0]] || '';
        h += '<input type="text" data-sp="' + f[0] + '" name="attributes[' + f[0] + ']" placeholder="' + f[1] + '" value="' + v.replace(/"/g, '&quot;') + '">';
      });
      h += '<p class="sp-fact-note">Emitiremos la factura con estos datos. * obligatorios.</p></div>';
      box.innerHTML = h;

      var chk = box.querySelector('.sp-fact-chk');
      chk.addEventListener('change', function () {
        box.querySelector('.sp-fact-fields').classList.toggle('sp-open', chk.checked);
        push(box);
      });
      box.querySelectorAll('.sp-fact-fields input').forEach(function (el) {
        el.addEventListener('change', function () { el.classList.remove('sp-err'); push(box); });
      });
      return box;
    }

    function guard(form, box) {
      form.addEventListener('submit', function (e) {
        var chk = box.querySelector('.sp-fact-chk');
        if (!chk.checked) return;
        var bad = false;
        ['Razon social', 'NIF/DNI'].forEach(function (k) {
          var el = box.querySelector('[data-sp="' + k + '"]');
          if (el && !el.value.trim()) { el.classList.add('sp-err'); bad = true; }
        });
        if (bad) {
          e.preventDefault();
          box.scrollIntoView({ behavior: 'smooth', block: 'center' });
        }
      });
    }

    function init() {
      fetch('/cart.js').then(function (r) { return r.json(); }).then(function (cart) {
        var pre = cart.attributes || {};
        document.querySelectorAll('button[name="checkout"], input[name="checkout"]').forEach(function (btn) {
          var form = btn.closest('form');
          if (!form || form.querySelector('.sp-fact')) return;
          var box = build(pre);
          btn.parentNode.insertBefore(box, btn);
          guard(form, box);
        });
      }).catch(function () {});
    }

    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', init);
    } else { init(); }
    // Drawers que es renderitzen tard: reintent suau
    setTimeout(init, 1500);
    setTimeout(init, 4000);
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

    if "sp-factura" in content:
        print("SKIP: bloc factura ja present (sp-factura). Cap canvi.")
        return

    with open("backup_theme_liquid_factura.liquid", "w", encoding="utf-8") as f:
        f.write(content)
    print("Backup written: backup_theme_liquid_factura.liquid (%d bytes)" % len(content))

    if content.count(ANCHOR) != 1:
        raise SystemExit("ERROR: anchor </body> found %d times (expected 1). Aborting." % content.count(ANCHOR))

    new_content = content.replace(ANCHOR, SNIPPET + "\n" + ANCHOR)

    if new_content == content or "sp-factura" not in new_content:
        raise SystemExit("ERROR: sanity check failed after replace. Aborting.")

    res = gql(WRITE_Q, {
        "themeId": THEME_ID,
        "files": [{"filename": FILENAME, "body": {"type": "TEXT", "value": new_content}}],
    })
    errs = res["themeFilesUpsert"]["userErrors"]
    if errs:
        raise SystemExit("themeFilesUpsert userErrors: %s" % json.dumps(errs, ensure_ascii=False))
    print("OK: %s updated (%d -> %d bytes)." % (FILENAME, len(content), len(new_content)))
    print("Desplegat: bloc 'Necesito factura' al carret (atributs de comanda).")


if __name__ == "__main__":
    main()
