name: Fix Variant Options (Color / Talla)

on:
  workflow_dispatch:
    inputs:
      dry_run:
        description: 'true = només mostra què faria (no modifica res)'
        required: true
        default: 'true'
      limit:
        description: 'Quants productes processar (0 = tots)'
        required: true
        default: '0'
      only_title:
        description: 'Només productes amb aquest text al títol (buit = tots)'
        required: false
        default: ''

jobs:
  fix:
    runs-on: ubuntu-latest
    timeout-minutes: 350
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: pip install requests

      - name: Run fix_variant_options.py
        env:
          JIMSPORTS_API_KEY: ${{ secrets.JIMSPORTS_API_KEY }}
          SHOPIFY_TOKEN: ${{ secrets.SHOPIFY_TOKEN }}
          DRY_RUN: ${{ github.event.inputs.dry_run || 'true' }}
          LIMIT: ${{ github.event.inputs.limit || '0' }}
          ONLY_TITLE: ${{ github.event.inputs.only_title || '' }}
          PYTHONUNBUFFERED: '1'
        run: python -u fix_variant_options.py
