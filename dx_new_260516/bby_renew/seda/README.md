# Brazil Retail Crawler Scaffolds

This folder contains Brazil-market crawler scaffolds aligned with the adjacent
retailer packages.

Channels and product types:

- `r_magalu`: `HHP`
- `r_casas_bahia`: `TV`, `REF`, `LDY`
- `magazine_luiza`: `TV`, `LDY`

Initial commands:

```powershell
python -m r_magalu.r_magalu_orchestrator
python -m r_casas_bahia.r_casas_bahia_orchestrator --product-type TV
python -m r_casas_bahia.r_casas_bahia_orchestrator --product-type REF
python -m r_casas_bahia.r_casas_bahia_orchestrator --product-type LDY
python -m magazine_luiza.magazine_luiza_orchestrator --product-type TV
python -m magazine_luiza.magazine_luiza_orchestrator --product-type LDY
```

The collection steps are intentionally marked as planned until each site's
listing parser, detail parser, request mode, and anti-bot strategy are tested.
